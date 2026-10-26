package app

import (
	"context"
	"io"
	"log/slog"

	"github.com/apache/arrow-go/v18/arrow"

	"github.com/richardrh/valet/internal/checks"
	"github.com/richardrh/valet/internal/config"
	"github.com/richardrh/valet/internal/data"
	"github.com/richardrh/valet/internal/resource"
	"github.com/richardrh/valet/internal/sink"
	"github.com/richardrh/valet/internal/source"
	"github.com/richardrh/valet/internal/storage"
)

// Runtime carries process-scoped collaborators. main supplies the writers and
// version; cli installs Log from the logging flags. Log always writes to Stderr
// so Stdout stays reserved for summaries, plans and JSON.
type Runtime struct {
	Log     *slog.Logger
	Stdout  io.Writer
	Stderr  io.Writer
	Version string
	RunID   string
	// BeforeCommit receives the finalized summary before artifact commitment,
	// or before returning a completed stdout-only report.
	BeforeCommit func(*data.Summary) error
}

func (rt Runtime) logger() *slog.Logger {
	if rt.Log == nil {
		return slog.New(slog.DiscardHandler)
	}
	return rt.Log
}

// Store is the storage handle shared by the input, reference and output sides.
type Store interface {
	Close() error
}

// Dataset is the discovered, replayable validation input.
type Dataset interface {
	Schema() *arrow.Schema
	Objects() []storage.Object
	SchemaIssues() []data.Issue
	Passes() int
	DecoderPassesPerOpen() int
	Open(context.Context) (source.Reader, error)
	Close() error
}

// Writer publishes bucket outputs and the terminal manifest.
// WriteBatch is called in input order by one goroutine. It must finish using
// borrowed batch/result data before returning, or retain its own accounted copy.
type Writer interface {
	WriteInputs([]storage.Object) error
	WriteIssue(data.Issue) error
	WriteBatch(*data.Batch, *data.Result) error
	Complete(*data.Summary, ...func(*data.Summary) error) (string, error)
	Abort() error
}

// Program evaluates compiled checks over validation batches.
// Evaluate and all summary mutation have one owner. A successful result is
// independently owned by its caller until Result.Release, including while a
// later batch is evaluated. Close runs only after all results are released.
type Program interface {
	Summary() *data.Summary
	Evaluate(*data.Batch) (*data.Result, error)
	Finalize() error
	Close() error
}

// Deps holds the constructors a run is assembled from. Production wires the
// real packages; tests substitute fakes. Constructors receive handles produced
// by the same Deps value.
type Deps struct {
	OpenStorage func(ctx context.Context, cfg *config.Config, budget *resource.Budget, log *slog.Logger) (Store, error)
	OpenSource  func(ctx context.Context, cfg *config.Config, store Store, budget *resource.Budget, log *slog.Logger, plan bool) (Dataset, error)
	OpenSink    func(ctx context.Context, cfg *config.Config, store Store, budget *resource.Budget, runID string, schema *arrow.Schema, log *slog.Logger) (Writer, error)
	Compile     func(ctx context.Context, cfg *config.Config, ds Dataset, store Store, budget *resource.Budget, log *slog.Logger, issues func(data.Issue) error) (Program, error)
	Describe    func(ctx context.Context, cfg *config.Config, ds Dataset, store Store, budget *resource.Budget) (*checks.Description, error)
}

// Production returns the dependencies backed by storage, source, sink and checks.
// Each constructor returns a nil interface on failure, never a typed nil.
func Production() Deps {
	return Deps{
		OpenStorage: func(ctx context.Context, cfg *config.Config, budget *resource.Budget, log *slog.Logger) (Store, error) {
			output := cfg.Source.S3
			if cfg.Output.URI != "" {
				output = cfg.Output.S3
			}
			store, err := storage.New(ctx, cfg.Source.S3, output, budget, storage.WithLogger(log))
			if err != nil {
				return nil, err
			}
			return store, nil
		},
		OpenSource: func(ctx context.Context, cfg *config.Config, store Store, budget *resource.Budget, log *slog.Logger, plan bool) (Dataset, error) {
			options := []source.Option{source.WithLogger(log)}
			if plan {
				options = append(options, source.WithPlan())
			}
			ds, err := source.New(ctx, cfg, store.(*storage.Store), budget, options...)
			if err != nil {
				return nil, err
			}
			return ds, nil
		},
		OpenSink: func(ctx context.Context, cfg *config.Config, store Store, budget *resource.Budget, runID string, schema *arrow.Schema, log *slog.Logger) (Writer, error) {
			writer, err := sink.New(ctx, cfg, store.(*storage.Store), budget, runID, schema, sink.WithLogger(log))
			if err != nil {
				return nil, err
			}
			return writer, nil
		},
		Compile: func(ctx context.Context, cfg *config.Config, ds Dataset, store Store, budget *resource.Budget, log *slog.Logger, issues func(data.Issue) error) (Program, error) {
			program, err := checks.Compile(ctx, cfg, ds.(*source.Dataset), store.(*storage.Store), budget, checks.WithSchemaIssueSink(issues), checks.WithLogger(log))
			if err != nil {
				return nil, err
			}
			return program, nil
		},
		Describe: func(ctx context.Context, cfg *config.Config, ds Dataset, store Store, budget *resource.Budget) (*checks.Description, error) {
			return checks.Describe(ctx, cfg, ds.(*source.Dataset), store.(*storage.Store), budget)
		},
	}
}
