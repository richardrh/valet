// Package cli implements argument parsing and terminal presentation. It does
// not own process signals or terminate the process.
package cli

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"maps"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"syscall"

	"github.com/richardrh/valet/internal/app"
	"github.com/richardrh/valet/internal/config"
	"github.com/richardrh/valet/internal/data"
	"github.com/richardrh/valet/internal/diagnostics"
	"github.com/richardrh/valet/internal/storage"
)

// Process exit codes. Signals other than SIGINT map to 128+signal number.
const (
	ExitOK           = 0
	ExitValidation   = 1
	ExitOperational  = 2
	ExitPresentation = 3
	ExitInterrupted  = 130
)

const usage = `Usage:
  valet validate -p PIPELINE [-i URI] [-o URI] [options]
  valet plan -p PIPELINE [-i URI] [options]
  valet schema -p PIPELINE [-i URI] [options]
  valet --help
  valet --version

Commands:
  validate  Validate the dataset; print summary JSON without artifacts by default.
  plan      Describe schemas, passes and state without executing validation.
  schema    Check Parquet footer/schema compatibility without reading row data.

Options:
  -p, --pipeline PATH     Local pipeline document: .yaml, .yml or .json (required)
  -i, --input URI         Replace the configured source with one file/object
  -o, --output URI        Publish artifacts in this directory/prefix
  --memory-budget SIZE    Managed execution budget, e.g. 512MiB (not RSS)
  --batch-target SIZE     Target Arrow batch size, e.g. 8MiB
  --workers N             Validation workers (default 1)
  --output-mode MODE      all, failures, or report (default report without output)
  --parquet-stats POLICY  off (default) or explicitly trusted producer statistics
  --format FORMAT         schema only: json (default) or readable text
  --plain                 Print the summary as key=value lines
  --stats                 Include per-rule failure counts and configured statistics
  --json-output PATH      Also export summary JSON; '-' explicitly selects JSON stdout
  -v, --verbose           Debug logging (same as --log-level debug)
  --log-level LEVEL       debug, info, warn or error (default info)
  --log-format FORMAT     text or json (default text)
  -h, --help              Show help without accessing storage
  --version               Show executable version without accessing storage

CLI values override YAML, which overrides defaults. When input.type is omitted
it is inferred from the first source URI suffix (.parquet, .ndjson/.jsonl,
.json, otherwise csv; .gz is ignored). Plan prints JSON; schema defaults to JSON
and accepts --format text. Validate defaults to one summary JSON on stdout.
Without an output URI it creates no artifacts; an output URI enables artifact
publication and a completion manifest (default mode all). --plain selects
key=value output; --stats selects statistics presentation unless JSON is explicit.
Schema checks metadata only, not row values. Logs and diagnostics use stderr.

Exit codes: 0 passed (warnings allowed), 1 validation/schema failure,
            2 pre-commit configuration/input/I/O/resource failure,
            3 terminal result committed but presentation failed,
            130 interrupted (SIGINT), 143 terminated (SIGTERM).
`

// Interrupted is the cancellation cause main records when a signal arrives.
type Interrupted struct{ Signal os.Signal }

func (i Interrupted) Error() string { return "interrupted by " + i.Signal.String() }

// ExitCode follows the shell convention of 128 plus the signal number.
func (i Interrupted) ExitCode() int {
	if s, ok := i.Signal.(syscall.Signal); ok && s > 0 {
		return 128 + int(s)
	}
	return ExitInterrupted
}

// exitCode maps a command failure to a process exit code. Only cancellation is
// an interruption; other errors stay operational even after a signal arrived.
func exitCode(ctx context.Context, err error) int {
	if err == nil {
		return ExitOK
	}
	cause := context.Cause(ctx)
	if !errors.Is(err, context.Canceled) && (cause == nil || !errors.Is(err, cause)) {
		return ExitOperational
	}
	var interrupted Interrupted
	if errors.As(cause, &interrupted) {
		return interrupted.ExitCode()
	}
	return ExitInterrupted
}

type options struct {
	command      string
	pipeline     string
	input        string
	output       string
	memory       string
	batch        string
	workers      int
	plain        bool
	verbose      bool
	stats        bool
	jsonOutput   string
	logLevel     string
	logFormat    string
	outputMode   string
	parquetStats string
	schemaFormat string
	recorder     *diagnostics.Recorder
	set          map[string]bool
}

func (o *options) has(names ...string) bool {
	for _, name := range names {
		if o.set[name] {
			return true
		}
	}
	return false
}

// Run returns a process exit code. Help/version paths never load configuration
// or construct application/storage resources. rt.Log is replaced by the logger
// selected with --log-level/--log-format, which always writes to rt.Stderr.
func Run(ctx context.Context, rt app.Runtime, args []string) (code int) {
	out := presenter{stdout: rt.Stdout, stderr: rt.Stderr}
	if len(args) == 0 {
		return out.help()
	}
	preparsed := preparsePresentation(args)
	out.log = newLogger(rt.Stderr, preparsed)
	switch args[0] {
	case "--version", "version":
		if len(args) != 1 {
			return out.fail(ctx, errors.New("version accepts no arguments"))
		}
		if _, err := fmt.Fprintf(rt.Stdout, "valet %s\n", rt.Version); err != nil {
			return out.fail(ctx, err)
		}
		return ExitOK
	case "help":
		if len(args) > 2 || (len(args) == 2 && args[1] != "validate" && args[1] != "plan" && args[1] != "schema") {
			return out.fail(ctx, errors.New("help accepts only validate, plan or schema"))
		}
		return out.help()
	}
	if slices.Contains(args, "--help") || slices.Contains(args, "-h") {
		return out.help()
	}
	opts, err := parse(args)
	if err != nil {
		return out.fail(ctx, diagnostics.PublicError(err))
	}
	var state *commandDiagnostics
	ctx, rt, state, err = beginDiagnostics(ctx, rt, opts)
	if err != nil {
		return out.fail(ctx, err)
	}
	defer func() { state.finish(ctx, code) }()
	out.log = rt.Log
	if err = ctx.Err(); err != nil {
		return out.fail(ctx, err)
	}
	cfg, err := config.Load(ctx, opts.pipeline)
	if err != nil {
		return out.fail(ctx, fmt.Errorf("load pipeline: %w", err))
	}
	opts.apply(cfg)
	if err = cfg.Validate(); err != nil {
		return out.fail(ctx, err)
	}
	state.config = cfg
	rt.Log.Debug("configuration resolved", "version", rt.Version, "command", opts.command, "name", cfg.Name,
		"input_type", cfg.Input.Type, "memory_budget_bytes", cfg.BudgetBytes(), "batch_target_bytes", cfg.BatchBytes(), "workers", cfg.Execution.Workers)
	if opts.command == "plan" {
		description, planErr := app.Plan(ctx, rt, cfg)
		if planErr != nil {
			return out.fail(ctx, planErr)
		}
		presentation := diagnostics.Start(ctx, diagnostics.Present)
		err = encodeJSON(rt.Stdout, description)
		presentation.End(err)
		if err != nil {
			return out.fail(ctx, fmt.Errorf("write plan: %w", err))
		}
		return ExitOK
	}
	if opts.command == "schema" {
		report, schemaErr := app.Schema(ctx, rt, cfg)
		if schemaErr != nil {
			return out.fail(ctx, schemaErr)
		}
		defer report.Release()
		presentation := diagnostics.Start(ctx, diagnostics.Present)
		if opts.schemaFormat == "text" {
			err = writeSchemaText(rt.Stdout, report)
		} else {
			err = encodeJSON(rt.Stdout, report)
		}
		presentation.End(err)
		if err != nil {
			return out.fail(ctx, fmt.Errorf("write schema report: %w", err))
		}
		if !report.Compatible {
			return ExitValidation
		}
		return ExitOK
	}
	if opts.jsonOutput != "" && opts.jsonOutput != "-" {
		rt.BeforeCommit = func(summary *data.Summary) error {
			return writeSummary(opts.jsonOutput, summary)
		}
	}
	summary, err := app.Run(ctx, rt, cfg)
	if err != nil {
		return out.fail(ctx, err)
	}
	state.summary = summary
	presentation := diagnostics.Start(ctx, diagnostics.Present)
	if opts.jsonOutput != "" || (!opts.plain && !opts.stats) {
		err = encodeJSON(rt.Stdout, summary)
	} else {
		err = out.summary(summary, cfg.Output.URI, opts.plain, opts.stats)
	}
	presentation.End(err)
	if err != nil {
		if summary.ManifestURI == "" {
			return out.fail(ctx, fmt.Errorf("write summary: %w", err))
		}
		return out.committedFailure(err, summary.ManifestURI)
	}
	if summary.Validation == data.ValidationFail || summary.Status == data.StatusSchemaRejected {
		return ExitValidation
	}
	return ExitOK
}

func preparsePresentation(args []string) *options {
	opts := &options{logLevel: "info", logFormat: "text"}
	for i := 1; i < len(args); i++ {
		arg := args[i]
		switch {
		case arg == "-v" || arg == "--verbose":
			opts.logLevel = "debug"
		case strings.HasPrefix(arg, "--log-format="):
			if value := strings.TrimPrefix(arg, "--log-format="); value == "text" || value == "json" {
				opts.logFormat = value
			}
		case arg == "--log-format" && i+1 < len(args):
			i++
			if args[i] == "text" || args[i] == "json" {
				opts.logFormat = args[i]
			}
		case strings.HasPrefix(arg, "--log-level="):
			if value := strings.TrimPrefix(arg, "--log-level="); logLevelKnown(value) {
				opts.logLevel = value
			}
		case arg == "--log-level" && i+1 < len(args):
			i++
			if logLevelKnown(args[i]) {
				opts.logLevel = args[i]
			}
		}
	}
	return opts
}

// parse reads the command line and performs every check that does not need
// the pipeline; semantic configuration rules belong to config.Validate.
func parse(args []string) (*options, error) {
	opts := &options{command: args[0], set: make(map[string]bool)}
	if opts.command != "validate" && opts.command != "plan" && opts.command != "schema" {
		return nil, fmt.Errorf("unknown command %q; use --help", opts.command)
	}
	var legacyStdout bool
	flags := flag.NewFlagSet(opts.command, flag.ContinueOnError)
	flags.SetOutput(io.Discard)
	for _, name := range []string{"p", "pipeline"} {
		flags.StringVar(&opts.pipeline, name, "", "")
	}
	for _, name := range []string{"i", "input"} {
		flags.StringVar(&opts.input, name, "", "")
	}
	for _, name := range []string{"o", "output"} {
		flags.StringVar(&opts.output, name, "", "")
	}
	for _, name := range []string{"v", "verbose"} {
		flags.BoolVar(&opts.verbose, name, false, "")
	}
	flags.StringVar(&opts.memory, "memory-budget", "", "")
	flags.StringVar(&opts.batch, "batch-target", "", "")
	flags.IntVar(&opts.workers, "workers", 0, "")
	flags.BoolVar(&opts.plain, "plain", false, "")
	flags.BoolVar(&opts.stats, "stats", false, "")
	flags.StringVar(&opts.jsonOutput, "json-output", "", "")
	flags.StringVar(&opts.logLevel, "log-level", "info", "")
	flags.StringVar(&opts.logFormat, "log-format", "text", "")
	flags.StringVar(&opts.outputMode, "output-mode", "", "")
	flags.StringVar(&opts.parquetStats, "parquet-stats", "", "")
	flags.StringVar(&opts.schemaFormat, "format", "json", "")
	flags.BoolVar(&legacyStdout, "stdout", false, "")
	if err := flags.Parse(args[1:]); err != nil {
		return nil, err
	}
	if flags.NArg() != 0 {
		return nil, fmt.Errorf("unexpected positional argument %q", flags.Arg(0))
	}
	flags.Visit(func(f *flag.Flag) { opts.set[f.Name] = true })
	switch {
	case opts.has("stdout"):
		return nil, errors.New("legacy --stdout is unsupported: validate writes summary JSON to stdout by default; select --output to publish artifacts")
	case strings.TrimSpace(opts.pipeline) == "":
		return nil, errors.New("--pipeline (-p) is required")
	case opts.command == "schema" && opts.has("output-mode", "parquet-stats"):
		return nil, errors.New("schema does not accept data output or statistics-trust options")
	case opts.command != "schema" && opts.has("format"):
		return nil, errors.New("--format is a schema presentation option")
	case opts.has("format") && opts.schemaFormat != "json" && opts.schemaFormat != "text":
		return nil, errors.New("--format must be json or text")
	case opts.has("output-mode") && opts.outputMode != "all" && opts.outputMode != "failures" && opts.outputMode != "report":
		return nil, errors.New("--output-mode must be all, failures or report")
	case opts.has("parquet-stats") && opts.parquetStats != "off" && opts.parquetStats != "trusted":
		return nil, errors.New("--parquet-stats must be off or trusted")
	case opts.command != "validate" && opts.has("json-output"):
		return nil, fmt.Errorf("%s already writes JSON to stdout; --json-output is a validate option", opts.command)
	case opts.command == "schema" && opts.has("o", "output", "plain", "stats", "batch-target", "workers"):
		return nil, errors.New("schema accepts input, memory-budget and logging options, not output or row-execution options")
	case opts.has("json-output") && opts.jsonOutput == "":
		return nil, errors.New("--json-output requires a nonempty path or '-'")
	case opts.plain && opts.jsonOutput == "-":
		return nil, errors.New("--plain conflicts with --json-output -")
	case opts.has("i", "input") && strings.TrimSpace(opts.input) == "":
		return nil, errors.New("--input requires a nonempty URI")
	case opts.has("i", "input") && opts.input == "-":
		return nil, errors.New("--input - (stdin) is unsupported: inputs must be replayable files or s3:// objects")
	case opts.has("o", "output") && strings.TrimSpace(opts.output) == "":
		return nil, errors.New("--output requires a nonempty URI")
	case opts.has("o", "output") && opts.output == "-":
		return nil, errors.New("--output - is unsupported: bucket outputs need a directory or s3:// prefix; use --json-output - for summary JSON")
	case opts.has("memory-budget") && strings.TrimSpace(opts.memory) == "":
		return nil, errors.New("--memory-budget requires a size")
	case opts.has("batch-target") && strings.TrimSpace(opts.batch) == "":
		return nil, errors.New("--batch-target requires a size")
	case opts.logFormat != "text" && opts.logFormat != "json":
		return nil, fmt.Errorf("--log-format must be text or json, not %q", opts.logFormat)
	case !logLevelKnown(opts.logLevel):
		return nil, fmt.Errorf("--log-level must be debug, info, warn or error, not %q", opts.logLevel)
	case opts.verbose && opts.has("log-level") && opts.logLevel != "debug":
		return nil, fmt.Errorf("--verbose selects debug logging and conflicts with --log-level %s", opts.logLevel)
	}
	if opts.verbose {
		opts.logLevel = "debug"
	}
	return opts, nil
}

// apply overlays explicit command-line values on the loaded pipeline.
func (o *options) apply(cfg *config.Config) {
	if o.has("i", "input") {
		cfg.Source = config.Source{URIs: []string{o.input}, S3: cfg.Source.S3}
	}
	if o.has("o", "output") {
		cfg.Output.URI = o.output
	}
	if o.has("memory-budget") {
		cfg.Execution.MemoryBudget = o.memory
	}
	if o.has("batch-target") {
		cfg.Execution.BatchTarget = o.batch
	}
	if o.has("workers") {
		cfg.Execution.Workers = o.workers
	}
	if o.has("output-mode") {
		cfg.Output.Mode = o.outputMode
	}
	if o.has("parquet-stats") {
		cfg.Input.Parquet.Statistics = o.parquetStats
	}
}

var logLevels = map[string]slog.Level{"debug": slog.LevelDebug, "info": slog.LevelInfo, "warn": slog.LevelWarn, "error": slog.LevelError}

func logLevelKnown(name string) bool {
	_, ok := logLevels[name]
	return ok
}

func newLogger(stderr io.Writer, opts *options) *slog.Logger {
	return slog.New(diagnostics.NewHandler(stderr, opts.logFormat, logLevels[opts.logLevel], opts.recorder))
}

// presenter owns every terminal write: stdout for help, plans and summaries;
// stderr for diagnostics. With JSON logging, diagnostics are log records.
type presenter struct {
	stdout, stderr io.Writer
	log            *slog.Logger
}

func (p presenter) help() int {
	if _, err := io.WriteString(p.stdout, usage); err != nil {
		return p.fail(context.Background(), err)
	}
	return ExitOK
}

func (p presenter) fail(ctx context.Context, err error) int {
	if p.log != nil {
		p.log.Error("command failed", "error", err)
	} else {
		fmt.Fprintf(p.stderr, "valet: %v\n", err)
	}
	return exitCode(ctx, err)
}

func (p presenter) committedFailure(err error, manifest string) int {
	if p.log != nil {
		p.log.Error("presentation failed after terminal commit", "error", err, "manifest", manifest)
	} else {
		_, _ = fmt.Fprintf(p.stderr, "valet: terminal result committed at %s; presentation failed: %v\n", manifest, err)
	}
	return ExitPresentation
}

func (p presenter) summary(summary *data.Summary, outputURI string, plain, stats bool) error {
	var b strings.Builder
	if plain {
		writePlainSummary(&b, summary, outputURI, stats)
	} else if err := writeHumanSummary(&b, summary, stats); err != nil {
		return err
	}
	_, err := io.WriteString(p.stdout, b.String())
	return err
}

// writePlainSummary mirrors the Python CLI's key=value lines, followed by
// Go-only run metadata.
func writePlainSummary(b *strings.Builder, s *data.Summary, outputURI string, stats bool) {
	fmt.Fprintf(b, "title=VALIDATION SUMMARY\ntotal=%d\npass=%d\nwarn=%d\nerror=%d\noutput_dir=%s\n", s.Total, s.Passed, s.Warned, s.Errored, outputURI)
	fmt.Fprintf(b, "status=%s\nvalidation=%s\nrun_id=%s\nmanifest=%s\n", s.Status, s.Validation, s.RunID, s.ManifestURI)
	fmt.Fprintf(b, "output_mode=%s\nverification=%s\ntrust_policy=%s\nvalue_rows=%d\nmetadata_claimed_rows=%d\naccounted_rows=%d\n", s.OutputMode, s.Verification, s.TrustPolicy, s.Coverage.ValueRows, s.Coverage.MetadataClaimedRows, s.Coverage.AccountedRows)
	if s.Status == data.StatusSchemaRejected {
		fmt.Fprintf(b, "rows_scanned=%d\nschema_issues=%d\n", s.RowsScanned, len(s.SchemaIssues))
	}
	if !stats {
		return
	}
	if counts := failureCounts(s); len(counts) > 0 {
		b.WriteString("check_stats={\n")
		for _, code := range slices.Sorted(maps.Keys(counts)) {
			fmt.Fprintf(b, "  %s=%d\n", code, counts[code])
		}
		b.WriteString("}\n")
	}
	if len(s.Stats) > 0 {
		b.WriteString("stats={\n")
		for _, code := range slices.Sorted(maps.Keys(s.Stats)) {
			value, err := json.Marshal(s.Stats[code])
			if err != nil {
				value = []byte(fmt.Sprint(s.Stats[code]))
			}
			fmt.Fprintf(b, "  %s=%s\n", code, value)
		}
		b.WriteString("}\n")
	}
}

func writeHumanSummary(b *strings.Builder, s *data.Summary, stats bool) error {
	fmt.Fprintf(b, "Status: %s; validation: %s\nRows: %d; pass: %d; warn: %d; error: %d\nPasses: %d; duration: %.3fs; managed peak: %d bytes\nManifest: %s\n", s.Status, s.Validation, s.Total, s.Passed, s.Warned, s.Errored, s.Passes, s.DurationSeconds, s.ManagedPeak, s.ManifestURI)
	fmt.Fprintf(b, "Output mode: %s; verification: %s; trust policy: %s\nValue-checked rows: %d; metadata-claimed rows: %d; accounted rows: %d\n", s.OutputMode, s.Verification, s.TrustPolicy, s.Coverage.ValueRows, s.Coverage.MetadataClaimedRows, s.Coverage.AccountedRows)
	if s.Status == data.StatusSchemaRejected {
		fmt.Fprintf(b, "Rows scanned: %d; schema issues: %d\n", s.RowsScanned, len(s.SchemaIssues))
	}
	if !stats {
		return nil
	}
	if counts := failureCounts(s); len(counts) > 0 {
		b.WriteString("Check statistics:\n")
		for _, code := range slices.Sorted(maps.Keys(counts)) {
			fmt.Fprintf(b, "  %d %s\n", counts[code], code)
		}
	}
	b.WriteString("Statistics: ")
	return encodeJSON(b, s.Stats)
}

// failureCounts merges nonzero rule and ingestion failure counts by code.
func failureCounts(s *data.Summary) map[string]uint64 {
	counts := make(map[string]uint64, len(s.RuleCounts)+len(s.IngestCounts))
	for _, source := range []map[string]uint64{s.RuleCounts, s.IngestCounts} {
		for code, n := range source {
			if n > 0 {
				counts[code] += n
			}
		}
	}
	return counts
}

func encodeJSON(writer io.Writer, value any) error {
	encoder := json.NewEncoder(writer)
	encoder.SetIndent("", "  ")
	return encoder.Encode(value)
}

var syncSummaryParent = storage.SyncParentDirectory

func writeSummary(path string, summary *data.Summary) (err error) {
	file, err := os.CreateTemp(filepath.Dir(path), ".valet-summary-*")
	if err != nil {
		return err
	}
	temp := file.Name()
	defer func() {
		if err != nil {
			err = errors.Join(err, os.Remove(temp))
		}
	}()
	if err = encodeJSON(file, summary); err == nil {
		err = file.Sync()
	}
	err = errors.Join(err, file.Close())
	if err != nil {
		return err
	}
	if err = os.Rename(temp, path); err != nil {
		return err
	}
	if err = syncSummaryParent(path); err != nil {
		rollbackErr := os.Remove(path)
		if rollbackErr == nil {
			rollbackErr = syncSummaryParent(path)
		}
		return errors.Join(err, rollbackErr)
	}
	return nil
}
