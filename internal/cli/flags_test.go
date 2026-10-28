package cli

import (
	"strings"
	"testing"
)

// Each command accepts only its own flags, and every rejection says what to
// do instead. All argument errors exit 2 without loading the pipeline.
func TestFlagErrorsNameTheProblem(t *testing.T) {
	for _, tc := range []struct {
		args []string
		want string
	}{
		{[]string{"schema", "-p", "x", "-o", "out"}, "-o is not a schema option; schema accepts --pipeline, --input, --format, --log-format, --log-level, --memory-budget, --verbose"},
		{[]string{"schema", "-p", "x", "--output-mode", "all"}, "--output-mode is not a schema option"},
		{[]string{"plan", "-p", "x", "--json-output", "s.json"}, "--json-output is not a plan option"},
		{[]string{"plan", "-p", "x", "--plain"}, "--plain is not a plan option"},
		{[]string{"validate", "-p", "x", "--format", "json"}, "--format is not a validate option"},
		{[]string{"validate", "-p", "x", "--bogus"}, "unknown flag --bogus; see valet --help"},
		{[]string{"validate", "-p", "x", "--output-mode", "some"}, `--output-mode must be all, failures or report, not "some"`},
		{[]string{"validate", "-p", "x", "--parquet-stats", "on"}, `--parquet-stats must be off or trusted, not "on"`},
		{[]string{"plan", "-p", "x", "--format", "text"}, `--format must be json or pipeline, not "text"`},
		{[]string{"schema", "-p", "x", "--format", "pipeline"}, `--format must be json or text, not "pipeline"`},
		{[]string{"validate", "-p", "x", "--log-level", "trace"}, `--log-level must be debug, info, warn or error, not "trace"`},
		{[]string{"validate", "-p", "x", "-i", "-"}, "--input - (stdin) is unsupported"},
		{[]string{"validate", "-p", "x", "-o", ""}, "--output requires a nonempty URI"},
		{[]string{"validate", "-p", "x", "--memory-budget", " "}, "--memory-budget requires a size"},
		{[]string{"validate", "-p", "x", "--workers"}, "--workers needs a value"},
		{[]string{"validate", "-p", "x", "--workers", "many"}, `--workers must be a number, not "many"`},
	} {
		t.Run(strings.Join(tc.args, " "), func(t *testing.T) {
			r := invoke(t, tc.args...)
			assertDiagnosticOnly(t, r, ExitOperational)
			if !strings.Contains(r.stderr, tc.want) {
				t.Errorf("stderr does not contain %q:\n%s", tc.want, r.stderr)
			}
		})
	}
}
