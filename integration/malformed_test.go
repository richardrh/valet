//go:build integration

package integration_test

import (
	"encoding/json"
	"slices"
	"strings"
	"testing"

	"github.com/google/go-cmp/cmp"
)

// With input.on_malformed: reject, a CSV row with the wrong number of fields
// becomes an error row with its original fields in rejects; the rest of the
// file is still validated. Malformed input whose record boundary is unknown
// still stops the run.
func TestMalformedCSVRowsCanBeRejected(t *testing.T) {
	const input = "id,amount\n1,2\n2,3,EXTRA\n3\n4,5\n"
	rules := `rules:
  - code: ID_PRESENT
    level: error
    column: id
    checks: [{type: not_null}]`

	t.Run("default stops", func(t *testing.T) {
		r := run(t, scenario{input: input, config: []string{rules}})
		if r.code != exitOperational {
			t.Fatalf("exit = %d, want %d; stderr=%s", r.code, exitOperational, r.stderr)
		}
	})

	r := run(t, scenario{input: input, config: []string{"input: {on_malformed: reject}", rules}})
	if r.code != exitValidationFailed {
		t.Fatalf("exit = %d, want %d; stderr=%s", r.code, exitValidationFailed, r.stderr)
	}
	if diff := cmp.Diff(counts{total: 4, pass: 2, error: 2}, countsOf(r), cmp.AllowUnexported(counts{})); diff != "" {
		t.Errorf("counts diff (-want +got):\n%s", diff)
	}
	if got := r.summary.IngestCounts["INGEST_MALFORMED"]; got != 2 {
		t.Errorf("ingest_counts[INGEST_MALFORMED] = %d, want 2", got)
	}
	if got, want := r.globalRows(t, "error"), rowNumbers(2, 3); !slices.Equal(got, want) {
		t.Errorf("error rows = %q, want %q", got, want)
	}
	if got, want := r.globalRows(t, "pass"), rowNumbers(1, 4); !slices.Equal(got, want) {
		t.Errorf("pass rows = %q, want %q", got, want)
	}
	// A malformed row is reported once, by INGEST_MALFORMED; its invalid cells
	// do not also fail column rules.
	if got := r.summary.RuleCounts["ID_PRESENT"]; got != 0 {
		t.Errorf("rule_counts[ID_PRESENT] = %d, want 0", got)
	}

	rejects := readRejects(t, r)
	if len(rejects) != 2 {
		t.Fatalf("rejects = %d, want 2", len(rejects))
	}
	for i, want := range []struct {
		raw    []string
		line   float64
		reason string
	}{
		{raw: []string{"2", "3", "EXTRA"}, line: 3, reason: "found 3 fields; the header has 2"},
		{raw: []string{"3"}, line: 4, reason: "found 1 field; the header has 2"},
	} {
		var raw []string
		if err := json.Unmarshal(rejects[i]["raw"], &raw); err != nil || !slices.Equal(raw, want.raw) {
			t.Errorf("reject %d raw = %s, want %q", i, rejects[i]["raw"], want.raw)
		}
		var line float64
		_ = json.Unmarshal(rejects[i]["line"], &line)
		if line != want.line {
			t.Errorf("reject %d line = %v, want %v", i, line, want.line)
		}
		var diagnostics []struct{ Code, Reason string }
		_ = json.Unmarshal(rejects[i]["diagnostics"], &diagnostics)
		if len(diagnostics) != 1 || diagnostics[0].Code != "INGEST_MALFORMED" || diagnostics[0].Reason != want.reason {
			t.Errorf("reject %d diagnostics = %s, want one INGEST_MALFORMED with reason %q", i, rejects[i]["diagnostics"], want.reason)
		}
	}

	t.Run("unknown boundaries still stop", func(t *testing.T) {
		r := run(t, scenario{input: "id,amount\n1,2\n3,\"unterminated\n4,5\n", config: []string{"input: {on_malformed: reject}", rules}})
		if r.code != exitOperational || !strings.Contains(string(r.stderr), "framing_reason=\"quote\"") {
			t.Errorf("exit = %d, want %d with a quote framing error; stderr=%s", r.code, exitOperational, r.stderr)
		}
	})
}
