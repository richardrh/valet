//go:build integration

package integration_test

import (
	"encoding/json"
	"path/filepath"
	"testing"
)

func TestRulesCookbook(t *testing.T) {
	pipelinePath, err := filepath.Abs(filepath.Join("..", "examples", "cookbook", "orders.yml"))
	if err != nil {
		t.Fatal(err)
	}
	inputPath, err := filepath.Abs(filepath.Join("..", "examples", "cookbook", "orders.csv"))
	if err != nil {
		t.Fatal(err)
	}
	stdout, stderr, code := invoke(t, "plan", "-p", pipelinePath, "-i", inputPath)
	if code != exitOK {
		t.Fatalf("cookbook plan exit=%d stdout=%s stderr=%s", code, stdout, stderr)
	}
	var plan map[string]any
	if err = json.Unmarshal(stdout, &plan); err != nil {
		t.Fatal(err)
	}
	checks, ok := plan["checks"].(map[string]any)
	if !ok {
		t.Fatalf("cookbook plan checks=%v", plan["checks"])
	}
	rules, ok := checks["rules"].([]any)
	if !ok || len(rules) != 9 {
		t.Fatalf("cookbook plan rules=%v", checks["rules"])
	}

	output := filepath.Join(t.TempDir(), "output")
	stdout, stderr, code = invoke(t, "validate", "-p", pipelinePath, "-i", inputPath, "-o", output, "--json-output", "-")
	if code != exitValidationFailed {
		t.Fatalf("cookbook validate exit=%d stdout=%s stderr=%s", code, stdout, stderr)
	}
	var result summary
	if err = json.Unmarshal(stdout, &result); err != nil {
		t.Fatal(err)
	}
	if got, want := (counts{total: result.Total, pass: result.Passed, warn: result.Warned, error: result.Errored}), (counts{total: 3, pass: 1, error: 2}); got != want {
		t.Fatalf("cookbook counts=%+v want=%+v", got, want)
	}
	if result.Validation != "fail" || result.ManifestURI == "" {
		t.Fatalf("cookbook summary=%+v", result)
	}
}
