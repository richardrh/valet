//go:build scale

package benchmarks_test

import (
	"context"
	"encoding/json"
	"flag"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"testing"

	"github.com/richardrh/valet/internal/testutil/batchgen"
)

const (
	gib = uint64(1 << 30)

	scaleBudget      = 128 * mib
	scaleStringWidth = 1024
	// At most 1/29 empty, 1/31 whitespace and 1/101 null payloads; 90% of
	// width is a conservative lower bound. Actual measured buffers gate volume.
	scaleMinRowBytes = scaleStringWidth * 9 / 10

	smallScaleVolume   = 10 * gib
	defaultScaleVolume = 100 * gib
	// Managed reservations may grow by at most this much between volumes.
	managedGrowthAllowance = 8 * mib
	// Controlled-runner candidate gate on sustained RSS growth between volumes.
	rssGrowthAllowance = 64 * mib
)

func scaleOptions(volume uint64) batchgen.Options {
	return batchgen.Options{
		Rows:         volume/scaleMinRowBytes + 1,
		BatchRows:    4096,
		Seed:         0x51ca1e,
		StringWidth:  scaleStringWidth,
		Cardinality:  1024,
		FailureEvery: 97,
		NullEvery:    101,
		LateFailure:  true,
		FileRows:     1000000,
	}
}

func scaleRun(t testing.TB, volume uint64) measurement {
	t.Helper()
	o := scaleOptions(volume)
	m, err := engine(context.Background(), engineRun{options: o, limit: scaleBudget, rules: numericRules()})
	if err != nil {
		t.Fatal(err)
	}
	if err := expectNumeric(m, o); err != nil {
		t.Fatal(err)
	}
	// Each run is hours long; stop at the first broken invariant.
	if m.LogicalBytes < volume {
		t.Fatalf("logical bytes = %d, want >= %d", m.LogicalBytes, volume)
	}
	if m.ManagedPeak > scaleBudget {
		t.Fatalf("managed peak = %d, want <= budget %d", m.ManagedPeak, scaleBudget)
	}
	if m.ManagedResidual != 0 {
		t.Fatalf("managed residual = %d, want 0", m.ManagedResidual)
	}
	if m.RSSSamples == 0 {
		t.Fatal("sustained RSS samples = 0, want > 0")
	}
	return m
}

// Child-process protocol: the parent re-executes this test binary with these
// explicit flags, so nothing is inherited through the environment and a child
// can never spawn further children.
var (
	scaleChildBytes  = flag.Uint64("scale.child-bytes", 0, "internal: measure this logical volume and exit (child process)")
	scaleChildResult = flag.String("scale.child-result", "", "internal: path receiving the child measurement JSON")
)

// measureInChild runs scaleRun for volume in a fresh process so runtime
// retention from one volume cannot inflate the next.
func measureInChild(t *testing.T, volume uint64) measurement {
	t.Helper()
	exe, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), "measurement.json")
	cmd := exec.Command(exe, "-test.run=^TestScale100GiB$", "-test.timeout=0", "-test.v",
		"-scale.child-bytes="+strconv.FormatUint(volume, 10), "-scale.child-result="+path)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	if err := cmd.Run(); err != nil {
		t.Fatalf("scale subprocess volume=%d: %v", volume, err)
	}
	body, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var m measurement
	if err := json.Unmarshal(body, &m); err != nil {
		t.Fatal(err)
	}
	t.Logf("actual scale measurement: %s", body)
	return m
}

// runScaleChild is the child half of measureInChild.
func runScaleChild(t *testing.T) {
	t.Helper()
	if *scaleChildResult == "" {
		t.Fatal("-scale.child-result is required with -scale.child-bytes")
	}
	body, err := json.MarshalIndent(scaleRun(t, *scaleChildBytes), "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(*scaleChildResult, body, 0600); err != nil {
		t.Fatal(err)
	}
}

// scaleVolume returns the large-run volume: VALET_SCALE_BYTES if set, else 100GiB.
func scaleVolume(t *testing.T) uint64 {
	t.Helper()
	text := os.Getenv("VALET_SCALE_BYTES")
	if text == "" {
		return defaultScaleVolume
	}
	volume, err := strconv.ParseUint(text, 10, 64)
	if err != nil {
		t.Fatalf("VALET_SCALE_BYTES = %q, want an integer of at least %d logical bytes: %v", text, defaultScaleVolume, err)
	}
	if volume < defaultScaleVolume {
		t.Fatalf("VALET_SCALE_BYTES = %d, want at least %d logical bytes", volume, defaultScaleVolume)
	}
	return volume
}

// TestScale100GiB runs only with -tags=scale and explicit test selection. Its
// child process measurements isolate runtime retention between 10 and 100GiB.
// VALET_SCALE_RSS_GATE=1 applies the controlled-runner 64MiB candidate gate;
// the ordinary scale gate always enforces volume, exact outcomes and managed memory.
func TestScale100GiB(t *testing.T) {
	if *scaleChildBytes != 0 {
		runScaleChild(t)
		return
	}
	volume := scaleVolume(t)
	results := []measurement{measureInChild(t, smallScaleVolume), measureInChild(t, volume)}
	small, large := results[0], results[1]
	if limit := small.ManagedPeak + managedGrowthAllowance; large.ManagedPeak > limit {
		t.Fatalf("managed peak at %d bytes = %d, want <= %d (peak at %d bytes + %d)",
			volume, large.ManagedPeak, limit, smallScaleVolume, managedGrowthAllowance)
	}
	rssGate := os.Getenv("VALET_SCALE_RSS_GATE") == "1"
	if limit := small.SustainedRSS + rssGrowthAllowance; rssGate && large.SustainedRSS > limit {
		t.Fatalf("sustained RSS at %d bytes = %d, want <= %d (10GiB sustained RSS %d + allowance %d, sampling=%dms)",
			volume, large.SustainedRSS, limit, small.SustainedRSS, rssGrowthAllowance, large.RSSIntervalMillis)
	}
	if artifact := os.Getenv("VALET_SCALE_ARTIFACT"); artifact != "" {
		body, err := json.MarshalIndent(results, "", "  ")
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(artifact, body, 0600); err != nil {
			t.Fatal(err)
		}
	}
}

func BenchmarkEngine100GiB(b *testing.B) {
	b.ReportAllocs()
	for range b.N {
		m := scaleRun(b, defaultScaleVolume)
		b.SetBytes(int64(m.LogicalBytes))
		b.ReportMetric(float64(m.ManagedPeak), "managed-peak-B")
		b.ReportMetric(float64(m.PeakRSS), "rss-peak-B")
		b.ReportMetric(float64(m.Rows), "rows/op")
		b.Logf("rows=%d bytes=%d width=%f digest=%s", m.Rows, m.LogicalBytes, m.AverageWidth, m.Digest)
	}
}
