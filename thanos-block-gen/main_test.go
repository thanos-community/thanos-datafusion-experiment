package main

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/thanos-io/thanos/pkg/block/metadata"
	"github.com/thanos-io/thanos/pkg/compact/downsample"
)

func TestCreatesRawAndFiveMinuteBlocks(t *testing.T) {
	t.Parallel()

	cfg := config{
		output:         t.TempDir(),
		mint:           1_700_000_000_000,
		maxt:           1_700_000_600_000,
		samples:        10,
		externalLabels: map[string]string{"cluster": "test"},
		instances:      2,
		routes:         1,
		nativeSeries:   1,
	}
	rawID, rawMeta, err := createRawBlock(context.Background(), cfg)
	if err != nil {
		t.Fatalf("create raw block: %v", err)
	}
	assertBlockFiles(t, cfg.output, rawID)
	if got := rawMeta.Thanos.Downsample.Resolution; got != downsample.ResLevel0 {
		t.Fatalf("raw resolution = %d, want %d", got, downsample.ResLevel0)
	}
	if rawMeta.Thanos.Labels["cluster"] != "test" {
		t.Fatalf("raw external labels = %#v", rawMeta.Thanos.Labels)
	}
	if len(rawMeta.Thanos.Files) == 0 || rawMeta.Thanos.Files[0].Hash == nil {
		t.Fatalf("raw file metadata = %#v, want SHA-256 file hashes", rawMeta.Thanos.Files)
	}
	if rawMeta.Thanos.UploadTime.IsZero() {
		t.Fatal("raw upload time is zero")
	}
	if rawMeta.Stats.NumSamples != uint64(cfg.seriesCount()*cfg.samples) {
		t.Fatalf("raw samples = %d, want %d samples for every series", rawMeta.Stats.NumSamples, cfg.seriesCount()*cfg.samples)
	}

	downsampledID, err := create5mBlock(context.Background(), cfg.output, rawID, rawMeta)
	if err != nil {
		t.Fatalf("create 5m block: %v", err)
	}
	assertBlockFiles(t, cfg.output, downsampledID)

	downsampledMeta, err := metadata.ReadFromDir(filepath.Join(cfg.output, downsampledID))
	if err != nil {
		t.Fatalf("read 5m metadata: %v", err)
	}
	if got := downsampledMeta.Thanos.Downsample.Resolution; got != downsample.ResLevel1 {
		t.Fatalf("5m resolution = %d, want %d", got, downsample.ResLevel1)
	}
	if len(downsampledMeta.Thanos.Files) == 0 || downsampledMeta.Thanos.Files[0].Hash == nil {
		t.Fatalf("5m file metadata = %#v, want SHA-256 file hashes", downsampledMeta.Thanos.Files)
	}
}

func assertBlockFiles(t *testing.T, output, id string) {
	t.Helper()
	for _, name := range []string{"chunks", "index", "meta.json"} {
		if _, err := os.Stat(filepath.Join(output, id, name)); err != nil {
			t.Fatalf("block %s missing %s: %v", id, name, err)
		}
	}
}

func TestParseConfigRejectsTooShortRange(t *testing.T) {
	_, err := parseConfig([]string{"--mint", "0", "--maxt", "299999"})
	if err == nil {
		t.Fatal("expected short range to be rejected")
	}
}

func TestDefaultConfigCreatesDenseOneHourFixture(t *testing.T) {
	cfg, err := parseConfig(nil)
	if err != nil {
		t.Fatalf("parse default config: %v", err)
	}
	if got := cfg.maxt - cfg.mint; got != 60*60*1000 {
		t.Fatalf("default duration = %dms, want one hour", got)
	}
	if cfg.samples != 240 {
		t.Fatalf("default samples = %d, want 240", cfg.samples)
	}
	if cfg.seriesCount() != 5700 {
		t.Fatalf("default series = %d, want 5700", cfg.seriesCount())
	}
}

func TestWritesReadableMetricSchemaTable(t *testing.T) {
	cfg := config{instances: 2, pods: 100, routes: 3, nativeSeries: 4}
	var output bytes.Buffer

	writeSeriesSchema(&output, cfg)

	for _, expected := range []string{
		"Generated time series",
		"METRIC",
		"__name__",
		"instance",
		"pod",
		"dummy_requests_total",
		"counter",
		"100",
		"dummy_request_duration_seconds_bucket",
		"classic histogram bucket",
		"dummy_native_histogram",
		"native histogram (integer)",
		"dummy_float_native_histogram",
		"native histogram (float)",
	} {
		if !strings.Contains(output.String(), expected) {
			t.Errorf("schema output missing %q:\n%s", expected, output.String())
		}
	}
}

func TestAllMetricFamiliesHaveRequestedPodCardinality(t *testing.T) {
	for _, schema := range metricSchemas(config{instances: 20, pods: 100, routes: 5, nativeSeries: 10}) {
		if got := schema.labelCardinality["pod"]; got != 100 {
			t.Errorf("%s pod cardinality = %d, want 100", schema.name, got)
		}
	}
}

func TestFixtureLabelValuesAreDistinctAndIdentifiable(t *testing.T) {
	values := newFixtureLabelValues(config{
		instances:    2,
		pods:         3,
		routes:       4,
		nativeSeries: 5,
	})

	for prefix, labels := range map[string][]string{
		"instance-": values.instances,
		"pod-":      values.pods,
		"route-":    values.routes,
		"series-":   values.series,
	} {
		seen := make(map[string]struct{}, len(labels))
		for _, value := range labels {
			if !strings.HasPrefix(value, prefix) {
				t.Errorf("label value %q does not start with %q", value, prefix)
			}
			seen[value] = struct{}{}
		}
		if len(seen) != len(labels) {
			t.Errorf("%s labels are not distinct: %v", prefix, labels)
		}
	}
	if !strings.HasPrefix(values.job, "job-") {
		t.Errorf("job label %q does not start with job-", values.job)
	}
}
