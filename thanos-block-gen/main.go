// thanos-block-gen creates small local Thanos blocks for reader development.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"text/tabwriter"
	"time"

	"github.com/go-kit/log"
	"github.com/prometheus/prometheus/model/histogram"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/model/value"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb"
	"github.com/thanos-io/thanos/pkg/block"
	"github.com/thanos-io/thanos/pkg/block/metadata"
	"github.com/thanos-io/thanos/pkg/compact/downsample"
	"github.com/thanos-io/thanos/pkg/logutil"
)

const fiveMinutesMillis int64 = 5 * 60 * 1000

var fixtureLabelDictionary = []string{
	"amber", "birch", "cobalt", "dahlia", "ember", "falcon", "grove", "harbor",
	"indigo", "juniper", "kepler", "linden", "marble", "nova", "onyx", "prairie",
}

type config struct {
	output         string
	mint           int64
	maxt           int64
	samples        int
	externalLabels map[string]string
	internalLabels map[string]string
	instances      int
	pods           int
	routes         int
	nativeSeries   int
	scalarEdges    bool
	clean          bool
	downsample5m   bool
	downsample1h   bool
}

func main() {
	if err := run(os.Args[1:], os.Stdout); err != nil {
		fmt.Fprintln(os.Stderr, "thanos-block-gen:", err)
		os.Exit(1)
	}
}

func run(args []string, stdout io.Writer) error {
	cfg, err := parseConfig(args)
	if err != nil {
		return err
	}

	if cfg.clean {
		if err := os.RemoveAll(cfg.output); err != nil {
			return fmt.Errorf("clean output directory: %w", err)
		}
	}
	if err := os.MkdirAll(cfg.output, 0o750); err != nil {
		return fmt.Errorf("create output directory: %w", err)
	}

	fmt.Fprintf(
		stdout,
		"level=info msg=%q output=%q mint=%d maxt=%d duration=%q samples=%d series=%d external_labels=%q downsample_5m=%t downsample_1h=%t clean=%t\n",
		"generating Thanos fixture blocks",
		cfg.output,
		cfg.mint,
		cfg.maxt,
		time.Duration(cfg.maxt-cfg.mint)*time.Millisecond,
		cfg.samples,
		cfg.seriesCount(),
		formatLabels(cfg.externalLabels),
		cfg.downsample5m,
		cfg.downsample1h,
		cfg.clean,
	)
	writeSeriesSchema(stdout, cfg)

	ctx := context.Background()
	rawID, rawMeta, err := createRawBlock(ctx, cfg)
	if err != nil {
		return err
	}
	fmt.Fprintf(stdout, "raw block: %s\n", rawID)

	if cfg.downsample5m {
		downsampledID, downsampledMeta, err := create5mBlock(ctx, cfg.output, rawID, rawMeta)
		if err != nil {
			return err
		}
		fmt.Fprintf(stdout, "5m block:  %s\n", downsampledID)
		if cfg.downsample1h {
			hourlyID, err := create1hBlock(ctx, cfg.output, downsampledID, downsampledMeta)
			if err != nil {
				return err
			}
			fmt.Fprintf(stdout, "1h block:  %s\n", hourlyID)
		}
	}
	fmt.Fprintf(stdout, "output:     %s\n", cfg.output)
	return nil
}

func parseConfig(args []string) (config, error) {
	defaultEnd := time.Now().Truncate(time.Second)
	cfg := config{
		output:         "target",
		mint:           defaultEnd.Add(-time.Hour).UnixMilli(),
		maxt:           defaultEnd.UnixMilli(),
		samples:        240,
		externalLabels: map[string]string{"cluster": "dummy", "replica": "0"},
		instances:      20,
		pods:           100,
		routes:         5,
		nativeSeries:   10,
		downsample5m:   true,
	}

	flags := flag.NewFlagSet("thanos-block-gen", flag.ContinueOnError)
	flags.SetOutput(os.Stderr)
	var externalLabels labelFlags
	var internalLabels labelFlags
	flags.StringVar(&cfg.output, "output", cfg.output, "directory in which to write block ULID directories")
	flags.Int64Var(&cfg.mint, "mint", cfg.mint, "minimum sample timestamp in Unix milliseconds")
	flags.Int64Var(&cfg.maxt, "maxt", cfg.maxt, "exclusive maximum sample timestamp in Unix milliseconds")
	flags.IntVar(&cfg.samples, "samples", cfg.samples, "number of samples per series")
	flags.IntVar(&cfg.instances, "instances", cfg.instances, "number of distinct instance label values")
	flags.IntVar(&cfg.pods, "pods", cfg.pods, "number of counter and gauge pod series")
	flags.IntVar(&cfg.routes, "routes", cfg.routes, "number of classic histogram route series")
	flags.IntVar(&cfg.nativeSeries, "native-series", cfg.nativeSeries, "number of integer and float native histogram series")
	flags.BoolVar(&cfg.scalarEdges, "scalar-edge-cases", false, "include counter resets, special gauge values, and zero histograms")
	flags.Var(&externalLabels, "external-label", "external label in name=value form; repeatable")
	flags.Var(&internalLabels, "internal-label", "internal series label in name=value form; repeatable")
	flags.BoolVar(&cfg.clean, "clean", false, "remove the output directory before generating blocks")
	flags.BoolVar(&cfg.downsample5m, "downsample-5m", true, "also generate a 5-minute downsampled block")
	flags.BoolVar(&cfg.downsample1h, "downsample-1h", false, "also generate a 1-hour block from the 5-minute block")
	if err := flags.Parse(args); err != nil {
		return config{}, err
	}
	if flags.NArg() != 0 {
		return config{}, fmt.Errorf("unexpected arguments: %s", strings.Join(flags.Args(), " "))
	}
	if cfg.samples < 2 {
		return config{}, errors.New("--samples must be at least 2")
	}
	if cfg.instances < 1 || cfg.pods < 1 || cfg.routes < 1 || cfg.nativeSeries < 1 {
		return config{}, errors.New("--instances, --pods, --routes, and --native-series must be at least 1")
	}
	if cfg.maxt-cfg.mint < fiveMinutesMillis {
		return config{}, errors.New("the time range must span at least 5 minutes")
	}
	if cfg.downsample1h && !cfg.downsample5m {
		return config{}, errors.New("--downsample-1h requires --downsample-5m")
	}
	for _, raw := range externalLabels {
		name, value, ok := strings.Cut(raw, "=")
		if !ok || name == "" {
			return config{}, fmt.Errorf("invalid --external-label %q; expected name=value", raw)
		}
		cfg.externalLabels[name] = value
	}
	cfg.internalLabels = make(map[string]string, len(internalLabels))
	for _, raw := range internalLabels {
		name, value, ok := strings.Cut(raw, "=")
		if !ok || name == "" || name == labels.MetricName {
			return config{}, fmt.Errorf("invalid --internal-label %q; expected non-__name__ name=value", raw)
		}
		cfg.internalLabels[name] = value
	}
	return cfg, nil
}

func (cfg config) seriesCount() int {
	_, pods, routes, nativeSeries := cfg.dimensions()
	return pods * (2 + routes*7 + nativeSeries*2)
}

func (cfg config) dimensions() (instances, pods, routes, nativeSeries int) {
	instances, pods, routes, nativeSeries = cfg.instances, cfg.pods, cfg.routes, cfg.nativeSeries
	if instances == 0 {
		instances = 1
	}
	if pods == 0 {
		pods = 1
	}
	if routes == 0 {
		routes = 1
	}
	if nativeSeries == 0 {
		nativeSeries = 1
	}
	return instances, pods, routes, nativeSeries
}

func formatLabels(labelsMap map[string]string) string {
	names := make([]string, 0, len(labelsMap))
	for name := range labelsMap {
		names = append(names, name)
	}
	sort.Strings(names)

	values := make([]string, 0, len(names))
	for _, name := range names {
		values = append(values, name+"="+labelsMap[name])
	}
	return strings.Join(values, ",")
}

type metricSchema struct {
	name             string
	metricType       string
	series           int
	labelCardinality map[string]int
}

func writeSeriesSchema(stdout io.Writer, cfg config) {
	schemas := metricSchemas(cfg)
	labelNames := allLabelNames(schemas)

	fmt.Fprintln(stdout, "\nGenerated time series")
	table := tabwriter.NewWriter(stdout, 0, 0, 2, ' ', 0)
	fmt.Fprint(table, "METRIC\tTYPE\tSERIES")
	for _, labelName := range labelNames {
		fmt.Fprintf(table, "\t%s", labelName)
	}
	fmt.Fprintln(table)
	fmt.Fprint(table, "------\t----\t------")
	for range labelNames {
		fmt.Fprint(table, "\t-")
	}
	fmt.Fprintln(table)
	for _, schema := range schemas {
		fmt.Fprintf(table, "%s\t%s\t%d", schema.name, schema.metricType, schema.series)
		for _, labelName := range labelNames {
			if cardinality, ok := schema.labelCardinality[labelName]; ok {
				fmt.Fprintf(table, "\t%d", cardinality)
				continue
			}
			fmt.Fprint(table, "\t-")
		}
		fmt.Fprintln(table)
	}
	_ = table.Flush()
}

func metricSchemas(cfg config) []metricSchema {
	instances, pods, routes, nativeSeries := cfg.dimensions()
	return []metricSchema{
		{
			name:       "dummy_requests_total",
			metricType: "counter",
			series:     pods,
			labelCardinality: map[string]int{
				"__name__": 1, "instance": instances, "job": 1, "method": 1, "pod": pods,
			},
		},
		{
			name:       "dummy_temperature_celsius",
			metricType: "gauge",
			series:     pods,
			labelCardinality: map[string]int{
				"__name__": 1, "instance": instances, "job": 1, "pod": pods, "room": 1,
			},
		},
		{
			name:       "dummy_request_duration_seconds_bucket",
			metricType: "classic histogram bucket",
			series:     pods * routes * 5,
			labelCardinality: map[string]int{
				"__name__": 1, "job": 1, "le": 5, "pod": pods, "route": routes,
			},
		},
		{
			name:       "dummy_request_duration_seconds_sum",
			metricType: "classic histogram sum",
			series:     pods * routes,
			labelCardinality: map[string]int{
				"__name__": 1, "job": 1, "pod": pods, "route": routes,
			},
		},
		{
			name:       "dummy_request_duration_seconds_count",
			metricType: "classic histogram count",
			series:     pods * routes,
			labelCardinality: map[string]int{
				"__name__": 1, "job": 1, "pod": pods, "route": routes,
			},
		},
		{
			name:       "dummy_native_histogram",
			metricType: "native histogram (integer)",
			series:     pods * nativeSeries,
			labelCardinality: map[string]int{
				"__name__": 1, "job": 1, "pod": pods, "series": nativeSeries,
			},
		},
		{
			name:       "dummy_float_native_histogram",
			metricType: "native histogram (float)",
			series:     pods * nativeSeries,
			labelCardinality: map[string]int{
				"__name__": 1, "job": 1, "pod": pods, "series": nativeSeries,
			},
		},
	}
}

func allLabelNames(schemas []metricSchema) []string {
	labelsSet := map[string]struct{}{}
	for _, schema := range schemas {
		for labelName := range schema.labelCardinality {
			labelsSet[labelName] = struct{}{}
		}
	}
	labelNames := make([]string, 0, len(labelsSet))
	for labelName := range labelsSet {
		labelNames = append(labelNames, labelName)
	}
	sort.Strings(labelNames)
	return labelNames
}

type labelFlags []string

func (f *labelFlags) String() string { return strings.Join(*f, ",") }

func (f *labelFlags) Set(value string) error {
	*f = append(*f, value)
	return nil
}

func createRawBlock(ctx context.Context, cfg config) (string, *metadata.Meta, error) {
	headOptions := tsdb.DefaultHeadOptions()
	headOptions.ChunkDirRoot = filepath.Join(cfg.output, ".head-chunks")
	headOptions.ChunkRange = cfg.maxt - cfg.mint

	head, err := tsdb.NewHead(nil, nil, nil, nil, headOptions, nil)
	if err != nil {
		return "", nil, fmt.Errorf("create TSDB head: %w", err)
	}
	defer func() {
		_ = head.Close()
		_ = os.RemoveAll(headOptions.ChunkDirRoot)
	}()

	if err := appendDummySamples(ctx, head, cfg); err != nil {
		return "", nil, err
	}

	compactor, err := tsdb.NewLeveledCompactor(ctx, nil, nil, []int64{cfg.maxt - cfg.mint}, nil, nil)
	if err != nil {
		return "", nil, fmt.Errorf("create compactor: %w", err)
	}
	ids, err := compactor.Write(cfg.output, head, cfg.mint, cfg.maxt, nil)
	if err != nil {
		return "", nil, fmt.Errorf("compact head into block: %w", err)
	}
	if len(ids) != 1 {
		return "", nil, fmt.Errorf("expected one raw block, got %d", len(ids))
	}

	blockDir := filepath.Join(cfg.output, ids[0].String())
	if err := os.Remove(filepath.Join(blockDir, "tombstones")); err != nil && !os.IsNotExist(err) {
		return "", nil, fmt.Errorf("remove empty tombstones: %w", err)
	}
	meta, err := enrichMetadata(ctx, blockDir, cfg.externalLabels, metadata.TestSource, downsample.ResLevel0)
	if err != nil {
		return "", nil, err
	}
	return ids[0].String(), meta, nil
}

func appendDummySamples(ctx context.Context, head *tsdb.Head, cfg config) error {
	classicBounds := []float64{0.1, 0.25, 0.5, 1, math.Inf(1)}
	step := (cfg.maxt - cfg.mint) / int64(cfg.samples)
	instances, pods, routes, nativeSeries := cfg.dimensions()
	fixtureLabels := newFixtureLabelValues(cfg)

	for i := 0; i < cfg.samples; i++ {
		timestamp := cfg.mint + int64(i)*step
		app := head.Appender(ctx)
		for pod := 0; pod < pods; pod++ {
			instance := pod % instances
			instanceLabel := fixtureLabels.instances[instance]
			podLabel := fixtureLabels.pods[pod]
			counterValue := float64(1_000 + pod*100 + i*7)
			if cfg.scalarEdges && i == cfg.samples/2 {
				counterValue = float64(10 + pod)
			}
			if err := appendFloat(app, withInternalLabels(labels.FromStrings(labels.MetricName, "dummy_requests_total", "instance", instanceLabel, "job", fixtureLabels.job, "method", "GET", "pod", podLabel), cfg.internalLabels), timestamp, counterValue); err != nil {
				return err
			}
			gaugeValue := 20 + float64(instance)/10 + math.Sin(float64(i+pod)/6)*4
			if cfg.scalarEdges {
				switch i {
				case cfg.samples - 4:
					gaugeValue = math.Inf(1)
				case cfg.samples - 3:
					gaugeValue = math.Inf(-1)
				case cfg.samples - 2:
					gaugeValue = math.Copysign(0, -1)
				case cfg.samples - 1:
					gaugeValue = math.Float64frombits(value.StaleNaN)
				}
			}
			if err := appendFloat(app, withInternalLabels(labels.FromStrings(labels.MetricName, "dummy_temperature_celsius", "instance", instanceLabel, "job", fixtureLabels.job, "pod", podLabel, "room", "lab"), cfg.internalLabels), timestamp, gaugeValue); err != nil {
				return err
			}
		}
		for route := 0; route < routes; route++ {
			routeLabel := fixtureLabels.routes[route]
			for pod := 0; pod < pods; pod++ {
				podLabel := fixtureLabels.pods[pod]
				observations := float64(i + route + pod + 1)
				if cfg.scalarEdges && i == 0 {
					observations = 0
				}
				if err := appendFloat(app, withInternalLabels(labels.FromStrings(labels.MetricName, "dummy_request_duration_seconds_sum", "job", fixtureLabels.job, "pod", podLabel, "route", routeLabel), cfg.internalLabels), timestamp, observations*0.42); err != nil {
					return err
				}
				if err := appendFloat(app, withInternalLabels(labels.FromStrings(labels.MetricName, "dummy_request_duration_seconds_count", "job", fixtureLabels.job, "pod", podLabel, "route", routeLabel), cfg.internalLabels), timestamp, observations); err != nil {
					return err
				}
				for bucketIndex, bound := range classicBounds {
					count := math.Min(observations, float64((bucketIndex+1)*(i+2)))
					if err := appendFloat(app, withInternalLabels(labels.FromStrings(labels.MetricName, "dummy_request_duration_seconds_bucket", "job", fixtureLabels.job, "le", strconv.FormatFloat(bound, 'g', -1, 64), "pod", podLabel, "route", routeLabel), cfg.internalLabels), timestamp, count); err != nil {
						return err
					}
				}
			}
		}

		for series := 0; series < nativeSeries; series++ {
			seriesLabel := fixtureLabels.series[series]
			for pod := 0; pod < pods; pod++ {
				podLabel := fixtureLabels.pods[pod]
				if _, err := app.AppendHistogram(0, withInternalLabels(labels.FromStrings(labels.MetricName, "dummy_native_histogram", "job", fixtureLabels.job, "pod", podLabel, "series", seriesLabel), cfg.internalLabels), timestamp, nativeHistogram(i+series+pod), nil); err != nil {
					_ = app.Rollback()
					return fmt.Errorf("append native histogram: %w", err)
				}
				if _, err := app.AppendHistogram(0, withInternalLabels(labels.FromStrings(labels.MetricName, "dummy_float_native_histogram", "job", fixtureLabels.job, "pod", podLabel, "series", seriesLabel), cfg.internalLabels), timestamp, nil, floatNativeHistogram(i+series+pod)); err != nil {
					_ = app.Rollback()
					return fmt.Errorf("append float native histogram: %w", err)
				}
			}
		}
		if err := app.Commit(); err != nil {
			return fmt.Errorf("commit samples: %w", err)
		}
	}
	return nil
}

type fixtureLabelValues struct {
	instances []string
	job       string
	pods      []string
	routes    []string
	series    []string
}

func newFixtureLabelValues(cfg config) fixtureLabelValues {
	instances, pods, routes, nativeSeries := cfg.dimensions()
	return fixtureLabelValues{
		instances: labelValues("instance", instances, 1),
		job:       labelValues("job", 1, 3)[0],
		pods:      labelValues("pod", pods, 5),
		routes:    labelValues("route", routes, 7),
		series:    labelValues("series", nativeSeries, 11),
	}
}

func labelValues(prefix string, count, offset int) []string {
	values := make([]string, count)
	for index := range values {
		word := fixtureLabelDictionary[(index*7+offset)%len(fixtureLabelDictionary)]
		values[index] = fmt.Sprintf("%s-%s-%03d", prefix, word, index)
	}
	return values
}

func appendFloat(app storage.Appender, labels labels.Labels, timestamp int64, value float64) error {
	if _, err := app.Append(0, labels, timestamp, value); err != nil {
		_ = app.Rollback()
		return fmt.Errorf("append float sample: %w", err)
	}
	return nil
}

func withInternalLabels(series labels.Labels, internal map[string]string) labels.Labels {
	builder := labels.NewBuilder(series)
	for name, value := range internal {
		builder.Set(name, value)
	}
	return builder.Labels()
}

func nativeHistogram(i int) *histogram.Histogram {
	return &histogram.Histogram{
		Schema:          0,
		Count:           uint64(24 + i/2),
		Sum:             float64(i) * 0.5,
		ZeroThreshold:   0.001,
		ZeroCount:       uint64(12 + i/2),
		PositiveSpans:   []histogram.Span{{Offset: 0, Length: 3}},
		PositiveBuckets: []int64{2, 1, 1},
		NegativeSpans:   []histogram.Span{{Offset: 0, Length: 2}},
		NegativeBuckets: []int64{1, 1},
	}
}

func floatNativeHistogram(i int) *histogram.FloatHistogram {
	return &histogram.FloatHistogram{
		Schema:          0,
		Count:           float64(15 + i),
		Sum:             float64(i) * 0.75,
		ZeroThreshold:   0.01,
		ZeroCount:       5.5 + float64(i)/10,
		PositiveSpans:   []histogram.Span{{Offset: -1, Length: 3}},
		PositiveBuckets: []float64{1, 0.5, 0.25},
		NegativeSpans:   []histogram.Span{{Offset: 1, Length: 2}},
		NegativeBuckets: []float64{0.5, 0.25},
	}
}

func enrichMetadata(ctx context.Context, blockDir string, externalLabels map[string]string, source metadata.SourceType, resolution int64) (*metadata.Meta, error) {
	meta, err := metadata.ReadFromDir(blockDir)
	if err != nil {
		return nil, fmt.Errorf("read raw block metadata: %w", err)
	}
	stats, err := block.GatherIndexHealthStats(ctx, log.NewNopLogger(), filepath.Join(blockDir, "index"), meta.MinTime, meta.MaxTime)
	if err != nil {
		return nil, fmt.Errorf("gather index stats: %w", err)
	}
	files, err := collectFiles(blockDir)
	if err != nil {
		return nil, err
	}
	return metadata.InjectThanos(log.NewNopLogger(), blockDir, metadata.Thanos{
		Labels:     externalLabels,
		Downsample: metadata.ThanosDownsample{Resolution: resolution},
		Source:     source,
		Files:      files,
		IndexStats: metadata.IndexStats{SeriesMaxSize: stats.SeriesMaxSize},
		UploadTime: time.Now().UTC(),
	}, nil)
}

func collectFiles(blockDir string) ([]metadata.File, error) {
	files := make([]metadata.File, 0)
	err := filepath.WalkDir(blockDir, func(path string, entry os.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if entry.IsDir() || filepath.Base(path) == "meta.json" {
			return nil
		}
		relativePath, err := filepath.Rel(blockDir, path)
		if err != nil {
			return fmt.Errorf("make block file path relative: %w", err)
		}
		hash, err := metadata.CalculateHash(path, metadata.SHA256Func, log.NewNopLogger())
		if err != nil {
			return fmt.Errorf("hash block file %s: %w", relativePath, err)
		}
		info, err := entry.Info()
		if err != nil {
			return fmt.Errorf("stat block file %s: %w", relativePath, err)
		}
		files = append(files, metadata.File{
			RelPath:   filepath.ToSlash(relativePath),
			SizeBytes: info.Size(),
			Hash:      &hash,
		})
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("walk block files: %w", err)
	}
	sort.Slice(files, func(i, j int) bool { return files[i].RelPath < files[j].RelPath })
	return files, nil
}

func create5mBlock(ctx context.Context, output, rawID string, rawMeta *metadata.Meta) (string, *metadata.Meta, error) {
	rawDir := filepath.Join(output, rawID)
	rawBlock, err := tsdb.OpenBlock(logutil.GoKitLogToSlog(log.NewNopLogger()), rawDir, downsample.NewPool(), tsdb.DefaultPostingsDecoderFactory)
	if err != nil {
		return "", nil, fmt.Errorf("open raw block for downsampling: %w", err)
	}
	defer rawBlock.Close()

	id, err := downsample.Downsample(ctx, log.NewNopLogger(), rawMeta, rawBlock, output, downsample.ResLevel1)
	if err != nil {
		return "", nil, fmt.Errorf("downsample raw block to 5 minutes: %w", err)
	}
	downsampledDir := filepath.Join(output, id.String())
	meta, err := enrichMetadata(ctx, downsampledDir, rawMeta.Thanos.Labels, metadata.CompactorSource, downsample.ResLevel1)
	if err != nil {
		return "", nil, fmt.Errorf("enrich 5m metadata: %w", err)
	}
	return id.String(), meta, nil
}

func create1hBlock(ctx context.Context, output, fiveMinuteID string, fiveMinuteMeta *metadata.Meta) (string, error) {
	fiveMinuteDir := filepath.Join(output, fiveMinuteID)
	fiveMinuteBlock, err := tsdb.OpenBlock(logutil.GoKitLogToSlog(log.NewNopLogger()), fiveMinuteDir, downsample.NewPool(), tsdb.DefaultPostingsDecoderFactory)
	if err != nil {
		return "", fmt.Errorf("open 5m block for downsampling: %w", err)
	}
	defer fiveMinuteBlock.Close()

	id, err := downsample.Downsample(ctx, log.NewNopLogger(), fiveMinuteMeta, fiveMinuteBlock, output, downsample.ResLevel2)
	if err != nil {
		return "", fmt.Errorf("downsample 5m block to 1 hour: %w", err)
	}
	hourlyDir := filepath.Join(output, id.String())
	if _, err := enrichMetadata(ctx, hourlyDir, fiveMinuteMeta.Thanos.Labels, metadata.CompactorSource, downsample.ResLevel2); err != nil {
		return "", fmt.Errorf("enrich 1h metadata: %w", err)
	}
	return id.String(), nil
}
