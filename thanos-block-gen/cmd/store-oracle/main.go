// store-oracle queries the Go Thanos BucketStore and prints a stable JSON result.
package main

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"flag"
	"fmt"
	"math"
	"os"
	"strings"

	"github.com/go-kit/log"
	"github.com/prometheus/prometheus/model/histogram"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
	"github.com/thanos-io/objstore"
	"github.com/thanos-io/objstore/providers/filesystem"
	"github.com/thanos-io/thanos/pkg/block"
	"github.com/thanos-io/thanos/pkg/store"
	"github.com/thanos-io/thanos/pkg/store/storepb"
)

type oracleSeries struct {
	Labels map[string]string `json:"labels"`
	Chunks []oracleChunk     `json:"chunks"`
}

type oracleChunk struct {
	MinTime    int64                   `json:"min_time"`
	MaxTime    int64                   `json:"max_time"`
	Encoding   storepb.Chunk_Encoding  `json:"encoding"`
	Data       string                  `json:"data"`
	Hash       uint64                  `json:"hash"`
	Samples    []oracleSample          `json:"samples,omitempty"`
	Histograms []oracleHistogramSample `json:"histograms,omitempty"`
	Count      *oracleEncodedChunk     `json:"count,omitempty"`
	Sum        *oracleEncodedChunk     `json:"sum,omitempty"`
	Min        *oracleEncodedChunk     `json:"min,omitempty"`
	Max        *oracleEncodedChunk     `json:"max,omitempty"`
	Counter    *oracleEncodedChunk     `json:"counter,omitempty"`
}

type oracleEncodedChunk struct {
	Encoding   storepb.Chunk_Encoding  `json:"encoding"`
	Data       string                  `json:"data"`
	Hash       uint64                  `json:"hash"`
	Samples    []oracleSample          `json:"samples,omitempty"`
	Histograms []oracleHistogramSample `json:"histograms,omitempty"`
}

type oracleSample struct {
	Timestamp int64  `json:"timestamp"`
	ValueBits uint64 `json:"value_bits"`
}

type oracleHistogramSample struct {
	Timestamp          int64        `json:"timestamp"`
	Kind               string       `json:"kind"`
	CounterResetHint   int          `json:"counter_reset_hint"`
	Schema             int32        `json:"schema"`
	Count              uint64       `json:"count"`
	SumBits            uint64       `json:"sum_bits"`
	ZeroThresholdBits  uint64       `json:"zero_threshold_bits"`
	ZeroCount          uint64       `json:"zero_count"`
	PositiveSpans      []oracleSpan `json:"positive_spans"`
	PositiveBuckets    []int64      `json:"positive_buckets,omitempty"`
	PositiveBucketBits []uint64     `json:"positive_bucket_bits,omitempty"`
	NegativeSpans      []oracleSpan `json:"negative_spans"`
	NegativeBuckets    []int64      `json:"negative_buckets,omitempty"`
	NegativeBucketBits []uint64     `json:"negative_bucket_bits,omitempty"`
	CustomValueBits    []uint64     `json:"custom_value_bits,omitempty"`
}

type oracleSpan struct {
	Offset int32  `json:"offset"`
	Length uint32 `json:"length"`
}

type seriesServer struct {
	storepb.Store_SeriesServer
	ctx      context.Context
	series   []*storepb.Series
	warnings []string
}

func (s *seriesServer) Context() context.Context {
	return s.ctx
}

func (s *seriesServer) Send(response *storepb.SeriesResponse) error {
	if warning := response.GetWarning(); warning != "" {
		s.warnings = append(s.warnings, warning)
	}
	if series := response.GetSeries(); series != nil {
		s.series = append(s.series, series)
	}
	if batch := response.GetBatch(); batch != nil {
		s.series = append(s.series, batch.Series...)
	}
	return nil
}

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "store-oracle:", err)
		os.Exit(1)
	}
}

func run() error {
	var bucketDir string
	var metric string
	var aggregateNames string
	var maxResolution int64
	flag.StringVar(&bucketDir, "bucket", "", "filesystem bucket containing Thanos blocks")
	flag.StringVar(&metric, "metric", "", "metric name to query")
	flag.StringVar(&aggregateNames, "aggregates", "raw", "comma-separated StoreAPI aggregates")
	flag.Int64Var(&maxResolution, "max-resolution", 0, "maximum StoreAPI resolution window in milliseconds")
	flag.Parse()
	if bucketDir == "" || metric == "" {
		return fmt.Errorf("--bucket and --metric are required")
	}

	cacheDir, err := os.MkdirTemp("", "thanos-store-oracle-")
	if err != nil {
		return err
	}
	defer os.RemoveAll(cacheDir)

	bucket, err := filesystem.NewBucket(bucketDir)
	if err != nil {
		return fmt.Errorf("open filesystem bucket: %w", err)
	}
	defer bucket.Close()

	logger := log.NewNopLogger()
	instrumentedBucket := objstore.WithNoopInstr(bucket)
	fetcher, err := block.NewMetaFetcher(
		logger,
		4,
		instrumentedBucket,
		block.NewConcurrentLister(logger, instrumentedBucket),
		cacheDir,
		nil,
		nil,
	)
	if err != nil {
		return fmt.Errorf("create metadata fetcher: %w", err)
	}
	bucketStore, err := store.NewBucketStore(
		instrumentedBucket,
		fetcher,
		cacheDir,
		store.NewChunksLimiterFactory(0),
		store.NewSeriesLimiterFactory(0),
		store.NewBytesLimiterFactory(0),
		store.NewGapBasedPartitioner(store.PartitionerMaxGapSize),
		4,
		store.DefaultPostingOffsetInMemorySampling,
		false,
		false,
		0,
	)
	if err != nil {
		return fmt.Errorf("create bucket store: %w", err)
	}
	defer bucketStore.Close()
	if err := bucketStore.SyncBlocks(context.Background()); err != nil {
		return fmt.Errorf("sync blocks: %w", err)
	}

	aggregates, err := parseAggregates(aggregateNames)
	if err != nil {
		return err
	}
	server := &seriesServer{ctx: context.Background()}
	if err := bucketStore.Series(&storepb.SeriesRequest{
		MinTime:                 math.MinInt64,
		MaxTime:                 math.MaxInt64,
		Matchers:                []storepb.LabelMatcher{{Type: storepb.LabelMatcher_EQ, Name: "__name__", Value: metric}},
		Aggregates:              aggregates,
		MaxResolutionWindow:     maxResolution,
		PartialResponseStrategy: storepb.PartialResponseStrategy_ABORT,
	}, server); err != nil {
		return fmt.Errorf("query bucket store: %w", err)
	}
	if len(server.warnings) != 0 {
		return fmt.Errorf("bucket store returned warnings: %v", server.warnings)
	}

	result := make([]oracleSeries, 0, len(server.series))
	for _, series := range server.series {
		converted := oracleSeries{
			Labels: make(map[string]string, len(series.Labels)),
			Chunks: make([]oracleChunk, 0, len(series.Chunks)),
		}
		for _, label := range series.Labels {
			converted.Labels[label.Name] = label.Value
		}
		for _, chunk := range series.Chunks {
			convertedChunk := oracleChunk{
				MinTime: chunk.MinTime,
				MaxTime: chunk.MaxTime,
			}
			if chunk.Raw != nil {
				encoded, err := convertChunk(chunk.Raw)
				if err != nil {
					return err
				}
				convertedChunk.Encoding = encoded.Encoding
				convertedChunk.Data = encoded.Data
				convertedChunk.Hash = encoded.Hash
				convertedChunk.Samples = encoded.Samples
				convertedChunk.Histograms = encoded.Histograms
			}
			convertedChunk.Count, err = convertOptionalChunk(chunk.Count)
			if err != nil {
				return err
			}
			convertedChunk.Sum, err = convertOptionalChunk(chunk.Sum)
			if err != nil {
				return err
			}
			convertedChunk.Min, err = convertOptionalChunk(chunk.Min)
			if err != nil {
				return err
			}
			convertedChunk.Max, err = convertOptionalChunk(chunk.Max)
			if err != nil {
				return err
			}
			convertedChunk.Counter, err = convertOptionalChunk(chunk.Counter)
			if err != nil {
				return err
			}
			converted.Chunks = append(converted.Chunks, convertedChunk)
		}
		result = append(result, converted)
	}
	return json.NewEncoder(os.Stdout).Encode(result)
}

func parseAggregates(names string) ([]storepb.Aggr, error) {
	values := make([]storepb.Aggr, 0)
	for _, name := range strings.Split(names, ",") {
		aggregate, ok := map[string]storepb.Aggr{
			"raw":     storepb.Aggr_RAW,
			"count":   storepb.Aggr_COUNT,
			"sum":     storepb.Aggr_SUM,
			"min":     storepb.Aggr_MIN,
			"max":     storepb.Aggr_MAX,
			"counter": storepb.Aggr_COUNTER,
		}[strings.ToLower(strings.TrimSpace(name))]
		if !ok {
			return nil, fmt.Errorf("unknown aggregate %q", name)
		}
		values = append(values, aggregate)
	}
	return values, nil
}

func convertChunk(chunk *storepb.Chunk) (oracleEncodedChunk, error) {
	samples, histograms, err := decodeSamples(chunk)
	if err != nil {
		return oracleEncodedChunk{}, err
	}
	return oracleEncodedChunk{
		Encoding:   chunk.Type,
		Data:       hex.EncodeToString(chunk.Data),
		Hash:       chunk.Hash,
		Samples:    samples,
		Histograms: histograms,
	}, nil
}

func convertOptionalChunk(chunk *storepb.Chunk) (*oracleEncodedChunk, error) {
	if chunk == nil {
		return nil, nil
	}
	converted, err := convertChunk(chunk)
	if err != nil {
		return nil, err
	}
	return &converted, nil
}

func decodeSamples(chunk *storepb.Chunk) ([]oracleSample, []oracleHistogramSample, error) {
	encoding := map[storepb.Chunk_Encoding]chunkenc.Encoding{
		storepb.Chunk_XOR:             chunkenc.EncXOR,
		storepb.Chunk_HISTOGRAM:       chunkenc.EncHistogram,
		storepb.Chunk_FLOAT_HISTOGRAM: chunkenc.EncFloatHistogram,
	}[chunk.Type]
	decoded, err := chunkenc.FromData(encoding, chunk.Data)
	if err != nil {
		return nil, nil, fmt.Errorf("decode chunk: %w", err)
	}
	iterator := decoded.Iterator(nil)
	var samples []oracleSample
	var histograms []oracleHistogramSample
	for valueType := iterator.Next(); valueType != chunkenc.ValNone; valueType = iterator.Next() {
		switch valueType {
		case chunkenc.ValFloat:
			timestamp, sampleValue := iterator.At()
			samples = append(samples, oracleSample{
				Timestamp: timestamp,
				ValueBits: math.Float64bits(sampleValue),
			})
		case chunkenc.ValHistogram:
			timestamp, value := iterator.AtHistogram(nil)
			histograms = append(histograms, integerHistogramSample(timestamp, value))
		case chunkenc.ValFloatHistogram:
			timestamp, value := iterator.AtFloatHistogram(nil)
			histograms = append(histograms, floatHistogramSample(timestamp, value))
		default:
			return nil, nil, fmt.Errorf("chunk returned value type %s", valueType)
		}
	}
	if err := iterator.Err(); err != nil {
		return nil, nil, fmt.Errorf("iterate chunk: %w", err)
	}
	return samples, histograms, nil
}

func integerHistogramSample(timestamp int64, value *histogram.Histogram) oracleHistogramSample {
	return oracleHistogramSample{
		Timestamp:         timestamp,
		Kind:              "histogram",
		CounterResetHint:  int(value.CounterResetHint),
		Schema:            value.Schema,
		Count:             value.Count,
		SumBits:           math.Float64bits(value.Sum),
		ZeroThresholdBits: math.Float64bits(value.ZeroThreshold),
		ZeroCount:         value.ZeroCount,
		PositiveSpans:     convertSpans(value.PositiveSpans),
		PositiveBuckets:   value.PositiveBuckets,
		NegativeSpans:     convertSpans(value.NegativeSpans),
		NegativeBuckets:   value.NegativeBuckets,
		CustomValueBits:   floatBits(value.CustomValues),
	}
}

func floatHistogramSample(timestamp int64, value *histogram.FloatHistogram) oracleHistogramSample {
	return oracleHistogramSample{
		Timestamp:          timestamp,
		Kind:               "float_histogram",
		CounterResetHint:   int(value.CounterResetHint),
		Schema:             value.Schema,
		Count:              math.Float64bits(value.Count),
		SumBits:            math.Float64bits(value.Sum),
		ZeroThresholdBits:  math.Float64bits(value.ZeroThreshold),
		ZeroCount:          math.Float64bits(value.ZeroCount),
		PositiveSpans:      convertSpans(value.PositiveSpans),
		PositiveBucketBits: floatBits(value.PositiveBuckets),
		NegativeSpans:      convertSpans(value.NegativeSpans),
		NegativeBucketBits: floatBits(value.NegativeBuckets),
		CustomValueBits:    floatBits(value.CustomValues),
	}
}

func convertSpans(spans []histogram.Span) []oracleSpan {
	converted := make([]oracleSpan, len(spans))
	for i, span := range spans {
		converted[i] = oracleSpan{Offset: span.Offset, Length: span.Length}
	}
	return converted
}

func floatBits(values []float64) []uint64 {
	bits := make([]uint64, len(values))
	for i, value := range values {
		bits[i] = math.Float64bits(value)
	}
	return bits
}
