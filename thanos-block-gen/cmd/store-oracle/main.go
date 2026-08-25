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
	"time"

	"github.com/go-kit/log"
	"github.com/gogo/protobuf/types"
	"github.com/prometheus/prometheus/model/histogram"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
	"github.com/thanos-io/objstore"
	"github.com/thanos-io/objstore/providers/filesystem"
	"github.com/thanos-io/thanos/pkg/block"
	"github.com/thanos-io/thanos/pkg/info/infopb"
	"github.com/thanos-io/thanos/pkg/store"
	"github.com/thanos-io/thanos/pkg/store/hintspb"
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

type stringFlags []string

func (f *stringFlags) String() string {
	return strings.Join(*f, ",")
}

func (f *stringFlags) Set(value string) error {
	*f = append(*f, value)
	return nil
}

type seriesServer struct {
	storepb.Store_SeriesServer
	ctx       context.Context
	series    []*storepb.Series
	warnings  []string
	responses []*storepb.SeriesResponse
}

func (s *seriesServer) Context() context.Context {
	return s.ctx
}

func (s *seriesServer) Send(response *storepb.SeriesResponse) error {
	s.responses = append(s.responses, response)
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
	var minTime int64
	var maxTime int64
	var shardEnabled bool
	var shardIndex int64
	var shardTotal int64
	var shardBy bool
	var shardLabels string
	var wireFormat bool
	var withoutReplicaLabels string
	var limit int64
	var matchLabel string
	var streamWireFormat bool
	var hintsType string
	var blockMatchers stringFlags
	var skipChunks bool
	var hintsTypeURL string
	var endpoint string
	var label string
	var seriesMatchers stringFlags
	var deletionMarkDelay time.Duration
	flag.StringVar(&bucketDir, "bucket", "", "filesystem bucket containing Thanos blocks")
	flag.StringVar(&metric, "metric", "", "metric name to query")
	flag.StringVar(&aggregateNames, "aggregates", "raw", "comma-separated StoreAPI aggregates")
	flag.Int64Var(&maxResolution, "max-resolution", 0, "maximum StoreAPI resolution window in milliseconds")
	flag.Int64Var(&minTime, "min-time", math.MinInt64, "minimum StoreAPI query timestamp")
	flag.Int64Var(&maxTime, "max-time", math.MaxInt64, "maximum StoreAPI query timestamp")
	flag.BoolVar(&shardEnabled, "shard-enabled", false, "include ShardInfo in the StoreAPI request")
	flag.Int64Var(&shardIndex, "shard-index", 0, "ShardInfo shard index")
	flag.Int64Var(&shardTotal, "shard-total", 1, "ShardInfo total shard count")
	flag.BoolVar(&shardBy, "shard-by", false, "ShardInfo grouping-by mode")
	flag.StringVar(&shardLabels, "shard-labels", "", "comma-separated ShardInfo labels")
	flag.BoolVar(&wireFormat, "wire-format", false, "emit hex-encoded Series protobufs")
	flag.StringVar(&withoutReplicaLabels, "without-replica-labels", "\x00", "comma-separated labels to remove")
	flag.Int64Var(&limit, "limit", 0, "StoreAPI series result limit")
	flag.StringVar(&matchLabel, "match-label", "", "additional exact matcher in name=value form")
	flag.BoolVar(&streamWireFormat, "stream-wire-format", false, "emit hex-encoded SeriesResponse protobufs")
	flag.StringVar(&hintsType, "hints-type", "none", "request hints type: none, request, response, unknown, invalid-url, or malformed")
	flag.Var(&blockMatchers, "block-matcher", "block matcher in TYPE:name:value form; repeatable")
	flag.BoolVar(&skipChunks, "skip-chunks", false, "omit chunks from Series responses")
	flag.StringVar(&hintsTypeURL, "hints-type-url", "", "override the request hints Any type URL")
	flag.StringVar(&endpoint, "endpoint", "series", "API endpoint: series, label-names, label-values, or info")
	flag.StringVar(&label, "label", "", "label name for the label-values endpoint")
	flag.Var(&seriesMatchers, "series-matcher", "series matcher in TYPE:name:value form; repeatable")
	flag.DurationVar(&deletionMarkDelay, "deletion-mark-delay", 24*time.Hour, "grace delay before filtering deletion-marked blocks")
	flag.Parse()
	if bucketDir == "" || (endpoint == "series" && metric == "") {
		return fmt.Errorf("--bucket is required; --metric is required for Series")
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
		[]block.MetadataFilter{
			block.NewIgnoreDeletionMarkFilter(logger, instrumentedBucket, deletionMarkDelay, 4),
			block.NewDeduplicateFilter(4),
		},
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
		true,
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
	matchers := []storepb.LabelMatcher{{Type: storepb.LabelMatcher_EQ, Name: "__name__", Value: metric}}
	if endpoint != "series" {
		matchers = nil
	}
	if matchLabel != "" {
		name, value, ok := strings.Cut(matchLabel, "=")
		if !ok || name == "" {
			return fmt.Errorf("invalid --match-label %q", matchLabel)
		}
		matchers = append(matchers, storepb.LabelMatcher{Type: storepb.LabelMatcher_EQ, Name: name, Value: value})
	}
	parsedSeriesMatchers, err := parseMatchers(seriesMatchers)
	if err != nil {
		return err
	}
	matchers = append(matchers, parsedSeriesMatchers...)
	var shardInfo *storepb.ShardInfo
	if shardEnabled {
		shardInfo = &storepb.ShardInfo{
			ShardIndex:  shardIndex,
			TotalShards: shardTotal,
			By:          shardBy,
			Labels:      splitNonEmpty(shardLabels),
		}
	}
	hints, err := requestHints(endpoint, hintsType, blockMatchers)
	if err != nil {
		return err
	}
	if hints != nil && hintsTypeURL != "" {
		hints.TypeUrl = hintsTypeURL
	}
	switch endpoint {
	case "info":
		minTime, maxTime := bucketStore.TimeRange()
		response := &infopb.InfoResponse{
			LabelSets:     bucketStore.LabelSet(),
			ComponentType: "store",
			Store: &infopb.StoreInfo{
				MinTime:                      minTime,
				MaxTime:                      maxTime,
				SupportsSharding:             true,
				SupportsWithoutReplicaLabels: true,
				TsdbInfos:                    bucketStore.TSDBInfos(),
			},
		}
		data, err := response.Marshal()
		if err != nil {
			return fmt.Errorf("marshal info response: %w", err)
		}
		return json.NewEncoder(os.Stdout).Encode(hex.EncodeToString(data))
	case "label-names":
		response, err := bucketStore.LabelNames(context.Background(), &storepb.LabelNamesRequest{
			Start:                minTime,
			End:                  maxTime,
			Hints:                hints,
			Matchers:             matchers,
			WithoutReplicaLabels: splitRequestedLabels(withoutReplicaLabels),
			Limit:                limit,
		})
		if err != nil {
			return fmt.Errorf("query bucket store label names: %w", err)
		}
		data, err := response.Marshal()
		if err != nil {
			return fmt.Errorf("marshal label names response: %w", err)
		}
		return json.NewEncoder(os.Stdout).Encode(hex.EncodeToString(data))
	case "label-values":
		response, err := bucketStore.LabelValues(context.Background(), &storepb.LabelValuesRequest{
			Label:                label,
			Start:                minTime,
			End:                  maxTime,
			Hints:                hints,
			Matchers:             matchers,
			WithoutReplicaLabels: splitRequestedLabels(withoutReplicaLabels),
			Limit:                limit,
		})
		if err != nil {
			return fmt.Errorf("query bucket store label values: %w", err)
		}
		data, err := response.Marshal()
		if err != nil {
			return fmt.Errorf("marshal label values response: %w", err)
		}
		return json.NewEncoder(os.Stdout).Encode(hex.EncodeToString(data))
	case "series":
	default:
		return fmt.Errorf("invalid --endpoint %q", endpoint)
	}
	if err := bucketStore.Series(&storepb.SeriesRequest{
		MinTime:                 minTime,
		MaxTime:                 maxTime,
		Matchers:                matchers,
		Aggregates:              aggregates,
		MaxResolutionWindow:     maxResolution,
		PartialResponseStrategy: storepb.PartialResponseStrategy_ABORT,
		ShardInfo:               shardInfo,
		WithoutReplicaLabels:    splitRequestedLabels(withoutReplicaLabels),
		Limit:                   limit,
		Hints:                   hints,
		SkipChunks:              skipChunks,
	}, server); err != nil {
		return fmt.Errorf("query bucket store: %w", err)
	}
	if len(server.warnings) != 0 {
		return fmt.Errorf("bucket store returned warnings: %v", server.warnings)
	}
	if streamWireFormat {
		encoded := make([]string, 0, len(server.responses))
		for _, response := range server.responses {
			data, err := response.Marshal()
			if err != nil {
				return fmt.Errorf("marshal response: %w", err)
			}
			encoded = append(encoded, hex.EncodeToString(data))
		}
		return json.NewEncoder(os.Stdout).Encode(encoded)
	}
	if wireFormat {
		encoded := make([]string, 0, len(server.series))
		for _, series := range server.series {
			data, err := series.Marshal()
			if err != nil {
				return fmt.Errorf("marshal StoreAPI series: %w", err)
			}
			encoded = append(encoded, fmt.Sprintf("%x", data))
		}
		return json.NewEncoder(os.Stdout).Encode(encoded)
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

func splitNonEmpty(value string) []string {
	if value == "" {
		return nil
	}
	values := strings.Split(value, ",")
	result := values[:0]
	for _, item := range values {
		if item != "" {
			result = append(result, item)
		}
	}
	return result
}

func splitRequestedLabels(value string) []string {
	if value == "\x00" {
		return nil
	}
	return strings.Split(value, ",")
}

func parseMatchers(rawMatchers []string) ([]storepb.LabelMatcher, error) {
	matchers := make([]storepb.LabelMatcher, 0, len(rawMatchers))
	for _, raw := range rawMatchers {
		rawType, remainder, ok := strings.Cut(raw, ":")
		if !ok {
			return nil, fmt.Errorf("invalid --block-matcher %q", raw)
		}
		name, value, ok := strings.Cut(remainder, ":")
		if !ok {
			return nil, fmt.Errorf("invalid --block-matcher %q", raw)
		}
		matcherType, ok := map[string]storepb.LabelMatcher_Type{
			"eq":      storepb.LabelMatcher_EQ,
			"neq":     storepb.LabelMatcher_NEQ,
			"re":      storepb.LabelMatcher_RE,
			"nre":     storepb.LabelMatcher_NRE,
			"invalid": storepb.LabelMatcher_Type(99),
		}[rawType]
		if !ok {
			return nil, fmt.Errorf("invalid block matcher type %q", rawType)
		}
		matchers = append(matchers, storepb.LabelMatcher{
			Type:  matcherType,
			Name:  name,
			Value: value,
		})
	}
	return matchers, nil
}

func requestHints(endpoint, kind string, rawMatchers []string) (*types.Any, error) {
	matchers, err := parseMatchers(rawMatchers)
	if err != nil {
		return nil, err
	}
	switch kind {
	case "none":
		return nil, nil
	case "request":
		switch endpoint {
		case "series":
			return types.MarshalAny(&hintspb.SeriesRequestHints{BlockMatchers: matchers})
		case "label-names":
			return types.MarshalAny(&hintspb.LabelNamesRequestHints{BlockMatchers: matchers})
		case "label-values":
			return types.MarshalAny(&hintspb.LabelValuesRequestHints{BlockMatchers: matchers})
		default:
			return nil, fmt.Errorf("invalid --endpoint %q", endpoint)
		}
	case "response":
		switch endpoint {
		case "series":
			return types.MarshalAny(&hintspb.SeriesResponseHints{})
		case "label-names":
			return types.MarshalAny(&hintspb.LabelNamesResponseHints{})
		case "label-values":
			return types.MarshalAny(&hintspb.LabelValuesResponseHints{})
		default:
			return nil, fmt.Errorf("invalid --endpoint %q", endpoint)
		}
	case "unknown":
		return &types.Any{TypeUrl: "type.googleapis.com/unknown.RequestHints"}, nil
	case "invalid-url":
		return &types.Any{TypeUrl: "hintspb.RequestHints"}, nil
	case "malformed":
		messageName := map[string]string{
			"series":       "SeriesRequestHints",
			"label-names":  "LabelNamesRequestHints",
			"label-values": "LabelValuesRequestHints",
		}[endpoint]
		if messageName == "" {
			return nil, fmt.Errorf("invalid --endpoint %q", endpoint)
		}
		return &types.Any{
			TypeUrl: "type.googleapis.com/hintspb." + messageName,
			Value:   []byte{0xff},
		}, nil
	default:
		return nil, fmt.Errorf("invalid --hints-type %q", kind)
	}
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
