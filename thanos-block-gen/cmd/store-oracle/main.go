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

	"github.com/go-kit/log"
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
	MinTime  int64                  `json:"min_time"`
	MaxTime  int64                  `json:"max_time"`
	Encoding storepb.Chunk_Encoding `json:"encoding"`
	Data     string                 `json:"data"`
	Hash     uint64                 `json:"hash"`
	Samples  []oracleSample         `json:"samples,omitempty"`
}

type oracleSample struct {
	Timestamp int64  `json:"timestamp"`
	ValueBits uint64 `json:"value_bits"`
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
	flag.StringVar(&bucketDir, "bucket", "", "filesystem bucket containing Thanos blocks")
	flag.StringVar(&metric, "metric", "", "metric name to query")
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

	server := &seriesServer{ctx: context.Background()}
	if err := bucketStore.Series(&storepb.SeriesRequest{
		MinTime:  math.MinInt64,
		MaxTime:  math.MaxInt64,
		Matchers: []storepb.LabelMatcher{{Type: storepb.LabelMatcher_EQ, Name: "__name__", Value: metric}},
		Aggregates: []storepb.Aggr{
			storepb.Aggr_RAW,
		},
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
			if chunk.Raw == nil {
				return fmt.Errorf("series returned a non-raw chunk")
			}
			samples, err := decodeFloatSamples(chunk.Raw)
			if err != nil {
				return err
			}
			converted.Chunks = append(converted.Chunks, oracleChunk{
				MinTime:  chunk.MinTime,
				MaxTime:  chunk.MaxTime,
				Encoding: chunk.Raw.Type,
				Data:     hex.EncodeToString(chunk.Raw.Data),
				Hash:     chunk.Raw.Hash,
				Samples:  samples,
			})
		}
		result = append(result, converted)
	}
	return json.NewEncoder(os.Stdout).Encode(result)
}

func decodeFloatSamples(chunk *storepb.Chunk) ([]oracleSample, error) {
	if chunk.Type != storepb.Chunk_XOR {
		return nil, nil
	}
	decoded, err := chunkenc.FromData(chunkenc.EncXOR, chunk.Data)
	if err != nil {
		return nil, fmt.Errorf("decode XOR chunk: %w", err)
	}
	iterator := decoded.Iterator(nil)
	var samples []oracleSample
	for valueType := iterator.Next(); valueType != chunkenc.ValNone; valueType = iterator.Next() {
		if valueType != chunkenc.ValFloat {
			return nil, fmt.Errorf("XOR chunk returned value type %s", valueType)
		}
		timestamp, sampleValue := iterator.At()
		samples = append(samples, oracleSample{
			Timestamp: timestamp,
			ValueBits: math.Float64bits(sampleValue),
		})
	}
	if err := iterator.Err(); err != nil {
		return nil, fmt.Errorf("iterate XOR chunk: %w", err)
	}
	return samples, nil
}
