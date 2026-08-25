package storeapi

import (
	"bytes"
	"context"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/thanos-io/thanos/pkg/block/metadata"
	"github.com/thanos-io/thanos/pkg/info/infopb"
	"github.com/thanos-io/thanos/pkg/store/labelpb"
	"github.com/thanos-io/thanos/pkg/store/storepb"
	"github.com/thanos-io/thanos/pkg/testutil/e2eutil"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

const (
	conformanceMinTime = int64(1_700_000_000_000)
	conformanceMaxTime = conformanceMinTime + 60_000
)

func TestThanosV1ReaderStoreAPIConformance(t *testing.T) {
	readerBinary := os.Getenv("THANOS_V1_READER_BIN")
	if readerBinary == "" {
		t.Skip("set THANOS_V1_READER_BIN to run the external StoreAPI conformance suite")
	}

	server := startConformanceReader(t, readerBinary)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	t.Cleanup(cancel)

	conn, err := grpc.NewClient(server.address, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("connect to reader: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })

	store := storepb.NewStoreClient(conn)
	info := infopb.NewInfoClient(conn)
	waitForStore(t, ctx, store)

	t.Run("Info", func(t *testing.T) {
		response, err := info.Info(ctx, &infopb.InfoRequest{})
		if err != nil {
			t.Fatalf("Info: %v", err)
		}
		if response.ComponentType != "store" {
			t.Fatalf("component type = %q, want store", response.ComponentType)
		}
		if response.Store == nil {
			t.Fatal("Info returned no Store metadata")
		}
		if response.Store.MinTime != conformanceMinTime || response.Store.MaxTime != conformanceMaxTime {
			t.Fatalf("store range = [%d, %d], want [%d, %d]", response.Store.MinTime, response.Store.MaxTime, conformanceMinTime, conformanceMaxTime)
		}
		if response.Store.SupportsSharding || response.Store.SupportsWithoutReplicaLabels {
			t.Fatal("reader advertised an unsupported StoreAPI capability")
		}
		if got := labelSets(response.LabelSets); !equalStrings(got, []string{"region=eu-west"}) {
			t.Fatalf("label sets = %v, want [region=eu-west]", got)
		}
	})

	t.Run("Series matcher acceptance", func(t *testing.T) {
		cases := []struct {
			name     string
			matchers []storepb.LabelMatcher
			want     []string
		}{
			{"equal", []storepb.LabelMatcher{matcher(storepb.LabelMatcher_EQ, "n", "1")}, []string{"__name__=up,i=a,n=1,region=eu-west", "__name__=up,i=b,n=1,region=eu-west", "__name__=up,n=1,region=eu-west"}},
			{"not equal", []storepb.LabelMatcher{matcher(storepb.LabelMatcher_NEQ, "n", "1")}, []string{"__name__=up,n=2,region=eu-west", "__name__=up,n=2.5,region=eu-west"}},
			{"regex", []storepb.LabelMatcher{matcher(storepb.LabelMatcher_RE, "i", ".+")}, []string{"__name__=up,i=a,n=1,region=eu-west", "__name__=up,i=b,n=1,region=eu-west"}},
			{"negative regex", []storepb.LabelMatcher{matcher(storepb.LabelMatcher_NRE, "i", "^a$")}, []string{"__name__=up,i=b,n=1,region=eu-west", "__name__=up,n=1,region=eu-west", "__name__=up,n=2,region=eu-west", "__name__=up,n=2.5,region=eu-west"}},
			{"missing label", []storepb.LabelMatcher{matcher(storepb.LabelMatcher_EQ, "missing", "")}, []string{"__name__=up,i=a,n=1,region=eu-west", "__name__=up,i=b,n=1,region=eu-west", "__name__=up,n=1,region=eu-west", "__name__=up,n=2,region=eu-west", "__name__=up,n=2.5,region=eu-west"}},
			{"external label", []storepb.LabelMatcher{matcher(storepb.LabelMatcher_EQ, "region", "eu-west")}, []string{"__name__=up,i=a,n=1,region=eu-west", "__name__=up,i=b,n=1,region=eu-west", "__name__=up,n=1,region=eu-west", "__name__=up,n=2,region=eu-west", "__name__=up,n=2.5,region=eu-west"}},
		}
		for _, tc := range cases {
			t.Run(tc.name, func(t *testing.T) {
				series := querySeries(t, ctx, store, &storepb.SeriesRequest{
					MinTime:    conformanceMinTime,
					MaxTime:    conformanceMaxTime,
					Matchers:   tc.matchers,
					SkipChunks: true,
				})
				if got := seriesLabels(series); !equalStrings(got, tc.want) {
					t.Fatalf("labels = %v, want %v", got, tc.want)
				}
			})
		}
	})

	t.Run("Series response options", func(t *testing.T) {
		series := querySeries(t, ctx, store, &storepb.SeriesRequest{
			MinTime:           conformanceMinTime,
			MaxTime:           conformanceMaxTime,
			Matchers:          []storepb.LabelMatcher{matcher(storepb.LabelMatcher_RE, "n", ".*")},
			Limit:             2,
			ResponseBatchSize: 2,
		})
		if len(series) != 2 {
			t.Fatalf("limited series = %d, want 2", len(series))
		}
		for _, result := range series {
			if len(result.Chunks) == 0 || result.Chunks[0].Raw == nil || len(result.Chunks[0].Raw.Data) == 0 {
				t.Fatalf("Series returned no raw XOR chunk: %#v", result.Chunks)
			}
		}
	})

	t.Run("Label APIs", func(t *testing.T) {
		names, err := store.LabelNames(ctx, &storepb.LabelNamesRequest{
			Start:    conformanceMinTime,
			End:      conformanceMaxTime,
			Matchers: []storepb.LabelMatcher{matcher(storepb.LabelMatcher_EQ, "n", "1")},
		})
		if err != nil {
			t.Fatalf("LabelNames: %v", err)
		}
		if !equalStrings(names.Names, []string{"__name__", "i", "n", "region"}) {
			t.Fatalf("label names = %v", names.Names)
		}

		values, err := store.LabelValues(ctx, &storepb.LabelValuesRequest{
			Label:    "n",
			Start:    conformanceMinTime,
			End:      conformanceMaxTime,
			Matchers: []storepb.LabelMatcher{matcher(storepb.LabelMatcher_RE, "i", ".*")},
		})
		if err != nil {
			t.Fatalf("LabelValues: %v", err)
		}
		if !equalStrings(values.Values, []string{"1", "2", "2.5"}) {
			t.Fatalf("label values = %v", values.Values)
		}

		limited, err := store.LabelValues(ctx, &storepb.LabelValuesRequest{Label: "n", Start: conformanceMinTime, End: conformanceMaxTime, Limit: 2})
		if err != nil {
			t.Fatalf("limited LabelValues: %v", err)
		}
		if !equalStrings(limited.Values, []string{"1", "2"}) {
			t.Fatalf("limited label values = %v", limited.Values)
		}
	})

	t.Run("unsupported and invalid requests", func(t *testing.T) {
		_, err := store.LabelValues(ctx, &storepb.LabelValuesRequest{Label: "", Start: conformanceMinTime, End: conformanceMaxTime})
		if code := status.Code(err); code.String() != "InvalidArgument" {
			t.Fatalf("empty label status = %v, want InvalidArgument", code)
		}
		stream, err := store.Series(ctx, &storepb.SeriesRequest{
			MinTime:   conformanceMinTime,
			MaxTime:   conformanceMaxTime,
			ShardInfo: &storepb.ShardInfo{ShardIndex: 0, TotalShards: 1},
		})
		if err == nil {
			_, err = stream.Recv()
		}
		if code := status.Code(err); code.String() != "Unimplemented" {
			t.Fatalf("sharding status = %v, want Unimplemented", code)
		}
	})
}

type readerProcess struct {
	address string
	command *exec.Cmd
	output  *bytes.Buffer
}

func startConformanceReader(t *testing.T, binary string) readerProcess {
	t.Helper()
	root := t.TempDir()
	blocks := filepath.Join(root, "blocks")
	createConformanceBlock(t, blocks)
	listener := reserveListener(t)
	metricsListener := reserveListener(t)
	address := listener.Addr().String()
	metricsAddress := metricsListener.Addr().String()
	listenerFD := listenerFile(t, listener)
	metricsListenerFD := listenerFile(t, metricsListener)
	configPath := filepath.Join(root, "reader.toml")
	config := fmt.Sprintf("listen_addr = %q\nmetrics_listen_addr = %q\nindex_cache_location = %q\n\n[[repositories]]\nname = \"acceptance\"\nuri = %q\n", address, metricsAddress, filepath.Join(root, "cache"), "file://"+blocks)
	if err := os.WriteFile(configPath, []byte(config), 0o600); err != nil {
		t.Fatalf("write reader config: %v", err)
	}

	var output bytes.Buffer
	command := exec.Command(binary)
	command.Env = append(
		os.Environ(),
		"THANOS_READER_CONFIG="+configPath,
		"THANOS_READER_LISTEN_FD=3",
		"THANOS_READER_METRICS_LISTEN_FD=4",
		"OTEL_SDK_DISABLED=true",
		"RUST_LOG=info",
	)
	command.ExtraFiles = []*os.File{listenerFD, metricsListenerFD}
	command.Stdout = &output
	command.Stderr = &output
	if err := command.Start(); err != nil {
		_ = listenerFD.Close()
		_ = metricsListenerFD.Close()
		_ = listener.Close()
		_ = metricsListener.Close()
		t.Fatalf("start reader: %v", err)
	}
	_ = listenerFD.Close()
	_ = metricsListenerFD.Close()
	_ = listener.Close()
	_ = metricsListener.Close()
	t.Cleanup(func() {
		if command.Process != nil {
			_ = command.Process.Kill()
		}
		_ = command.Wait()
		if t.Failed() {
			t.Logf("reader output:\n%s", output.String())
		}
	})
	return readerProcess{address: address, command: command, output: &output}
}

func createConformanceBlock(t *testing.T, dir string) {
	t.Helper()
	series := []labels.Labels{
		labels.FromStrings(labels.MetricName, "up", "n", "1"),
		labels.FromStrings(labels.MetricName, "up", "n", "1", "i", "a"),
		labels.FromStrings(labels.MetricName, "up", "n", "1", "i", "b"),
		labels.FromStrings(labels.MetricName, "up", "n", "2"),
		labels.FromStrings(labels.MetricName, "up", "n", "2.5"),
	}
	if _, err := e2eutil.CreateBlock(context.Background(), dir, series, 2, conformanceMinTime, conformanceMaxTime, labels.FromStrings("region", "eu-west"), 0, metadata.NoneFunc, nil); err != nil {
		t.Fatalf("create upstream Thanos fixture block: %v", err)
	}
}

func waitForStore(t *testing.T, ctx context.Context, store storepb.StoreClient) {
	t.Helper()
	for {
		_, err := store.LabelNames(ctx, &storepb.LabelNamesRequest{Start: conformanceMinTime, End: conformanceMaxTime})
		if err == nil {
			return
		}
		if ctx.Err() != nil {
			t.Fatalf("reader did not become ready: %v", err)
		}
		time.Sleep(50 * time.Millisecond)
	}
}

func querySeries(t *testing.T, ctx context.Context, store storepb.StoreClient, request *storepb.SeriesRequest) []*storepb.Series {
	t.Helper()
	stream, err := store.Series(ctx, request)
	if err != nil {
		t.Fatalf("Series: %v", err)
	}
	var series []*storepb.Series
	for {
		response, err := stream.Recv()
		if err != nil {
			if strings.Contains(err.Error(), "EOF") {
				return series
			}
			t.Fatalf("receive Series response: %v", err)
		}
		switch result := response.Result.(type) {
		case *storepb.SeriesResponse_Series:
			series = append(series, result.Series)
		case *storepb.SeriesResponse_Batch:
			series = append(series, result.Batch.Series...)
		case *storepb.SeriesResponse_Warning:
			t.Fatalf("StoreAPI warning: %s", result.Warning)
		default:
			t.Fatalf("unexpected Series response: %#v", response.Result)
		}
	}
}

func matcher(kind storepb.LabelMatcher_Type, name, value string) storepb.LabelMatcher {
	return storepb.LabelMatcher{Type: kind, Name: name, Value: value}
}

func seriesLabels(series []*storepb.Series) []string {
	result := make([]string, 0, len(series))
	for _, current := range series {
		result = append(result, zLabelsKey(current.Labels))
	}
	sort.Strings(result)
	return result
}

func labelSets(sets []labelpb.ZLabelSet) []string {
	result := make([]string, 0, len(sets))
	for _, set := range sets {
		result = append(result, zLabelsKey(set.Labels))
	}
	sort.Strings(result)
	return result
}

func zLabelsKey(values []labelpb.ZLabel) string {
	pairs := make([]string, 0, len(values))
	for _, value := range values {
		pairs = append(pairs, value.Name+"="+value.Value)
	}
	sort.Strings(pairs)
	return strings.Join(pairs, ",")
}

func equalStrings(got, want []string) bool {
	return strings.Join(got, "\n") == strings.Join(want, "\n")
}

func reserveListener(t *testing.T) *net.TCPListener {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("reserve listener: %v", err)
	}
	tcpListener, ok := listener.(*net.TCPListener)
	if !ok {
		_ = listener.Close()
		t.Fatal("reserved listener is not TCP")
	}
	return tcpListener
}

func listenerFile(t *testing.T, listener *net.TCPListener) *os.File {
	t.Helper()
	file, err := listener.File()
	if err != nil {
		_ = listener.Close()
		t.Fatalf("duplicate listener file descriptor: %v", err)
	}
	return file
}
