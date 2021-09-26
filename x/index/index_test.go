package index_test

import (
	"context"
	"testing"

	"github.com/google/go-cmp/cmp"

	"github.com/tendermint/tendermint/x/feed"
	"github.com/tendermint/tendermint/x/index"
	"github.com/tendermint/tendermint/x/index/memstore"
)

func TestScan_emptyRange(t *testing.T) {
	r := index.NewReader(memstore.New())

	err := r.Scan(context.Background(), index.Query{}, func(index.Entry) error {
		return nil
	})
	if err != index.ErrSpanEmpty {
		t.Errorf("Scan empty: got %v, want %v", err, index.ErrSpanEmpty)
	}
}

func TestScan_stopEarly(t *testing.T) {
	ctx := context.Background()
	s := memstore.New()

	mustIndex(t, index.NewWriter(s), "A", "B", "stop", "C", "D")
	r := index.NewReader(s)

	var labels []string
	err := r.Scan(ctx, index.Query{}, func(e index.Entry) error {
		if e.Label == "stop" {
			return index.ErrStopScan
		}
		labels = append(labels, e.Label)
		return nil
	})
	if err != nil {
		t.Errorf("Scan: unexpected error: %v", err)
	}

	want := []string{"A", "B"}
	if diff := cmp.Diff(want, labels); diff != "" {
		t.Errorf("Wrong labels: (-want, +got)\n%s", diff)
	}
}

func mustIndex(t *testing.T, w index.Writer, labels ...string) {
	t.Helper()
	for _, label := range labels {
		if _, err := w.IndexItem(context.Background(), feed.Item{Label: label}); err != nil {
			t.Fatalf("Put %q failed: %v", label, err)
		}
	}
}
