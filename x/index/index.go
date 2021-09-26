// Package index defines an indexing interface for feed.Item values.
package index

import (
	"context"
	"errors"

	"github.com/tendermint/tendermint/x/feed"
)

var (
	// ErrStopScan is returned by the callback of a Scan method to indicate that
	// the scan should be ended without error.
	ErrStopScan = errors.New("stop scanning")

	// ErrSpanEmpty is returned when the caller requests a span of the index
	// that contains no entries.
	ErrSpanEmpty = errors.New("span is empty")
)

// Store is the interface to underlying indexing storage. The implementation
// controls how data are encoded and stored, but must be able to support a view
// of data that reflects the order in which items were written.
//
// Writing an item to the store assigns an opaque string cursor for the item.
// Cursors are not required to be ordered, meaning that the caller cannot
// compare cursor strings to find out which item was indexed earlier.
// The store is free to issue cursors that allow that comparison for its own
// use, howeever.
type Store interface {
	// Put writes the specified item to storage and returns a unique, non-empty
	// cursor for it.  It is an error if both label == "" and data == nil.
	// Each call to Put must write a new record, even if the arguments are
	// identical to a previous call.
	Put(ctx context.Context, id uint64, label string, data interface{}) (string, error)

	// Span reports the oldest and newest cursor values available in the index.
	// If no items are stored, Span reports ErrSpanEmpty.
	Span(ctx context.Context) (oldest, newest string, _ error)

	// Scan calls f with each indexed item matching the specified query, in the
	// order in which they were stored.  The scan stops when there are no
	// further items, or when f reports an error. If f reports an error, Scan
	// returns that error, otherwise nil.
	//
	// If the start and limit cursors in the query do not exist in the store,
	// Scan may either return no results (without error) or, if its cursor
	// format allows comparison, return all the results in the index with
	// cursors in the closed interval bounded by the start and limit.  In either
	// case, Scan may not report an error for missing start or limit keys.
	Scan(ctx context.Context, q Query, f func(Entry) error) error
}

// A Query records the parameters to a storage Scan.
type Query struct {
	// Match items at or after this cursor. If Start == "", the oldest cursor at
	// the point when the scan begins is used.
	Start string

	// Match items at or before the item with this cursor. If Limit == "", the
	// newest cursor at the point when the scan begins is used.
	Limit string

	// Report only items for which this predicate returns true.
	// If nil, report all items regardless of label.
	MatchLabel func(string) bool
}

// An Entry is the argument to the callback of a store's Scan method.
type Entry struct {
	Cursor string      // the storage cursor for this item
	Label  string      // the item label (may be "")
	Data   interface{} // the item data (may be nil)

	// N.B. An entry does not include the ID. ID values are provided to the
	// store as an aid to generating a cursor, but the value is meaningless
	// outside the feed that generated the item, so the store is not required to
	// persist the value explicitly.
}

// A Writer indexes items to an underlying index store.
type Writer struct{ store Store }

// NewWriter returns a Writer that delegates to the given Store.
func NewWriter(s Store) Writer { return Writer{store: s} }

// IndexItem adds itm to the store and returns its cursor.
func (w Writer) IndexItem(ctx context.Context, itm feed.Item) (string, error) {
	return w.store.Put(ctx, itm.ID(), itm.Label, itm.Data)
}

// A Reader retrieves items from an underlying index store.
type Reader struct{ store Store }

// NewReader returns a Reader that delegates to the given Store.
func NewReader(s Store) Reader { return Reader{store: s} }

// Span returns the oldest and newest cursors indexed by the store, or returns
// ErrSpanEmpty if no data are indexed.
func (r Reader) Span(ctx context.Context) (oldest, newst string, err error) {
	return r.store.Span(ctx)
}

// Scan calls f for each indexed item matching the specified query, in the
// order in which they were stored.
//
// If no items match the query, Scan returns ErrSpanEmpty.  Otherwise, scanning
// stops when there are no further items, or when f reports an error. If f
// returns ErrStopScan, Scan returns nil; otherwise Scan returns the error
// reported by f.
func (r Reader) Scan(ctx context.Context, q Query, f func(Entry) error) error {
	empty := true
	if err := r.store.Scan(ctx, q, func(entry Entry) error {
		empty = false
		return f(entry)
	}); err == ErrStopScan {
		return nil
	} else if err != nil {
		return err
	} else if empty {
		return ErrSpanEmpty
	}
	return nil
}
