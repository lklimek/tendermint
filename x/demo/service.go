package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/creachadair/jrpc2"
	"github.com/creachadair/jrpc2/handler"
	"github.com/google/uuid"

	"github.com/tendermint/tendermint/x/feed"
	"github.com/tendermint/tendermint/x/hub"
	"github.com/tendermint/tendermint/x/index"
	"github.com/tendermint/tendermint/x/match"
)

// service exports JSON-RPC methods to use a feed, hub, and listeners.
type service struct {
	feed  *feed.Feed
	hub   *hub.Hub
	index index.Reader
	hbeat *time.Ticker

	subs struct {
		sync.Mutex
		m map[string]*hub.Listener
	}
}

// methods returns a method assigner for the service.
func (s *service) methods() handler.Map {
	return handler.Map{
		"beat.adjust": handler.New(s.adjustBeat),

		"event.post": handler.New(s.postEvent),

		"listen.subscribe": handler.New(s.listen),
		"listen.next":      handler.New(s.listenNext),
		"listen.close":     handler.New(s.listenClose),

		"index.span": handler.New(s.indexSpan),
		"index.scan": handler.New(s.indexScan),
	}
}

// postEvent injects the given event into the feed.
func (s *service) postEvent(ctx context.Context, evt eventReq) (err error) {
	defer func() {
		if err == nil {
			jrpc2.ServerFromContext(ctx).Metrics().Count("feed.eventsPosted", 1)
		}
	}()
	label := "event"
	if evt.Kind != "" {
		label += "/" + evt.Kind
	}
	return s.feed.Add(ctx, feed.Item{
		Label: label,
		Data:  evt.event,
	})
}

// listen subscribes a new listener and returns its ID to the client.
func (s *service) listen(ctx context.Context, opts listenOpts) interface{} {
	s.subs.Lock()
	defer s.subs.Unlock()

	id := uuid.New().String()
	lst := s.hub.Listen(&hub.ListenOptions{
		MaxQueueLen: opts.MaxQueueLen,
		MatchLabel:  opts.Label,
	})
	s.subs.m[id] = lst
	jrpc2.ServerFromContext(ctx).Metrics().SetLabel("hub.numListeners", len(s.subs.m))
	return handler.Obj{"id": id}
}

// listenNext retrieves the next item from the given listener.
func (s *service) listenNext(ctx context.Context, req listenReq) (*nextItem, error) {
	s.subs.Lock()
	lst := s.subs.m[req.ID]
	s.subs.Unlock()

	if lst == nil {
		return nil, fmt.Errorf("unknown listener ID %q", req.ID)
	}
	itm, err := lst.Next(ctx)
	if err != nil {
		if err == hub.ErrListenerClosed {
			s.subs.Lock()
			delete(s.subs.m, req.ID)
			jrpc2.ServerFromContext(ctx).Metrics().SetLabel("hub.numListeners", len(s.subs.m))
			s.subs.Unlock()
		}
		return nil, err
	}
	return &nextItem{
		entry: entry{
			Cursor: []byte(itm.Cursor),
			Label:  itm.Label,
			Data:   itm.Data,
		},
		Lost: lst.Lost(),
	}, nil
}

// listenClose closes and releases a listneer.
func (s *service) listenClose(ctx context.Context, req listenReq) error {
	s.subs.Lock()
	defer s.subs.Unlock()

	lst := s.subs.m[req.ID]
	if lst == nil {
		return fmt.Errorf("unknown listener ID %q", req.ID)
	}
	return lst.Close()
}

// indexSpan reports the available index range.
func (s *service) indexSpan(ctx context.Context) (*span, error) {
	oldest, newest, err := s.index.Span(ctx)
	if err != nil {
		return nil, err
	}
	return &span{
		Oldest: []byte(oldest),
		Newest: []byte(newest),
	}, nil
}

// indexScan scans the contents of the index.
func (s *service) indexScan(ctx context.Context, req scanQuery) ([]*entry, error) {
	var entries []*entry
	err := s.index.Scan(ctx, index.Query{
		Start:      string(req.Start),
		Limit:      string(req.Limit),
		MatchLabel: match.AnyOrGlob(req.Label),
	}, func(e index.Entry) error {
		entries = append(entries, &entry{
			Cursor: []byte(e.Cursor),
			Label:  e.Label,
			Data:   e.Data,
		})
		if req.Count > 0 && len(entries) == req.Count {
			return index.ErrStopScan
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return entries, nil
}

// adjustBeat adjusts the heartbeat rate.
func (s *service) adjustBeat(ctx context.Context, arg [1]string) error {
	const minInterval = 250 * time.Millisecond

	if s.hbeat == nil {
		return errors.New("system has no heartbeat")
	}
	dur, err := time.ParseDuration(arg[0])
	if err != nil {
		return fmt.Errorf("invalid heartbeat interval: %v", err)
	}
	if dur < minInterval {
		dur = minInterval
	}
	log.Printf("Resetting heartbeat duration to %v", dur)
	s.hbeat.Reset(dur)
	jrpc2.ServerFromContext(ctx).Metrics().SetLabel("demo.heartbeatInterval", dur.String())
	return nil
}

// heartbeat periodically injects items into the feed, to simulate a workload
// from a running system.
func (s *service) heartbeat(ctx context.Context, d time.Duration) {
	s.hbeat = time.NewTicker(d)
	defer s.hbeat.Stop()
	var nBeats int
	for {
		select {
		case <-ctx.Done():
			return
		case when := <-s.hbeat.C:
			err := s.feed.Add(ctx, feed.Item{
				Label: "heartbeat",
				Data:  when.In(time.UTC),
			})
			if err != nil {
				log.Printf("Heartbeat failed: %v", err)
				return
			}
			nBeats++
			log.Printf("[heartbeat] %d published", nBeats)
		}
	}
}

type event struct {
	Type  string      `json:"type"`
	Attrs []eventAttr `json:"attributes",omitempty"`
}

type eventAttr struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

type eventReq struct {
	event
	Kind string `json:"kind,omitempty"`
}

type span struct {
	Oldest []byte `json:"oldest"`
	Newest []byte `json:"newest"`
}

type scanQuery struct {
	Start []byte `json:"start"`
	Limit []byte `json:"limit"`
	Label string `json:"label"`
	Count int    `json:"count"`
}

type entry struct {
	Cursor []byte      `json:"cursor"`
	Label  string      `json:"label,omitempty"`
	Data   interface{} `json:"data"`
}

type listenReq struct {
	ID string `json:"id"`
}

type listenOpts struct {
	MaxQueueLen int    `json:"maxQueueLen"`
	Label       string `json:"label"`
}

type nextItem struct {
	entry
	Lost int `json:"lost,omitempty"`
}
