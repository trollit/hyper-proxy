package service

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"github.com/trollit/hyper-proxy/internal/tool"
)

// LatestSrv caches the latest message of a websocket feed whose messages are complete snapshots,
// such as l2Book and allMids.
type LatestSrv struct {
	ctx    context.Context
	cancel context.CancelFunc
	logger *slog.Logger

	up  *Upstream
	ws  *WSManager
	sub Subscription

	// initBody is an optional REST request that validates the feed and provides initial data.
	initBody []byte
	// extract converts a websocket message into the REST response format.
	extract func(json.RawMessage) (json.RawMessage, error)

	mu     sync.RWMutex
	data   json.RawMessage
	ready  bool
	readyC chan struct{}

	failC      chan struct{}
	failed     atomic.Bool
	subscribed atomic.Bool
}

// NewBookSrv caches the l2Book of a coin (default aggregation, up to 20 levels per side).
func NewBookSrv(ctx context.Context, logger *slog.Logger, up *Upstream, ws *WSManager, coin string) *LatestSrv {
	initBody, _ := json.Marshal(map[string]string{"type": FeedL2Book, "coin": coin})

	return newLatestSrv(ctx, logger.With("feed", "l2Book", "coin", coin), up, ws,
		Subscription{Type: FeedL2Book, Coin: coin}, initBody,
		func(data json.RawMessage) (json.RawMessage, error) { return data, nil },
	)
}

// NewMidsSrv caches the mids of all coins of the first perp dex and spot.
func NewMidsSrv(ctx context.Context, logger *slog.Logger, up *Upstream, ws *WSManager) *LatestSrv {
	return newLatestSrv(ctx, logger.With("feed", "allMids"), up, ws,
		Subscription{Type: FeedAllMids}, nil,
		func(data json.RawMessage) (json.RawMessage, error) {
			var msg struct {
				Mids json.RawMessage `json:"mids"`
			}
			if err := json.Unmarshal(data, &msg); err != nil {
				return nil, err
			}
			if IsNull(msg.Mids) {
				return nil, errors.New("message without mids")
			}
			return msg.Mids, nil
		},
	)
}

func newLatestSrv(ctx context.Context, logger *slog.Logger, up *Upstream, ws *WSManager, sub Subscription, initBody []byte, extract func(json.RawMessage) (json.RawMessage, error)) *LatestSrv {
	s := &LatestSrv{
		logger:   logger,
		up:       up,
		ws:       ws,
		sub:      sub,
		initBody: initBody,
		extract:  extract,
		readyC:   make(chan struct{}),
		failC:    make(chan struct{}),
	}
	s.ctx, s.cancel = context.WithCancel(ctx)

	return s
}

func (s *LatestSrv) Start() {
	go s.init()
}

func (s *LatestSrv) Stop() {
	s.cancel()
	if s.subscribed.Load() {
		s.ws.Unsubscribe(s.sub)
	}
}

func (s *LatestSrv) Failed() bool {
	return s.failed.Load()
}

func (s *LatestSrv) IdleTimeout() time.Duration {
	return 2 * time.Minute
}

func (s *LatestSrv) fail(err error) {
	if s.ctx.Err() != nil {
		return
	}
	s.logger.Warn("Feed unavailable, requests are forwarded", "error", err)
	if s.failed.CompareAndSwap(false, true) {
		close(s.failC)
	}
}

func (s *LatestSrv) init() {
	if s.initBody != nil {
		data, err := s.fetchInitial()
		if err != nil {
			s.fail(err)
			return
		}
		s.set(data)
	}

	if err := s.ws.Subscribe(s.sub, s); err != nil {
		s.fail(err)
		return
	}
	s.subscribed.Store(true)

	// Stop() may have run before the subscription was registered.
	if s.ctx.Err() != nil {
		s.ws.Unsubscribe(s.sub)
	}
}

func (s *LatestSrv) fetchInitial() (json.RawMessage, error) {
	var err error
	for d, attempt := tool.NewDelayIterator(), 0; attempt < initAttempts; d.Delay() {
		waitForBan(s.ctx, s.up.BanDetector)
		if s.ctx.Err() != nil {
			return nil, s.ctx.Err()
		}

		var resp *Response
		resp, err = s.up.PostInfo(s.ctx, s.initBody, 2)
		switch {
		case errors.Is(err, ErrBanned):
			continue
		case err != nil:
		case bytes.Equal(bytes.TrimSpace(resp.Body), []byte("null")), resp.StatusCode >= 400 && resp.StatusCode < 500:
			// Unknown coins are answered with "null"; never subscribe to them.
			return nil, fmt.Errorf("%w: status %d", errUnknownFeed, resp.StatusCode)
		case resp.StatusCode != http.StatusOK:
			err = fmt.Errorf("unexpected status %d", resp.StatusCode)
		default:
			return resp.Body, nil
		}

		attempt++
		s.logger.Debug("Initial fetch failed, retrying", "error", err)
	}

	return nil, err
}

func (s *LatestSrv) set(data json.RawMessage) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.data = data
	if !s.ready {
		s.ready = true
		close(s.readyC)
	}
}

func (s *LatestSrv) onSubscribed() {}

func (s *LatestSrv) onDisconnected() {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Data is stale until the next websocket message arrives.
	if s.ready {
		s.ready = false
		s.readyC = make(chan struct{})
	}
}

func (s *LatestSrv) onMessage(data json.RawMessage) {
	out, err := s.extract(data)
	if err != nil {
		s.logger.Warn("Websocket message could not be decoded", "error", err)
		return
	}
	s.set(out)
}

// Get returns the latest data, waiting up to wait for the feed to become ready.
func (s *LatestSrv) Get(ctx context.Context, wait time.Duration) (json.RawMessage, bool) {
	s.mu.RLock()
	readyC := s.readyC
	s.mu.RUnlock()

	t := time.NewTimer(wait)
	defer t.Stop()

	select {
	case <-readyC:
	case <-s.failC:
		return nil, false
	case <-ctx.Done():
		return nil, false
	case <-s.ctx.Done():
		return nil, false
	case <-t.C:
		return nil, false
	}

	s.mu.RLock()
	defer s.mu.RUnlock()

	return s.data, s.ready
}
