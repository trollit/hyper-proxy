package service

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/trollit/hyper-proxy/internal/tool"
)

const (
	// With the delay iterator this retries transient errors for ~19s before giving up.
	initAttempts       = 8
	maxCandlesHeadroom = 200
)

// errUnknownFeed means the upstream does not know the requested coin/interval. Subscribing to an
// unknown coin makes Hyperliquid close the websocket, so such feeds are never subscribed.
var errUnknownFeed = errors.New("unknown coin or interval")

// CandlesSrv keeps the most recent candles of one coin and interval up to date via websocket.
type CandlesSrv struct {
	ctx    context.Context
	cancel context.CancelFunc
	logger *slog.Logger

	up   *Upstream
	ws   *WSManager
	sub  Subscription
	dur  time.Duration
	size int

	mu       sync.RWMutex
	candles  []Candle // sorted by OpenTime
	complete bool     // older start times would not return more data from upstream either
	ready    bool
	readyC   chan struct{}
	gen      int // incremented on every websocket disconnect

	failC      chan struct{}
	failed     atomic.Bool
	subscribed atomic.Bool
}

func NewCandlesSrv(ctx context.Context, logger *slog.Logger, up *Upstream, ws *WSManager, coin, interval string, size int) *CandlesSrv {
	s := &CandlesSrv{
		logger: logger.With("feed", "candle", "coin", coin, "interval", interval),
		up:     up,
		ws:     ws,
		sub:    Subscription{Type: FeedCandle, Coin: coin, Interval: interval},
		dur:    IntervalDurations[interval],
		size:   min(max(size, 1), MaxCandles),
		readyC: make(chan struct{}),
		failC:  make(chan struct{}),
	}
	s.ctx, s.cancel = context.WithCancel(ctx)

	return s
}

func (s *CandlesSrv) Start() {
	go s.init()
}

func (s *CandlesSrv) Stop() {
	s.cancel()
	if s.subscribed.Load() {
		s.ws.Unsubscribe(s.sub)
	}
}

func (s *CandlesSrv) Failed() bool {
	return s.failed.Load()
}

func (s *CandlesSrv) IdleTimeout() time.Duration {
	return max(2*s.dur, 2*time.Minute)
}

func (s *CandlesSrv) fail(err error) {
	if s.ctx.Err() != nil {
		return
	}
	s.logger.Warn("Candle feed unavailable, requests are forwarded", "error", err)
	if s.failed.CompareAndSwap(false, true) {
		close(s.failC)
	}
}

func (s *CandlesSrv) init() {
	// Upstream answers startTime 0 with the most recent candles it keeps (~MaxCandles).
	var start int64
	now := nowMillis()

	var candles []Candle
	var err error
	for d, attempt := tool.NewDelayIterator(), 0; attempt < initAttempts; d.Delay() {
		waitForBan(s.ctx, s.up.BanDetector)
		if s.ctx.Err() != nil {
			return
		}

		candles, err = s.fetch(start, now)
		if err == nil || errors.Is(err, errUnknownFeed) {
			break
		}
		if !errors.Is(err, ErrBanned) {
			attempt++
		}
		s.logger.Debug("Candle snapshot failed, retrying", "error", err)
	}
	if err != nil {
		s.fail(err)
		return
	}

	s.mu.Lock()
	s.merge(candles)
	s.complete = len(candles) < s.size || s.size == MaxCandles
	s.mu.Unlock()

	s.logger.Debug("Candle snapshot loaded", "candles", len(candles))

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

// fetch loads candles from the REST API.
func (s *CandlesSrv) fetch(start, end int64) ([]Candle, error) {
	body, err := json.Marshal(map[string]any{
		"type": "candleSnapshot",
		"req": map[string]any{
			"coin":      s.sub.Coin,
			"interval":  s.sub.Interval,
			"startTime": start,
			"endTime":   end,
		},
	})
	if err != nil {
		return nil, err
	}

	n := min((end-start)/s.dur.Milliseconds()+1, MaxCandles) // also caps start == 0
	resp, err := s.up.PostInfo(s.ctx, body, 20+int(n)/60)
	if err != nil {
		return nil, err
	}

	// Unknown coins are answered with "null", either with status 200 or 500.
	if bytes.Equal(bytes.TrimSpace(resp.Body), []byte("null")) || (resp.StatusCode >= 400 && resp.StatusCode < 500) {
		return nil, fmt.Errorf("%w: status %d", errUnknownFeed, resp.StatusCode)
	}
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("unexpected status %d", resp.StatusCode)
	}

	var candles []Candle
	if err := json.Unmarshal(resp.Body, &candles); err != nil {
		return nil, err
	}
	if len(candles) == 0 && start == 0 {
		// No history at all: nothing to subscribe to.
		return nil, errUnknownFeed
	}

	return candles, nil
}

// merge inserts or updates candles. A candle is replaced only by a version with at least as many
// trades, so stale REST data never overwrites newer websocket data. Callers must hold s.mu.
func (s *CandlesSrv) merge(candles []Candle) {
	for _, c := range candles {
		i := sort.Search(len(s.candles), func(i int) bool { return s.candles[i].OpenTime >= c.OpenTime })
		switch {
		case i < len(s.candles) && s.candles[i].OpenTime == c.OpenTime:
			if c.Trades >= s.candles[i].Trades {
				s.candles[i] = c
			}
		case i == len(s.candles):
			s.candles = append(s.candles, c)
		default:
			s.candles = append(s.candles, Candle{})
			copy(s.candles[i+1:], s.candles[i:])
			s.candles[i] = c
		}
	}

	keep := s.size
	if s.size == MaxCandles {
		// Upstream actually serves slightly more than MaxCandles; keep a superset of its window.
		keep += maxCandlesHeadroom
	}
	if excess := len(s.candles) - keep; excess > 0 {
		s.candles = append(s.candles[:0:0], s.candles[excess:]...)
		// Upstream keeps the same rolling window when the cache holds MaxCandles.
		if s.size < MaxCandles {
			s.complete = false
		}
	}
}

func (s *CandlesSrv) onSubscribed() {
	s.mu.RLock()
	gen := s.gen
	s.mu.RUnlock()

	go s.catchUp(gen)
}

func (s *CandlesSrv) onDisconnected() {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.gen++
	if s.ready {
		s.ready = false
		s.readyC = make(chan struct{})
	}
}

func (s *CandlesSrv) onMessage(data json.RawMessage) {
	var c Candle
	if err := json.Unmarshal(data, &c); err != nil {
		s.logger.Warn("Candle websocket message could not be decoded", "error", err)
		return
	}

	s.mu.Lock()
	s.merge([]Candle{c})
	s.mu.Unlock()
}

// catchUp fetches the candles that may have been missed between the snapshot (or the last
// disconnect) and the subscription becoming active, then marks the cache ready.
func (s *CandlesSrv) catchUp(gen int) {
	for d := tool.NewDelayIterator(); ; d.Delay() {
		s.mu.RLock()
		stale := s.gen != gen
		start := nowMillis() - 2*s.dur.Milliseconds()
		if len(s.candles) > 0 {
			start = s.candles[len(s.candles)-1].OpenTime
		}
		s.mu.RUnlock()

		// A newer subscription will catch up instead.
		if stale || s.ctx.Err() != nil {
			return
		}

		waitForBan(s.ctx, s.up.BanDetector)
		start = max(start, nowMillis()-int64(s.size)*s.dur.Milliseconds())
		candles, err := s.fetch(start, nowMillis())
		if errors.Is(err, errUnknownFeed) {
			candles, err = nil, nil
		}
		if err != nil {
			s.logger.Debug("Candle catch-up failed, retrying", "error", err)
			continue
		}

		s.mu.Lock()
		s.merge(candles)
		if s.gen == gen && !s.ready {
			s.ready = true
			close(s.readyC)
		}
		s.mu.Unlock()

		s.logger.Debug("Candle feed ready")
		return
	}
}

// Get returns the cached candles overlapping [start, end], matching the upstream filter. It waits up
// to wait for the cache to become ready and returns false if the request can't be served from cache.
func (s *CandlesSrv) Get(ctx context.Context, start, end int64, wait time.Duration) ([]Candle, bool) {
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

	if !s.ready || len(s.candles) == 0 {
		return nil, false
	}
	if start < s.candles[0].OpenTime && !s.complete {
		return nil, false
	}

	i := sort.Search(len(s.candles), func(i int) bool { return s.candles[i].CloseTime >= start })
	out := make([]Candle, 0, len(s.candles)-i)
	for ; i < len(s.candles) && s.candles[i].OpenTime <= end; i++ {
		out = append(out, s.candles[i])
	}

	return out, true
}
