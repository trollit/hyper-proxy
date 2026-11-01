package service

import (
	"context"
	"encoding/json"
	"log/slog"
	"sync"
	"time"
)

// Config controls what the service caches.
type Config struct {
	CandleCacheSize int
	MetaTTL         time.Duration
	AssetCtxTTL     time.Duration
}

// feed is a websocket backed cache that is torn down when idle.
type feed interface {
	Start()
	Stop()
	Failed() bool
	IdleTimeout() time.Duration
}

type feedEntry struct {
	feed     feed
	lastUsed time.Time
}

// Service owns the websocket connection and all caches.
type Service struct {
	ctx    context.Context
	cancel context.CancelFunc
	logger *slog.Logger
	cfg    Config

	Upstream  *Upstream
	ws        *WSManager
	infoCache *InfoCache
	funding   *FundingCache

	mu    sync.Mutex
	feeds map[Subscription]*feedEntry
}

func NewService(ctx context.Context, logger *slog.Logger, up *Upstream, cfg Config) *Service {
	s := &Service{
		logger:    logger,
		cfg:       cfg,
		Upstream:  up,
		ws:        NewWSManager(up.WebsocketURL(), logger),
		infoCache: NewInfoCache(up),
		funding:   NewFundingCache(up),
		feeds:     make(map[Subscription]*feedEntry),
	}
	s.ctx, s.cancel = context.WithCancel(ctx)

	go s.ws.Run(s.ctx)
	go s.expireLoop()

	return s
}

func (s *Service) Stop() {
	s.cancel()
}

func (s *Service) expireLoop() {
	t := time.NewTicker(5 * time.Second)
	defer t.Stop()

	for {
		select {
		case <-s.ctx.Done():
			return
		case now := <-t.C:
			s.removeExpired(now)
			s.infoCache.Purge(now)
			s.funding.Purge(now)
		}
	}
}

func (s *Service) removeExpired(now time.Time) {
	var expired []feed

	s.mu.Lock()
	for sub, e := range s.feeds {
		idle := now.Sub(e.lastUsed)
		if e.feed.Failed() || idle > e.feed.IdleTimeout() {
			s.logger.Debug("Feed closed", "subscription", sub, "failed", e.feed.Failed(), "idle", idle)
			delete(s.feeds, sub)
			expired = append(expired, e.feed)
		}
	}
	s.mu.Unlock()

	for _, f := range expired {
		f.Stop()
	}
}

// getFeed returns the feed for sub, creating and starting it if necessary.
func (s *Service) getFeed(sub Subscription, create func() feed) feed {
	s.mu.Lock()
	e, ok := s.feeds[sub]
	if !ok {
		e = &feedEntry{feed: create()}
		s.feeds[sub] = e
		e.feed.Start()
	}
	e.lastUsed = time.Now()
	s.mu.Unlock()

	return e.feed
}

// Candles returns cached candles of coin/interval overlapping [start, end].
func (s *Service) Candles(ctx context.Context, coin, interval string, start, end int64, wait time.Duration) ([]Candle, bool) {
	if _, ok := IntervalDurations[interval]; !ok || coin == "" {
		return nil, false
	}

	sub := Subscription{Type: FeedCandle, Coin: coin, Interval: interval}
	f := s.getFeed(sub, func() feed {
		return NewCandlesSrv(s.ctx, s.logger, s.Upstream, s.ws, coin, interval, s.cfg.CandleCacheSize)
	})

	return f.(*CandlesSrv).Get(ctx, start, end, wait)
}

// Book returns the cached l2Book of coin with default aggregation.
func (s *Service) Book(ctx context.Context, coin string, wait time.Duration) (json.RawMessage, bool) {
	if coin == "" {
		return nil, false
	}

	sub := Subscription{Type: FeedL2Book, Coin: coin}
	f := s.getFeed(sub, func() feed {
		return NewBookSrv(s.ctx, s.logger, s.Upstream, s.ws, coin)
	})

	return f.(*LatestSrv).Get(ctx, wait)
}

// AllMids returns the cached mids of the first perp dex and spot.
func (s *Service) AllMids(ctx context.Context, wait time.Duration) (json.RawMessage, bool) {
	sub := Subscription{Type: FeedAllMids}
	f := s.getFeed(sub, func() feed {
		return NewMidsSrv(s.ctx, s.logger, s.Upstream, s.ws)
	})

	return f.(*LatestSrv).Get(ctx, wait)
}

// InfoTTL returns how long responses of an info request type may be cached, or false if they must not be.
func (s *Service) InfoTTL(infoType string) (time.Duration, bool) {
	var ttl time.Duration
	switch infoType {
	case "meta", "spotMeta", "perpDexs":
		ttl = s.cfg.MetaTTL
	case "metaAndAssetCtxs", "spotMetaAndAssetCtxs":
		ttl = s.cfg.AssetCtxTTL
	}
	return ttl, ttl > 0
}

// CachedInfo answers a cacheable info request from the TTL cache.
func (s *Service) CachedInfo(ctx context.Context, body []byte, weight int, ttl time.Duration) (*CacheResult, error) {
	return s.infoCache.Get(ctx, body, weight, ttl)
}

// FundingHistory returns the cached funding history of coin with start <= time <= end.
func (s *Service) FundingHistory(ctx context.Context, coin string, start, end int64) ([]byte, bool) {
	if coin == "" {
		return nil, false
	}
	return s.funding.Get(ctx, coin, start, end)
}

// Stats summarizes the state of the caches for the status endpoint.
func (s *Service) Stats() map[string]any {
	counts := map[string]int{}
	failed := 0

	s.mu.Lock()
	for sub, e := range s.feeds {
		counts[sub.Type]++
		if e.feed.Failed() {
			failed++
		}
	}
	s.mu.Unlock()

	return map[string]any{
		"websocket": map[string]any{
			"connected":     s.ws.Connected(),
			"subscriptions": s.ws.Subscriptions(),
		},
		"feeds": map[string]any{
			FeedCandle:  counts[FeedCandle],
			FeedL2Book:  counts[FeedL2Book],
			FeedAllMids: counts[FeedAllMids],
			"failed":    failed,
		},
		"info_cache_entries": s.infoCache.Len(),
		"funding_series":     s.funding.Len(),
		"rate_limit": map[string]any{
			"weight_per_minute": weightPerMinute,
			"weight_available":  RateAvailable(),
		},
	}
}
