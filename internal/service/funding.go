package service

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"sort"
	"sync"
	"time"

	"golang.org/x/sync/singleflight"
)

const (
	// InfoFundingHistory is the info request type for historical funding rates.
	InfoFundingHistory = "fundingHistory"

	// fundingPageSize is the maximum number of entries upstream returns per fundingHistory request.
	fundingPageSize = 500
	// fundingInterval is how often Hyperliquid settles funding.
	fundingInterval = time.Hour
	// fundingPublishDelay is how long after a funding time its entry may take to show up upstream.
	fundingPublishDelay = time.Minute
	// fundingRefetchInterval limits upstream requests while a new funding entry is pending.
	fundingRefetchInterval = 15 * time.Second
	// fundingMaxPages bounds how much history a single fill may load. Longer ranges, such as
	// backtest downloads, are forwarded instead of cached.
	fundingMaxPages = 10
	// fundingIdleTimeout is how long a series is kept without requests.
	fundingIdleTimeout = 2 * time.Hour
)

var errFundingRange = errors.New("funding history range too long to cache")

// FundingCache caches the funding history of each coin as one growing series, so requests with
// any startTime (ccxt derives it from the current time) are answered without going upstream.
// Funding settles hourly, so the series only needs a refresh after each funding time.
type FundingCache struct {
	up *Upstream

	mu     sync.Mutex
	series map[string]*fundingSeries
	group  singleflight.Group
}

type fundingSeries struct {
	// entries holds every funding entry with from <= time <= complete, sorted by time.
	entries  []fundingEntry
	from     int64
	complete int64
	fetched  time.Time
	lastUsed time.Time
}

type fundingEntry struct {
	time int64
	raw  json.RawMessage
}

func NewFundingCache(up *Upstream) *FundingCache {
	return &FundingCache{up: up, series: make(map[string]*fundingSeries)}
}

// Get returns the funding history of coin with start <= time <= end, as upstream would.
// It returns false if the request has to be forwarded.
func (c *FundingCache) Get(ctx context.Context, coin string, start, end int64) ([]byte, bool) {
	for range 2 {
		if data, ok := c.lookup(coin, start, end); ok {
			return data, true
		}

		ch := c.group.DoChan(coin, func() (any, error) {
			// Don't let one canceled client abort the fetch shared with others.
			fetchCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 60*time.Second)
			defer cancel()
			return nil, c.fill(fetchCtx, coin, start)
		})

		select {
		case <-ctx.Done():
			return nil, false
		case res := <-ch:
			// A shared fill for another caller's start may have been out of range; ours may not be.
			if res.Err != nil && !errors.Is(res.Err, errFundingRange) {
				return nil, false
			}
		}
		// A concurrent fill may have been for a later start, so check again.
	}
	return nil, false
}

// lookup answers from the series if it covers [start, min(end, now)].
func (c *FundingCache) lookup(coin string, start, end int64) ([]byte, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()

	s, ok := c.series[coin]
	if !ok || start < s.from {
		return nil, false
	}
	if hi := min(end, nowMillis()); hi > s.complete && time.Since(s.fetched) >= fundingRefetchInterval {
		return nil, false
	}
	s.lastUsed = time.Now()

	i := sort.Search(len(s.entries), func(i int) bool { return s.entries[i].time >= start })

	var buf bytes.Buffer
	buf.WriteByte('[')
	for n := 0; i < len(s.entries) && s.entries[i].time <= end && n < fundingPageSize; i, n = i+1, n+1 {
		if n > 0 {
			buf.WriteByte(',')
		}
		buf.Write(s.entries[i].raw)
	}
	buf.WriteByte(']')
	return buf.Bytes(), true
}

// fill loads the entries of coin that are missing from the series, starting a new series at
// start if the existing one begins later.
func (c *FundingCache) fill(ctx context.Context, coin string, start int64) error {
	c.mu.Lock()
	s := c.series[coin]
	from := start
	if s != nil && s.from <= start {
		from = s.complete + 1
	} else {
		s = nil
	}
	c.mu.Unlock()

	fetchedAt := time.Now()
	if pages := (fetchedAt.UnixMilli()-from)/fundingInterval.Milliseconds()/fundingPageSize + 1; pages > fundingMaxPages {
		return errFundingRange
	}

	var entries []fundingEntry
	for cursor := from; ; {
		page, err := c.fetchPage(ctx, coin, cursor)
		if err != nil {
			return err
		}
		entries = append(entries, page...)
		if len(page) < fundingPageSize {
			break
		}
		cursor = page[len(page)-1].time + 1
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	if s == nil {
		s = &fundingSeries{from: start}
		c.series[coin] = s
	}
	for _, e := range entries {
		if n := len(s.entries); n == 0 || e.time > s.entries[n-1].time {
			s.entries = append(s.entries, e)
		}
	}
	s.complete = fetchedAt.Add(-fundingPublishDelay).UnixMilli()
	s.fetched = fetchedAt
	s.lastUsed = fetchedAt
	return nil
}

func (c *FundingCache) fetchPage(ctx context.Context, coin string, start int64) ([]fundingEntry, error) {
	body, err := json.Marshal(InfoRequest{Type: InfoFundingHistory, Coin: coin, StartTime: &start})
	if err != nil {
		return nil, err
	}

	hours := (nowMillis()-start)/fundingInterval.Milliseconds() + 1
	resp, err := c.up.PostInfo(ctx, body, fundingWeight(int(min(hours, fundingPageSize))))
	if err != nil {
		return nil, err
	}
	// Unknown coins are answered with "null" or an error status.
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("unexpected status %d", resp.StatusCode)
	}

	var raws []json.RawMessage
	if err := json.Unmarshal(resp.Body, &raws); err != nil || raws == nil {
		return nil, fmt.Errorf("unexpected fundingHistory response: %.100s", resp.Body)
	}

	entries := make([]fundingEntry, 0, len(raws))
	for _, raw := range raws {
		var e struct {
			Time int64 `json:"time"`
		}
		if err := json.Unmarshal(raw, &e); err != nil {
			return nil, fmt.Errorf("decode funding entry: %w", err)
		}
		entries = append(entries, fundingEntry{time: e.Time, raw: raw})
	}
	sort.SliceStable(entries, func(i, j int) bool { return entries[i].time < entries[j].time })
	return entries, nil
}

// Len returns the number of cached series.
func (c *FundingCache) Len() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.series)
}

// Purge removes series that were not requested for fundingIdleTimeout.
func (c *FundingCache) Purge(now time.Time) {
	c.mu.Lock()
	defer c.mu.Unlock()

	for coin, s := range c.series {
		if now.Sub(s.lastUsed) > fundingIdleTimeout {
			delete(c.series, coin)
		}
	}
}
