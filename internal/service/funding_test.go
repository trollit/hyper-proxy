package service

import (
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"
)

// fakeFundingUpstream serves hourly fundingHistory entries for testCoin like Hyperliquid does.
type fakeFundingUpstream struct {
	times    []int64
	requests atomic.Int32
}

func newFakeFundingUpstream(t *testing.T, hours int) (*fakeFundingUpstream, *Upstream) {
	t.Helper()

	f := &fakeFundingUpstream{}
	last := time.Now().Truncate(time.Hour).UnixMilli() + 3
	for i := hours - 1; i >= 0; i-- {
		f.times = append(f.times, last-int64(i)*time.Hour.Milliseconds())
	}

	srv := httptest.NewServer(http.HandlerFunc(f.serve))
	t.Cleanup(srv.Close)
	return f, NewUpstream(srv.URL, NewBanDetector(slog.New(slog.DiscardHandler)))
}

func (f *fakeFundingUpstream) serve(w http.ResponseWriter, r *http.Request) {
	f.requests.Add(1)

	body, _ := io.ReadAll(r.Body)
	var req InfoRequest
	_ = json.Unmarshal(body, &req)
	if req.Coin != testCoin {
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = w.Write([]byte("null"))
		return
	}

	out := []map[string]any{}
	for _, ts := range f.times {
		if ts >= *req.StartTime && (req.EndTime == nil || ts <= *req.EndTime) && len(out) < fundingPageSize {
			out = append(out, map[string]any{"coin": testCoin, "fundingRate": fmt.Sprint(ts), "premium": "0", "time": ts})
		}
	}
	_ = json.NewEncoder(w).Encode(out)
}

func fundingTimes(t *testing.T, data []byte) []int64 {
	t.Helper()

	var entries []struct {
		Time int64 `json:"time"`
	}
	if err := json.Unmarshal(data, &entries); err != nil {
		t.Fatalf("decode %s: %v", data, err)
	}
	times := make([]int64, len(entries))
	for i, e := range entries {
		times[i] = e.Time
	}
	return times
}

func TestFundingCacheServesLaterStartsFromCache(t *testing.T) {
	f, up := newFakeFundingUpstream(t, 1200)
	c := NewFundingCache(up)
	now := nowMillis()
	hour := time.Hour.Milliseconds()

	data, ok := c.Get(t.Context(), testCoin, now-600*hour, now)
	if !ok {
		t.Fatal("expected first request to be cached")
	}
	if got := fundingTimes(t, data); len(got) != fundingPageSize || got[0] != f.times[len(f.times)-600] {
		t.Fatalf("expected the first %d entries from the start, got %d starting %v", fundingPageSize, len(got), got[:1])
	}
	fetches := f.requests.Load()

	// ccxt moves startTime with the clock, so later requests start later.
	data, ok = c.Get(t.Context(), testCoin, now-500*hour+1234, now+5000)
	if !ok {
		t.Fatal("expected later start to be served")
	}
	if got := fundingTimes(t, data); len(got) != 500 || got[0] != f.times[len(f.times)-500] || got[499] != f.times[len(f.times)-1] {
		t.Fatalf("unexpected entries: %d from %d to %d", len(got), got[0], got[len(got)-1])
	}

	data, _ = c.Get(t.Context(), testCoin, now-10*hour, now-5*hour)
	if got := fundingTimes(t, data); len(got) != 5 {
		t.Fatalf("expected endTime to bound the result, got %d entries", len(got))
	}

	if f.requests.Load() != fetches {
		t.Fatalf("expected no upstream requests, got %d", f.requests.Load()-fetches)
	}
}

func TestFundingCacheRefillsEarlierStart(t *testing.T) {
	f, up := newFakeFundingUpstream(t, 1200)
	c := NewFundingCache(up)
	now := nowMillis()
	hour := time.Hour.Milliseconds()

	if _, ok := c.Get(t.Context(), testCoin, now-100*hour, now); !ok {
		t.Fatal("expected first request to be cached")
	}
	data, ok := c.Get(t.Context(), testCoin, now-1000*hour, now)
	if !ok {
		t.Fatal("expected earlier start to be served")
	}
	if got := fundingTimes(t, data); len(got) != fundingPageSize || got[0] != f.times[len(f.times)-1000] {
		t.Fatalf("unexpected entries: %d starting %d", len(got), got[0])
	}
}

func TestFundingCacheRefreshesAfterFundingTime(t *testing.T) {
	f, up := newFakeFundingUpstream(t, 10)
	c := NewFundingCache(up)
	now := nowMillis()

	if _, ok := c.Get(t.Context(), testCoin, now-20*time.Hour.Milliseconds(), now); !ok {
		t.Fatal("expected first request to be cached")
	}

	// Pretend the last fill happened before the previous funding time.
	c.mu.Lock()
	s := c.series[testCoin]
	s.complete = f.times[len(f.times)-2]
	s.entries = s.entries[:len(s.entries)-1]
	s.fetched = time.Now().Add(-time.Minute)
	c.mu.Unlock()

	fetches := f.requests.Load()
	data, _ := c.Get(t.Context(), testCoin, now-20*time.Hour.Milliseconds(), now)
	if got := fundingTimes(t, data); len(got) != 10 || f.requests.Load() != fetches+1 {
		t.Fatalf("expected one refresh returning all 10 entries, got %d entries after %d requests", len(got), f.requests.Load()-fetches)
	}
}

func TestFundingCacheForwardsUnknownAndLongRanges(t *testing.T) {
	f, up := newFakeFundingUpstream(t, 10)
	c := NewFundingCache(up)
	now := nowMillis()

	if _, ok := c.Get(t.Context(), "NOPE", now-time.Hour.Milliseconds(), now); ok {
		t.Error("expected unknown coin to be forwarded")
	}

	fetches := f.requests.Load()
	if _, ok := c.Get(t.Context(), testCoin, now-int64(fundingMaxPages+1)*fundingPageSize*time.Hour.Milliseconds(), now); ok {
		t.Error("expected long range to be forwarded")
	}
	if f.requests.Load() != fetches {
		t.Error("expected no upstream requests for a long range")
	}
	if c.Len() != 0 {
		t.Errorf("expected no series, got %d", c.Len())
	}
}
