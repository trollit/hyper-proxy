package service

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"sync"
	"time"

	"golang.org/x/sync/singleflight"
)

// staleRetention is how long expired entries are kept to answer requests while the upstream is unavailable.
const staleRetention = 10 * time.Minute

// InfoCache is a TTL cache for public, non-user-specific info requests (market metadata and asset contexts).
// Concurrent misses for the same request are collapsed into a single upstream request.
type InfoCache struct {
	up *Upstream

	mu      sync.RWMutex
	entries map[string]infoEntry
	group   singleflight.Group
}

type infoEntry struct {
	body    []byte
	expires time.Time
}

// CacheResult is the outcome of an InfoCache lookup.
type CacheResult struct {
	*Response
	// Source is "cache", "stale-cache" or "upstream".
	Source string
}

func NewInfoCache(up *Upstream) *InfoCache {
	return &InfoCache{up: up, entries: make(map[string]infoEntry)}
}

// Get returns the cached response for body, fetching it upstream if it is missing or expired.
// When the upstream fails, an expired entry is returned if one is still retained.
func (c *InfoCache) Get(ctx context.Context, body []byte, weight int, ttl time.Duration) (*CacheResult, error) {
	key := canonicalKey(body)

	c.mu.RLock()
	entry, found := c.entries[key]
	c.mu.RUnlock()

	if found && time.Now().Before(entry.expires) {
		return &CacheResult{Response: &Response{StatusCode: http.StatusOK, Body: entry.body}, Source: "cache"}, nil
	}

	ch := c.group.DoChan(key, func() (any, error) {
		// Don't let one canceled client abort the fetch shared with others.
		fetchCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 30*time.Second)
		defer cancel()

		resp, err := c.up.PostInfo(fetchCtx, body, weight)
		if err == nil && resp.StatusCode == http.StatusOK {
			c.mu.Lock()
			c.entries[key] = infoEntry{body: resp.Body, expires: time.Now().Add(ttl)}
			c.mu.Unlock()
		}
		return resp, err
	})

	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case res := <-ch:
		if res.Err != nil || res.Val.(*Response).StatusCode >= http.StatusInternalServerError {
			if found {
				return &CacheResult{Response: &Response{StatusCode: http.StatusOK, Body: entry.body}, Source: "stale-cache"}, nil
			}
			if res.Err != nil {
				return nil, res.Err
			}
		}
		return &CacheResult{Response: res.Val.(*Response), Source: "upstream"}, nil
	}
}

// Len returns the number of cached entries.
func (c *InfoCache) Len() int {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return len(c.entries)
}

// Purge removes entries that expired longer than staleRetention ago.
func (c *InfoCache) Purge(now time.Time) {
	c.mu.Lock()
	defer c.mu.Unlock()

	for k, e := range c.entries {
		if now.Sub(e.expires) > staleRetention {
			delete(c.entries, k)
		}
	}
}

// canonicalKey normalizes a JSON body so that key order and whitespace don't cause cache misses.
func canonicalKey(body []byte) string {
	dec := json.NewDecoder(bytes.NewReader(body))
	dec.UseNumber()

	var v any
	if err := dec.Decode(&v); err != nil {
		return string(body)
	}
	out, err := json.Marshal(v)
	if err != nil {
		return string(body)
	}
	return string(out)
}
