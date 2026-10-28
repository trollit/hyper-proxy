package service

import (
	"log/slog"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"
)

// BanDetector tracks whether Hyperliquid is currently rate limiting us and until when to back off.
type BanDetector struct {
	mu     sync.Mutex
	logger *slog.Logger

	banned       bool
	recoveryTime time.Time

	// Error counters for gradual ban detection
	errorCount   int
	lastError    time.Time
	backoffCount int
}

func NewBanDetector(logger *slog.Logger) *BanDetector {
	return &BanDetector{logger: logger}
}

// IsBanned reports whether requests to the upstream should currently be suspended.
func (bd *BanDetector) IsBanned() bool {
	bd.mu.Lock()
	defer bd.mu.Unlock()

	if !bd.banned {
		return false
	}
	if time.Now().Before(bd.recoveryTime) {
		return true
	}

	bd.banned = false
	bd.logger.Info("API backoff lifted, resuming normal operation")
	return false
}

// Status returns the ban state and the time the ban is expected to be lifted.
func (bd *BanDetector) Status() (bool, time.Time) {
	bd.mu.Lock()
	defer bd.mu.Unlock()

	return bd.banned && time.Now().Before(bd.recoveryTime), bd.recoveryTime
}

// CheckResponse inspects an upstream response or transport error and returns true if requests should back off.
func (bd *BanDetector) CheckResponse(resp *http.Response, err error) bool {
	bd.mu.Lock()
	defer bd.mu.Unlock()

	now := time.Now()

	if resp != nil {
		switch resp.StatusCode {
		case http.StatusTooManyRequests:
			banUntil := parseRetryAfter(resp, now)
			if banUntil.IsZero() {
				banUntil = now.Add(1 * time.Minute)
			}
			bd.setBanned(banUntil)
			bd.backoffCount = 0
			bd.logger.Warn("API rate limited (429), suspending requests", "banUntil", banUntil)
			return true
		case http.StatusForbidden:
			banUntil := now.Add(5 * time.Minute)
			bd.setBanned(banUntil)
			bd.logger.Warn("API access forbidden (403), suspending requests", "banUntil", banUntil)
			return true
		case http.StatusOK:
			bd.errorCount = 0
			bd.backoffCount = 0
		}
		return false
	}

	if err != nil && isConnectionError(err) {
		if now.Sub(bd.lastError) > time.Minute {
			bd.errorCount = 0
		}
		bd.errorCount++
		bd.lastError = now

		// If too many errors in short time, use exponential backoff
		if bd.errorCount >= 5 {
			backoff := bd.nextBackoff()
			bd.setBanned(now.Add(backoff))
			bd.logger.Warn("API connection issues detected, suspending requests", "errorCount", bd.errorCount, "backoffDuration", backoff, "banUntil", bd.recoveryTime)
			bd.errorCount = 0
			return true
		}
	}

	return false
}

func (bd *BanDetector) setBanned(until time.Time) {
	bd.banned = true
	bd.recoveryTime = until
}

// nextBackoff returns 2^n seconds, capped at 10 minutes.
func (bd *BanDetector) nextBackoff() time.Duration {
	if bd.backoffCount < 30 {
		bd.backoffCount++
	}

	backoff := time.Duration(1<<bd.backoffCount) * time.Second
	return min(backoff, 10*time.Minute)
}

func isConnectionError(err error) bool {
	msg := err.Error()
	return strings.Contains(msg, "connection refused") ||
		strings.Contains(msg, "connection reset") ||
		strings.Contains(msg, "timeout") ||
		strings.Contains(msg, "no route to host")
}

func parseRetryAfter(resp *http.Response, now time.Time) time.Time {
	seconds, err := strconv.Atoi(resp.Header.Get("Retry-After"))
	if err != nil || seconds <= 0 {
		return time.Time{}
	}
	return now.Add(time.Duration(seconds) * time.Second)
}
