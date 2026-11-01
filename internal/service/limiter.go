package service

import (
	"context"
	"encoding/json"
	"time"

	"golang.org/x/time/rate"
)

// Hyperliquid allows an aggregated REST weight of 1200 per minute per IP.
const weightPerMinute = 1200

var Limiter = rate.NewLimiter(rate.Limit(weightPerMinute/60), weightPerMinute)

// InfoWeight returns the rate limit weight of an info request.
// See https://hyperliquid.gitbook.io/hyperliquid-docs/for-developers/api/rate-limits-and-user-limits
func InfoWeight(r *InfoRequest) int {
	switch r.Type {
	case FeedL2Book, FeedAllMids, "clearinghouseState", "orderStatus", "spotClearinghouseState", "exchangeStatus":
		return 2
	case "userRole":
		return 60
	case "candleSnapshot":
		// Additional weight per 60 candles returned.
		return 20 + estimateCandles(r)/60
	case InfoFundingHistory:
		return fundingWeight(estimateFundings(r))
	default:
		return 20
	}
}

// fundingWeight returns the weight of a fundingHistory request returning n entries:
// additional weight per 20 items returned.
func fundingWeight(n int) int {
	return 20 + n/20
}

func estimateFundings(r *InfoRequest) int {
	if r.StartTime == nil {
		return fundingPageSize
	}

	end := nowMillis()
	if r.EndTime != nil && *r.EndTime < end {
		end = *r.EndTime
	}
	if end < *r.StartTime {
		return 0
	}

	n := (end-*r.StartTime)/fundingInterval.Milliseconds() + 1
	return int(min(n, fundingPageSize))
}

func estimateCandles(r *InfoRequest) int {
	cr, err := r.CandleReq()
	if err != nil || cr.StartTime == nil {
		return MaxCandles
	}

	dur, ok := IntervalDurations[cr.Interval]
	if !ok {
		return MaxCandles
	}

	end := nowMillis()
	if cr.EndTime != nil && *cr.EndTime < end {
		end = *cr.EndTime
	}
	if end < *cr.StartTime {
		return 0
	}

	n := (end-*cr.StartTime)/dur.Milliseconds() + 1
	return int(min(n, MaxCandles))
}

// ExchangeWeight returns the rate limit weight of an exchange action: 1 + floor(batch_length / 40).
func ExchangeWeight(body []byte) int {
	var req struct {
		Action struct {
			Orders   []json.RawMessage `json:"orders"`
			Cancels  []json.RawMessage `json:"cancels"`
			Modifies []json.RawMessage `json:"modifies"`
		} `json:"action"`
	}
	if err := json.Unmarshal(body, &req); err != nil {
		return 1
	}

	n := max(len(req.Action.Orders), len(req.Action.Cancels), len(req.Action.Modifies))
	return 1 + n/40
}

// RateWait blocks until the given weight is available in the shared IP budget.
func RateWait(ctx context.Context, weight int) error {
	return Limiter.WaitN(ctx, min(max(weight, 1), weightPerMinute))
}

// RateAvailable returns the currently available weight.
func RateAvailable() int {
	return int(Limiter.TokensAt(time.Now()))
}
