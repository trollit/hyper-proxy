package service

import (
	"encoding/json"
	"time"
)

// Websocket feed types, also used as info request types.
const (
	FeedCandle  = "candle"
	FeedL2Book  = "l2Book"
	FeedAllMids = "allMids"
)

// MaxCandles is the number of most recent candles Hyperliquid keeps available per coin and interval.
const MaxCandles = 5000

// IntervalDurations maps the candle intervals supported by Hyperliquid to their length.
var IntervalDurations = map[string]time.Duration{
	"1m":  1 * time.Minute,
	"3m":  3 * time.Minute,
	"5m":  5 * time.Minute,
	"15m": 15 * time.Minute,
	"30m": 30 * time.Minute,
	"1h":  1 * time.Hour,
	"2h":  2 * time.Hour,
	"4h":  4 * time.Hour,
	"8h":  8 * time.Hour,
	"12h": 12 * time.Hour,
	"1d":  24 * time.Hour,
	"3d":  3 * 24 * time.Hour,
	"1w":  7 * 24 * time.Hour,
	"1M":  30 * 24 * time.Hour,
}

// InfoRequest holds the fields of a POST /info body that the proxy needs for routing.
type InfoRequest struct {
	Type     string          `json:"type"`
	Coin     string          `json:"coin,omitempty"`
	Dex      string          `json:"dex,omitempty"`
	NSigFigs json.RawMessage `json:"nSigFigs,omitempty"`
	Mantissa json.RawMessage `json:"mantissa,omitempty"`
	Req      json.RawMessage `json:"req,omitempty"`

	// StartTime and EndTime bound fundingHistory requests.
	StartTime *int64 `json:"startTime,omitempty"`
	EndTime   *int64 `json:"endTime,omitempty"`
}

// CandleRequest is the "req" object of a candleSnapshot info request.
type CandleRequest struct {
	Coin      string `json:"coin"`
	Interval  string `json:"interval"`
	StartTime *int64 `json:"startTime"`
	EndTime   *int64 `json:"endTime"`
}

// CandleReq parses the "req" object of a candleSnapshot request.
func (r *InfoRequest) CandleReq() (*CandleRequest, error) {
	var cr CandleRequest
	if err := json.Unmarshal(r.Req, &cr); err != nil {
		return nil, err
	}
	return &cr, nil
}

// IsNull reports whether an optional raw JSON field is absent or null.
func IsNull(raw json.RawMessage) bool {
	return len(raw) == 0 || string(raw) == "null"
}

// Candle is a single OHLCV candle as returned by Hyperliquid.
type Candle struct {
	OpenTime  int64  `json:"t"`
	CloseTime int64  `json:"T"`
	Coin      string `json:"s"`
	Interval  string `json:"i"`
	Open      string `json:"o"`
	Close     string `json:"c"`
	High      string `json:"h"`
	Low       string `json:"l"`
	Volume    string `json:"v"`
	Trades    int64  `json:"n"`
}

func nowMillis() int64 {
	return time.Now().UnixMilli()
}
