package service

import (
	"encoding/json"
	"log/slog"
	"testing"
)

type recordingSubscriber struct {
	messages []string
}

func (r *recordingSubscriber) onSubscribed()   {}
func (r *recordingSubscriber) onDisconnected() {}
func (r *recordingSubscriber) onMessage(data json.RawMessage) {
	r.messages = append(r.messages, string(data))
}

func TestDispatchRoutesBySubscription(t *testing.T) {
	m := NewWSManager("wss://example.invalid/ws", slog.New(slog.DiscardHandler))
	btc, eth, book, mids := &recordingSubscriber{}, &recordingSubscriber{}, &recordingSubscriber{}, &recordingSubscriber{}
	m.subs[Subscription{Type: FeedCandle, Coin: testCoin, Interval: "1m"}] = btc
	m.subs[Subscription{Type: FeedCandle, Coin: "ETH", Interval: "1m"}] = eth
	m.subs[Subscription{Type: FeedL2Book, Coin: "@107"}] = book
	m.subs[Subscription{Type: FeedAllMids}] = mids

	m.dispatch([]byte(`{"channel":"candle","data":{"t":1,"s":"BTC","i":"1m"}}`))
	m.dispatch([]byte(`{"channel":"candle","data":[{"t":2,"s":"ETH","i":"1m"},{"t":3,"s":"BTC","i":"1m"}]}`))
	m.dispatch([]byte(`{"channel":"candle","data":{"t":4,"s":"BTC","i":"5m"}}`))
	m.dispatch([]byte(`{"channel":"l2Book","data":{"coin":"@107","time":1,"levels":[[],[]]}}`))
	m.dispatch([]byte(`{"channel":"allMids","data":{"mids":{"BTC":"1"}}}`))
	m.dispatch([]byte(`{"channel":"pong"}`))

	if len(btc.messages) != 2 || len(eth.messages) != 1 || len(book.messages) != 1 || len(mids.messages) != 1 {
		t.Fatalf("unexpected routing: btc=%v eth=%v book=%v mids=%v", btc.messages, eth.messages, book.messages, mids.messages)
	}
}

func TestMidsExtract(t *testing.T) {
	s := NewMidsSrv(t.Context(), slog.New(slog.DiscardHandler), nil, nil)
	out, err := s.extract(json.RawMessage(`{"mids":{"BTC":"81461.5"}}`))
	if err != nil || string(out) != `{"BTC":"81461.5"}` {
		t.Fatalf("got %s, %v", out, err)
	}
}

func TestCanonicalKey(t *testing.T) {
	a := canonicalKey([]byte(`{"type": "metaAndAssetCtxs", "dex": "xyz"}`))
	b := canonicalKey([]byte(`{"dex":"xyz","type":"metaAndAssetCtxs"}`))
	if a != b {
		t.Errorf("keys differ: %q vs %q", a, b)
	}
}

func TestWeights(t *testing.T) {
	start := nowMillis() - 600*60_000
	req, _ := json.Marshal(map[string]any{"coin": "BTC", "interval": "1m", "startTime": start})

	tests := []struct {
		req  InfoRequest
		want int
	}{
		{InfoRequest{Type: FeedL2Book}, 2},
		{InfoRequest{Type: "meta"}, 20},
		{InfoRequest{Type: "userRole"}, 60},
		{InfoRequest{Type: "candleSnapshot", Req: req}, 20 + 601/60},
		{InfoRequest{Type: "candleSnapshot"}, 20 + MaxCandles/60},
	}
	for _, tt := range tests {
		if got := InfoWeight(&tt.req); got != tt.want {
			t.Errorf("InfoWeight(%s) = %d, want %d", tt.req.Type, got, tt.want)
		}
	}

	if got := ExchangeWeight([]byte(`{"action":{"type":"order","orders":[` + repeat(`{}`, 85) + `]}}`)); got != 3 {
		t.Errorf("ExchangeWeight = %d, want 3", got)
	}
}

func repeat(s string, n int) string {
	out := s
	for range n - 1 {
		out += "," + s
	}
	return out
}
