package service

import (
	"context"
	"encoding/json"
	"log/slog"
	"testing"
	"time"
)

const testCoin = "BTC"

func newTestCandlesSrv(t *testing.T, size int) *CandlesSrv {
	t.Helper()

	s := NewCandlesSrv(context.Background(), slog.New(slog.DiscardHandler), nil, nil, testCoin, "1m", size)
	t.Cleanup(s.cancel)
	return s
}

func candle(openMin int64, trades int64) Candle {
	open := openMin * 60_000
	return Candle{OpenTime: open, CloseTime: open + 59_999, Coin: testCoin, Interval: "1m", Trades: trades}
}

func markReady(s *CandlesSrv) {
	s.ready = true
	close(s.readyC)
}

func TestMergeKeepsVersionWithMostTrades(t *testing.T) {
	s := newTestCandlesSrv(t, 10)

	s.merge([]Candle{candle(1, 5), candle(2, 5)})
	s.merge([]Candle{candle(2, 7)}) // websocket update
	s.merge([]Candle{candle(2, 6)}) // late REST snapshot must not overwrite it
	s.merge([]Candle{candle(0, 1)}) // out of order insert

	if len(s.candles) != 3 {
		t.Fatalf("expected 3 candles, got %d", len(s.candles))
	}
	for i, want := range []int64{0, 1, 2} {
		if s.candles[i].OpenTime != want*60_000 {
			t.Errorf("candle %d: open time %d, want %d", i, s.candles[i].OpenTime, want*60_000)
		}
	}
	if s.candles[2].Trades != 7 {
		t.Errorf("expected newest version with 7 trades, got %d", s.candles[2].Trades)
	}
}

func TestMergeTrimsOldest(t *testing.T) {
	s := newTestCandlesSrv(t, 3)
	s.complete = true

	s.merge([]Candle{candle(1, 1), candle(2, 1), candle(3, 1), candle(4, 1)})

	if len(s.candles) != 3 || s.candles[0].OpenTime != 2*60_000 {
		t.Fatalf("expected oldest candle trimmed, got %+v", s.candles)
	}
	if s.complete {
		t.Error("trimming a partial cache must clear complete")
	}
}

func TestGetUsesOverlapSemantics(t *testing.T) {
	s := newTestCandlesSrv(t, 10)
	s.merge([]Candle{candle(1, 1), candle(2, 1), candle(3, 1), candle(4, 1)})
	markReady(s)

	// Start in the middle of candle 2, end at the open of candle 3: upstream returns candles 2 and 3.
	got, ok := s.Get(context.Background(), 2*60_000+30_000, 3*60_000, time.Second)
	if !ok {
		t.Fatal("expected cache hit")
	}
	if len(got) != 2 || got[0].OpenTime != 2*60_000 || got[1].OpenTime != 3*60_000 {
		t.Fatalf("unexpected candles %+v", got)
	}
}

func TestGetForwardsWhenStartBeforeCache(t *testing.T) {
	s := newTestCandlesSrv(t, 10)
	s.merge([]Candle{candle(5, 1), candle(6, 1)})
	markReady(s)

	if _, ok := s.Get(context.Background(), 60_000, 6*60_000, time.Second); ok {
		t.Error("expected miss for start before cached range")
	}

	s.complete = true
	if _, ok := s.Get(context.Background(), 60_000, 6*60_000, time.Second); !ok {
		t.Error("expected hit when cache holds the complete upstream history")
	}
}

func TestGetWaitsForReady(t *testing.T) {
	s := newTestCandlesSrv(t, 10)
	s.merge([]Candle{candle(1, 1)})

	if _, ok := s.Get(context.Background(), 60_000, 60_000, 10*time.Millisecond); ok {
		t.Error("expected miss before ready")
	}

	s.onDisconnected() // no-op while not ready
	markReady(s)
	if _, ok := s.Get(context.Background(), 60_000, 60_000, 10*time.Millisecond); !ok {
		t.Error("expected hit once ready")
	}

	s.onDisconnected()
	if _, ok := s.Get(context.Background(), 60_000, 60_000, 10*time.Millisecond); ok {
		t.Error("expected miss after disconnect")
	}
}

func TestOnMessageDecodesWebsocketCandle(t *testing.T) {
	s := newTestCandlesSrv(t, 10)
	s.onMessage(json.RawMessage(`{"t":60000,"T":119999,"s":"BTC","i":"1m","o":"1.0","c":"2.0","h":"3.0","l":"0.5","v":"10.5","n":42}`))

	want := Candle{OpenTime: 60_000, CloseTime: 119_999, Coin: testCoin, Interval: "1m", Open: "1.0", Close: "2.0", High: "3.0", Low: "0.5", Volume: "10.5", Trades: 42}
	if len(s.candles) != 1 || s.candles[0] != want {
		t.Fatalf("got %+v, want %+v", s.candles, want)
	}
}
