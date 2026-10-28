package handler

import (
	"encoding/json"
	"errors"
	"net/http"
	"time"

	"github.com/trollit/hyper-proxy/internal/service"
)

const (
	// The first candle request for a coin may queue behind the rate limiter for its snapshot.
	candleWait = 30 * time.Second
	feedWait   = 5 * time.Second
)

// info handles POST /info, the single endpoint for all Hyperliquid market data.
func (h *Handler) info(w http.ResponseWriter, r *http.Request) {
	body, ok := readBody(w, r)
	if !ok {
		return
	}

	var req service.InfoRequest
	if r.Method != http.MethodPost || json.Unmarshal(body, &req) != nil {
		h.forward(w, r, body, 20)
		return
	}

	switch req.Type {
	case "candleSnapshot":
		if h.candleSnapshot(w, r, &req) {
			return
		}
	case service.FeedL2Book:
		// The websocket only identifies books by coin, so only the default aggregation is cached.
		if service.IsNull(req.NSigFigs) && service.IsNull(req.Mantissa) {
			if data, ok := h.srv.Book(r.Context(), req.Coin, feedWait); ok {
				writeJSON(w, "websocket", http.StatusOK, data)
				return
			}
		}
	case service.FeedAllMids:
		if req.Dex == "" {
			if data, ok := h.srv.AllMids(r.Context(), feedWait); ok {
				writeJSON(w, "websocket", http.StatusOK, data)
				return
			}
		}
	default:
		if ttl, ok := h.srv.InfoTTL(req.Type); ok {
			h.cachedInfo(w, r, body, &req, ttl)
			return
		}
	}

	h.forward(w, r, body, service.InfoWeight(&req))
}

func (h *Handler) cachedInfo(w http.ResponseWriter, r *http.Request, body []byte, req *service.InfoRequest, ttl time.Duration) {
	res, err := h.srv.CachedInfo(r.Context(), body, service.InfoWeight(req), ttl)
	switch {
	case errors.Is(err, service.ErrBanned):
		_, until := h.srv.Upstream.BanDetector.Status()
		writeBackoff(w, until)
	case err != nil:
		if r.Context().Err() != nil {
			return
		}
		service.GetStatusTracker().RecordError(err)
		writeJSONError(w, http.StatusBadGateway, "bad_gateway", "upstream fetch failed")
	default:
		writeJSON(w, res.Source, res.StatusCode, res.Body)
	}
}

// candleSnapshot serves candles from the websocket cache. It returns false if the request has to be forwarded.
func (h *Handler) candleSnapshot(w http.ResponseWriter, r *http.Request, req *service.InfoRequest) bool {
	cr, err := req.CandleReq()
	if err != nil || cr.StartTime == nil {
		return false
	}

	now := time.Now().UnixMilli()
	end := now
	if cr.EndTime != nil {
		end = *cr.EndTime
	}
	if *cr.StartTime > end {
		return false
	}

	candles, ok := h.srv.Candles(r.Context(), cr.Coin, cr.Interval, *cr.StartTime, end, candleWait)
	if !ok {
		return false
	}

	// Until the first trade of a new period there is no candle for it; return a flat candle at the
	// previous close so clients see the current period.
	if h.opts.EnableFakeCandles && len(candles) > 0 {
		last := candles[len(candles)-1]
		openTime := last.CloseTime + 1
		closeTime := openTime + (last.CloseTime - last.OpenTime)
		if now >= openTime && now <= closeTime && end >= openTime {
			candles = append(candles, service.Candle{
				OpenTime:  openTime,
				CloseTime: closeTime,
				Coin:      last.Coin,
				Interval:  last.Interval,
				Open:      last.Close,
				Close:     last.Close,
				High:      last.Close,
				Low:       last.Close,
				Volume:    "0.0",
				Trades:    0,
			})
		}
	}

	buf := GetBuffer()
	defer PutBuffer(buf)

	encoder := json.NewEncoder(buf)
	encoder.SetEscapeHTML(false)
	if err := encoder.Encode(candles); err != nil {
		return false
	}

	writeJSON(w, "websocket", http.StatusOK, buf.Bytes())
	return true
}
