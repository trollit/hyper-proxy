package handler

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httputil"
	"net/url"
	"strconv"
	"sync"
	"time"

	"github.com/trollit/hyper-proxy/internal/logcache"
	"github.com/trollit/hyper-proxy/internal/service"
)

const maxRequestBody = 1 << 20

// Options controls the behavior of the handler.
type Options struct {
	EnableFakeCandles  bool
	AlwaysShowForwards bool
}

// Handler serves cached Hyperliquid API responses and forwards everything else upstream.
type Handler struct {
	logger  *slog.Logger
	srv     *service.Service
	opts    Options
	proxy   *httputil.ReverseProxy
	restart context.CancelFunc
}

// bufferPool implements httputil.BufferPool interface.
type bufferPool struct {
	pool sync.Pool
}

func (bp *bufferPool) Get() []byte {
	if b, ok := bp.pool.Get().(*[]byte); ok {
		return *b
	}
	return make([]byte, 32*1024)
}

func (bp *bufferPool) Put(b []byte) {
	bp.pool.Put(&b)
}

// NewHandler creates the proxy handler. restart is called to shut the process down via /restart.
func NewHandler(logger *slog.Logger, srv *service.Service, opts Options, restart context.CancelFunc) (*Handler, error) {
	target, err := url.Parse(srv.Upstream.BaseURL)
	if err != nil {
		return nil, fmt.Errorf("invalid upstream URL: %w", err)
	}

	h := &Handler{logger: logger, srv: srv, opts: opts, restart: restart}
	h.proxy = &httputil.ReverseProxy{
		Rewrite: func(pr *httputil.ProxyRequest) {
			pr.SetURL(target)
		},
		Transport:      srv.Upstream.Transport(),
		BufferPool:     &bufferPool{},
		ModifyResponse: h.modifyResponse,
		ErrorHandler:   h.proxyError,
	}

	return h, nil
}

func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	start := time.Now()
	service.GetStatusTracker().RecordRequest()

	switch r.URL.Path {
	case "/status":
		h.status(w)
	case "/restart":
		h.restartHandler(w, r)
	case "/info":
		h.info(w, r)
	default:
		body, ok := readBody(w, r)
		if !ok {
			return
		}
		weight := 20
		if r.URL.Path == "/exchange" {
			weight = service.ExchangeWeight(body)
		}
		h.forward(w, r, body, weight)
	}

	h.logger.Debug("request served",
		"duration", time.Since(start),
		"method", r.Method,
		"uri", r.RequestURI,
		"remote", r.RemoteAddr,
	)
}

func readBody(w http.ResponseWriter, r *http.Request) ([]byte, bool) {
	if r.Body == nil {
		return nil, true
	}

	body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, maxRequestBody))
	if err != nil {
		writeJSONError(w, http.StatusRequestEntityTooLarge, "request_too_large", "request body could not be read")
		return nil, false
	}
	return body, true
}

// forward proxies the request to the upstream API, honouring the rate limit and any active ban.
func (h *Handler) forward(w http.ResponseWriter, r *http.Request, body []byte, weight int) {
	logger := h.logger.With("method", r.Method, "uri", r.RequestURI, "remote", r.RemoteAddr)

	if banned, until := h.srv.Upstream.BanDetector.Status(); banned {
		logcache.LogOncePerDuration("warn", fmt.Sprintf("API is rate limited, rejecting requests until %v", until))
		writeBackoff(w, until)
		return
	}

	msg := "Request was not cachable (forwarded to upstream)"
	if h.opts.AlwaysShowForwards {
		logger.Info(msg, "weight", weight)
	} else {
		logger.Debug(msg, "weight", weight)
	}

	if err := service.RateWait(r.Context(), weight); err != nil {
		logcache.LogOncePerDuration("error", fmt.Sprintf("Rate wait failed: %v", err))
		writeJSONError(w, http.StatusServiceUnavailable, "rate_wait_failed", "service temporarily unavailable")
		return
	}

	out := r.Clone(r.Context())
	out.Body = io.NopCloser(bytes.NewReader(body))
	out.ContentLength = int64(len(body))
	h.proxy.ServeHTTP(w, out)
}

func (h *Handler) modifyResponse(resp *http.Response) error {
	if !h.srv.Upstream.BanDetector.CheckResponse(resp, nil) {
		resp.Header.Set("Data-Source", "upstream")
		return nil
	}

	// Pass the rate limit on to the client, with a Retry-After matching our own backoff.
	_, until := h.srv.Upstream.BanDetector.Status()
	resp.Header.Set("Retry-After", strconv.Itoa(retryAfterSeconds(until)))
	resp.Header.Set("X-Backoff-Until", until.Format(time.RFC3339))
	resp.Header.Set("Data-Source", "upstream")
	return nil
}

func (h *Handler) proxyError(w http.ResponseWriter, r *http.Request, err error) {
	if errors.Is(err, context.Canceled) {
		return
	}

	logcache.LogOncePerDuration("error", fmt.Sprintf("proxy transport error: %v", err))
	service.GetStatusTracker().RecordError(err)
	h.srv.Upstream.BanDetector.CheckResponse(nil, err)

	writeJSONError(w, http.StatusBadGateway, "bad_gateway", "upstream fetch failed")
}

func retryAfterSeconds(until time.Time) int {
	secs := int(time.Until(until).Seconds())
	if secs < 1 {
		secs = 30
	}
	return secs
}

func writeBackoff(w http.ResponseWriter, until time.Time) {
	w.Header().Set("Retry-After", strconv.Itoa(retryAfterSeconds(until)))
	w.Header().Set("X-Backoff-Until", until.Format(time.RFC3339))
	w.Header().Set("Data-Source", "ban-protection")
	writeJSONError(w, http.StatusTooManyRequests, "rate_limited", "upstream rate limit hit, proxy is backing off")
}

func writeJSONError(w http.ResponseWriter, status int, code, msg string) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(map[string]string{"error": code, "message": msg})
}

func writeJSON(w http.ResponseWriter, source string, status int, data []byte) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Data-Source", source)
	w.WriteHeader(status)
	_, _ = w.Write(data)
}

func (h *Handler) status(w http.ResponseWriter) {
	banned, until := h.srv.Upstream.BanDetector.Status()
	banInfo := map[string]any{"banned": banned, "recovery_time": nil}
	if banned {
		banInfo["recovery_time"] = until.Format(time.RFC3339)
	}

	response := map[string]any{
		"proxy_status": service.GetStatusTracker().GetStatus(),
		"upstream":     h.srv.Upstream.BaseURL,
		"ban_info":     banInfo,
		"cache":        h.srv.Stats(),
		"config": map[string]any{
			"fake_candles_enabled": h.opts.EnableFakeCandles,
			"always_show_forwards": h.opts.AlwaysShowForwards,
		},
	}

	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(response); err != nil {
		h.logger.Error("Failed to encode status response", "err", err)
	}
}

func (h *Handler) restartHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeJSONError(w, http.StatusMethodNotAllowed, "method_not_allowed", "only GET method allowed")
		return
	}

	h.logger.Warn("RESTART initiated", "remote", r.RemoteAddr)

	w.Header().Set("Content-Type", "application/json")
	response := map[string]any{
		"message":   "Restart initiated",
		"status":    "success",
		"timestamp": time.Now().Format(time.RFC3339),
		"warning":   "Service will restart in 2 seconds. This will interrupt all active connections.",
	}
	if err := json.NewEncoder(w).Encode(response); err != nil {
		h.logger.Error("Failed to encode restart response", "err", err)
	}
	if flusher, ok := w.(http.Flusher); ok {
		flusher.Flush()
	}

	// The process exits once shut down; a process manager or Docker restart policy brings it back.
	go func() {
		time.Sleep(2 * time.Second)
		h.logger.Warn("Executing restart")
		h.restart()
	}()
}
