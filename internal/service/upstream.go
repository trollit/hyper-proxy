package service

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"
)

// ErrBanned is returned when requests are suspended because the upstream rate limited us.
var ErrBanned = errors.New("upstream rate limited, backing off")

const maxResponseSize = 64 << 20

// Response is a fully read upstream response.
type Response struct {
	StatusCode int
	Body       []byte
}

// Upstream performs requests against the Hyperliquid REST API.
type Upstream struct {
	BaseURL     string
	BanDetector *BanDetector

	transport *http.Transport
	client    *http.Client
}

func NewUpstream(baseURL string, bd *BanDetector) *Upstream {
	transport := &http.Transport{
		Proxy:               http.ProxyFromEnvironment,
		MaxIdleConns:        200,
		MaxIdleConnsPerHost: 50,
		MaxConnsPerHost:     50,
		IdleConnTimeout:     90 * time.Second,
		TLSHandshakeTimeout: 10 * time.Second,
		ForceAttemptHTTP2:   true,
	}

	return &Upstream{
		BaseURL:     strings.TrimSuffix(baseURL, "/"),
		BanDetector: bd,
		transport:   transport,
		client:      &http.Client{Transport: transport, Timeout: 60 * time.Second},
	}
}

// Transport returns the pooled transport, shared with the reverse proxy.
func (u *Upstream) Transport() http.RoundTripper {
	return u.transport
}

// WebsocketURL returns the websocket endpoint matching the REST base URL.
func (u *Upstream) WebsocketURL() string {
	switch {
	case strings.HasPrefix(u.BaseURL, "https://"):
		return "wss://" + strings.TrimPrefix(u.BaseURL, "https://") + "/ws"
	case strings.HasPrefix(u.BaseURL, "http://"):
		return "ws://" + strings.TrimPrefix(u.BaseURL, "http://") + "/ws"
	default:
		return u.BaseURL + "/ws"
	}
}

// PostInfo sends an info request, honouring the shared rate limit and the ban detector.
func (u *Upstream) PostInfo(ctx context.Context, body []byte, weight int) (*Response, error) {
	if u.BanDetector.IsBanned() {
		return nil, ErrBanned
	}

	if err := RateWait(ctx, weight); err != nil {
		return nil, fmt.Errorf("rate wait: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, u.BaseURL+"/info", bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := u.client.Do(req)
	if u.BanDetector.CheckResponse(resp, err) {
		if resp != nil {
			resp.Body.Close()
		}
		return nil, ErrBanned
	}
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	data, err := io.ReadAll(io.LimitReader(resp.Body, maxResponseSize))
	if err != nil {
		return nil, err
	}

	return &Response{StatusCode: resp.StatusCode, Body: data}, nil
}

// waitForBan sleeps until a current ban is lifted or the context ends.
func waitForBan(ctx context.Context, bd *BanDetector) {
	banned, until := bd.Status()
	if !banned {
		return
	}

	t := time.NewTimer(time.Until(until))
	defer t.Stop()
	select {
	case <-ctx.Done():
	case <-t.C:
	}
}
