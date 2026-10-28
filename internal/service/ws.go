package service

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"sync"
	"time"

	"github.com/gorilla/websocket"
	"golang.org/x/time/rate"

	"github.com/trollit/hyper-proxy/internal/tool"
)

const (
	// Hyperliquid allows at most 1000 subscriptions and 10 connections per IP, so all
	// feeds are multiplexed over a single connection.
	maxSubscriptions = 1000
	pingInterval     = 30 * time.Second
	readTimeout      = 90 * time.Second
	writeTimeout     = 10 * time.Second
)

var ErrTooManySubscriptions = errors.New("websocket subscription limit reached")

// Subscription identifies a websocket feed. It is also the routing key for incoming messages.
type Subscription struct {
	Type     string `json:"type"`
	Coin     string `json:"coin,omitempty"`
	Interval string `json:"interval,omitempty"`
}

// wsSubscriber receives the messages of a single subscription.
type wsSubscriber interface {
	// onSubscribed is called once the subscription is active on a live connection,
	// both initially and after every reconnect.
	onSubscribed()
	// onDisconnected is called when the connection carrying the subscription is lost.
	onDisconnected()
	onMessage(data json.RawMessage)
}

type wsCommand struct {
	Method       string        `json:"method"`
	Subscription *Subscription `json:"subscription,omitempty"`
}

// WSManager maintains a single websocket connection to Hyperliquid and multiplexes subscriptions over it.
type WSManager struct {
	url    string
	logger *slog.Logger

	mu   sync.Mutex
	conn *websocket.Conn
	subs map[Subscription]wsSubscriber

	writeMu sync.Mutex
	// Hyperliquid allows 2000 messages per minute sent across all connections.
	sendLimiter *rate.Limiter
}

func NewWSManager(url string, logger *slog.Logger) *WSManager {
	return &WSManager{
		url:         url,
		logger:      logger.With("component", "websocket"),
		subs:        make(map[Subscription]wsSubscriber),
		sendLimiter: rate.NewLimiter(rate.Every(time.Minute/1800), 100),
	}
}

// Connected reports whether the websocket is currently connected.
func (m *WSManager) Connected() bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.conn != nil
}

// Subscriptions returns the number of registered subscriptions.
func (m *WSManager) Subscriptions() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.subs)
}

// Run keeps the connection alive until ctx is done, reconnecting and resubscribing as needed.
func (m *WSManager) Run(ctx context.Context) {
	for d := tool.NewDelayIterator(); ctx.Err() == nil; d.Delay() {
		dialCtx, cancel := context.WithTimeout(ctx, 15*time.Second)
		conn, resp, err := websocket.DefaultDialer.DialContext(dialCtx, m.url, nil)
		cancel()
		if resp != nil && resp.Body != nil {
			resp.Body.Close()
		}
		if err != nil {
			if ctx.Err() == nil {
				m.logger.Error("websocket connection failed", "url", m.url, "error", err)
			}
			continue
		}

		m.logger.Info("websocket connected", "url", m.url)
		d.Reset()
		err = m.serve(ctx, conn)
		if ctx.Err() != nil {
			return
		}
		m.logger.Warn("websocket disconnected, reconnecting", "error", err)
	}
}

// Subscribe registers a subscriber and subscribes immediately if connected.
func (m *WSManager) Subscribe(sub Subscription, s wsSubscriber) error {
	m.mu.Lock()
	if _, ok := m.subs[sub]; !ok && len(m.subs) >= maxSubscriptions {
		m.mu.Unlock()
		return ErrTooManySubscriptions
	}
	m.subs[sub] = s
	conn := m.conn
	m.mu.Unlock()

	if conn == nil {
		// Subscribed by serve() once connected.
		return nil
	}

	if err := m.send(context.Background(), conn, wsCommand{Method: "subscribe", Subscription: &sub}); err != nil {
		// The read loop notices the broken connection and resubscribes after reconnecting.
		m.logger.Warn("websocket subscribe failed", "subscription", sub, "error", err)
		return nil
	}
	s.onSubscribed()
	return nil
}

// Unsubscribe removes a subscriber and unsubscribes upstream if connected.
func (m *WSManager) Unsubscribe(sub Subscription) {
	m.mu.Lock()
	if _, ok := m.subs[sub]; !ok {
		m.mu.Unlock()
		return
	}
	delete(m.subs, sub)
	conn := m.conn
	m.mu.Unlock()

	if conn == nil {
		return
	}
	if err := m.send(context.Background(), conn, wsCommand{Method: "unsubscribe", Subscription: &sub}); err != nil {
		m.logger.Warn("websocket unsubscribe failed", "subscription", sub, "error", err)
	}
}

func (m *WSManager) serve(ctx context.Context, conn *websocket.Conn) error {
	connCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	go func() {
		<-connCtx.Done()
		conn.Close()
	}()

	m.mu.Lock()
	m.conn = conn
	subs := make([]Subscription, 0, len(m.subs))
	for sub := range m.subs {
		subs = append(subs, sub)
	}
	m.mu.Unlock()

	defer m.disconnected()

	go m.resubscribe(connCtx, conn, subs)
	go m.ping(connCtx, conn)

	for {
		if err := conn.SetReadDeadline(time.Now().Add(readTimeout)); err != nil {
			return err
		}
		_, msg, err := conn.ReadMessage()
		if err != nil {
			return err
		}
		m.dispatch(msg)
	}
}

func (m *WSManager) resubscribe(ctx context.Context, conn *websocket.Conn, subs []Subscription) {
	for _, sub := range subs {
		m.mu.Lock()
		s, ok := m.subs[sub]
		m.mu.Unlock()
		if !ok {
			continue
		}

		if err := m.send(ctx, conn, wsCommand{Method: "subscribe", Subscription: &sub}); err != nil {
			m.logger.Warn("websocket resubscribe failed", "subscription", sub, "error", err)
			return
		}
		s.onSubscribed()
	}

	if len(subs) > 0 {
		m.logger.Info("websocket resubscribed", "subscriptions", len(subs))
	}
}

func (m *WSManager) ping(ctx context.Context, conn *websocket.Conn) {
	t := time.NewTicker(pingInterval)
	defer t.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			if err := m.send(ctx, conn, wsCommand{Method: "ping"}); err != nil {
				m.logger.Warn("websocket ping failed", "error", err)
				conn.Close()
				return
			}
		}
	}
}

func (m *WSManager) disconnected() {
	m.mu.Lock()
	m.conn = nil
	subs := make([]wsSubscriber, 0, len(m.subs))
	for _, s := range m.subs {
		subs = append(subs, s)
	}
	m.mu.Unlock()

	for _, s := range subs {
		s.onDisconnected()
	}
}

func (m *WSManager) send(ctx context.Context, conn *websocket.Conn, cmd wsCommand) error {
	if err := m.sendLimiter.Wait(ctx); err != nil {
		return err
	}

	m.writeMu.Lock()
	defer m.writeMu.Unlock()

	if err := conn.SetWriteDeadline(time.Now().Add(writeTimeout)); err != nil {
		return err
	}
	return conn.WriteJSON(cmd)
}

func (m *WSManager) dispatch(msg []byte) {
	var env struct {
		Channel string          `json:"channel"`
		Data    json.RawMessage `json:"data"`
	}
	if err := json.Unmarshal(msg, &env); err != nil {
		m.logger.Warn("websocket message could not be decoded", "error", err)
		return
	}

	switch env.Channel {
	case "pong", "subscriptionResponse":
	case "error":
		m.logger.Warn("websocket error message", "data", string(env.Data))
	case FeedCandle:
		// The docs describe Candle[], the live feed sends a single candle; accept both.
		items := []json.RawMessage{env.Data}
		if len(env.Data) > 0 && env.Data[0] == '[' {
			if err := json.Unmarshal(env.Data, &items); err != nil {
				m.logger.Warn("websocket candle message could not be decoded", "error", err)
				return
			}
		}
		for _, item := range items {
			var c struct {
				Coin     string `json:"s"`
				Interval string `json:"i"`
			}
			if err := json.Unmarshal(item, &c); err != nil {
				continue
			}
			m.route(Subscription{Type: FeedCandle, Coin: c.Coin, Interval: c.Interval}, item)
		}
	case FeedL2Book:
		var b struct {
			Coin string `json:"coin"`
		}
		if err := json.Unmarshal(env.Data, &b); err != nil {
			return
		}
		m.route(Subscription{Type: FeedL2Book, Coin: b.Coin}, env.Data)
	case FeedAllMids:
		m.route(Subscription{Type: FeedAllMids}, env.Data)
	default:
		m.logger.Debug("websocket message on unhandled channel", "channel", env.Channel)
	}
}

func (m *WSManager) route(sub Subscription, data json.RawMessage) {
	m.mu.Lock()
	s, ok := m.subs[sub]
	m.mu.Unlock()

	if !ok {
		m.logger.Debug("websocket message without subscriber", "subscription", sub)
		return
	}
	s.onMessage(data)
}
