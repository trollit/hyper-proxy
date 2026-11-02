package service

import (
	"context"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

// Instruments are created on the global meter provider, which forwards to the provider installed
// by the telemetry package (or discards everything when none is).
var meter = otel.Meter("github.com/trollit/hyper-proxy/internal/service")

var (
	rateWeight = must(meter.Int64Counter("hyperproxy.ratelimit.weight",
		metric.WithDescription("Rate limit weight spent on upstream requests"),
		metric.WithUnit("{weight}")))
	rateWait = must(meter.Float64Histogram("hyperproxy.ratelimit.wait.duration",
		metric.WithDescription("Time spent waiting for rate limit weight"),
		metric.WithUnit("s"),
		metric.WithExplicitBucketBoundaries(0, 0.01, 0.05, 0.1, 0.5, 1, 5, 10, 30, 60)))
	bans = must(meter.Int64Counter("hyperproxy.upstream.bans",
		metric.WithDescription("Times the proxy started backing off from the upstream")))
	wsConnects = must(meter.Int64Counter("hyperproxy.websocket.connects",
		metric.WithDescription("Websocket connection attempts"),
		metric.WithUnit("{connection}")))
	wsMessages = must(meter.Int64Counter("hyperproxy.websocket.messages",
		metric.WithDescription("Websocket messages received"),
		metric.WithUnit("{message}")))
	fundingFills = must(meter.Int64Counter("hyperproxy.funding.fills",
		metric.WithDescription("Funding history series fills from the upstream")))
)

func must[T any](instrument T, err error) T {
	if err != nil {
		otel.Handle(err)
	}
	return instrument
}

var (
	resultOK    = metric.WithAttributes(attribute.String("result", "ok"))
	resultError = metric.WithAttributes(attribute.String("result", "failed"))
)

func resultAttr(err error) metric.MeasurementOption {
	if err != nil {
		return resultError
	}
	return resultOK
}

// registerGauges reports the state of the service on every collection.
func (s *Service) registerGauges() (metric.Registration, error) {
	connected, err := meter.Int64ObservableGauge("hyperproxy.websocket.connected",
		metric.WithDescription("Whether the websocket is connected (1) or not (0)"))
	if err != nil {
		return nil, err
	}
	subscriptions, err := meter.Int64ObservableGauge("hyperproxy.websocket.subscriptions",
		metric.WithDescription("Active websocket subscriptions"),
		metric.WithUnit("{subscription}"))
	if err != nil {
		return nil, err
	}
	feeds, err := meter.Int64ObservableGauge("hyperproxy.feeds",
		metric.WithDescription("Active websocket backed feeds"),
		metric.WithUnit("{feed}"))
	if err != nil {
		return nil, err
	}
	cacheEntries, err := meter.Int64ObservableGauge("hyperproxy.cache.entries",
		metric.WithDescription("Entries in the REST caches"),
		metric.WithUnit("{entry}"))
	if err != nil {
		return nil, err
	}
	available, err := meter.Int64ObservableGauge("hyperproxy.ratelimit.available",
		metric.WithDescription("Rate limit weight currently available"),
		metric.WithUnit("{weight}"))
	if err != nil {
		return nil, err
	}
	banned, err := meter.Int64ObservableGauge("hyperproxy.upstream.banned",
		metric.WithDescription("Whether the proxy is backing off from the upstream (1) or not (0)"))
	if err != nil {
		return nil, err
	}

	return meter.RegisterCallback(func(_ context.Context, o metric.Observer) error {
		o.ObserveInt64(connected, boolInt(s.ws.Connected()))
		o.ObserveInt64(subscriptions, int64(s.ws.Subscriptions()))
		o.ObserveInt64(available, int64(RateAvailable()))
		o.ObserveInt64(banned, boolInt(s.Upstream.BanDetector.IsBanned()))
		o.ObserveInt64(cacheEntries, int64(s.infoCache.Len()), metric.WithAttributes(attribute.String("cache", "info")))
		o.ObserveInt64(cacheEntries, int64(s.funding.Len()), metric.WithAttributes(attribute.String("cache", "funding")))

		counts := map[string]int64{FeedCandle: 0, FeedL2Book: 0, FeedAllMids: 0}
		failed := map[string]int64{}
		s.mu.Lock()
		for sub, e := range s.feeds {
			counts[sub.Type]++
			if e.feed.Failed() {
				failed[sub.Type]++
			}
		}
		s.mu.Unlock()
		for typ, n := range counts {
			o.ObserveInt64(feeds, n-failed[typ], metric.WithAttributes(attribute.String("type", typ), attribute.String("state", "active")))
			o.ObserveInt64(feeds, failed[typ], metric.WithAttributes(attribute.String("type", typ), attribute.String("state", "failed")))
		}
		return nil
	}, connected, subscriptions, feeds, cacheEntries, available, banned)
}

func boolInt(b bool) int64 {
	if b {
		return 1
	}
	return 0
}
