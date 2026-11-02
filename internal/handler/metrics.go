package handler

import (
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"

	"github.com/trollit/hyper-proxy/internal/service"
)

var requestDuration, _ = otel.Meter("github.com/trollit/hyper-proxy/internal/handler").Float64Histogram(
	"hyperproxy.request.duration",
	metric.WithDescription("Duration of proxied requests by endpoint, info type and data source"),
	metric.WithUnit("s"),
	metric.WithExplicitBucketBoundaries(0.0005, 0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30),
)

// knownInfoTypes bounds the info.type attribute; client supplied types would otherwise
// create unbounded metric series.
var knownInfoTypes = map[string]bool{
	"candleSnapshot":           true,
	service.FeedL2Book:         true,
	service.FeedAllMids:        true,
	service.InfoFundingHistory: true,
	"meta":                     true,
	"spotMeta":                 true,
	"perpDexs":                 true,
	"metaAndAssetCtxs":         true,
	"spotMetaAndAssetCtxs":     true,
	"clearinghouseState":       true,
	"spotClearinghouseState":   true,
	"openOrders":               true,
	"frontendOpenOrders":       true,
	"orderStatus":              true,
	"userFills":                true,
	"userFillsByTime":          true,
	"userFunding":              true,
	"l2Snapshot":               true,
}

func requestAttrs(path, infoType, source string) metric.MeasurementOption {
	switch path {
	case "/info", "/exchange", "/status", "/restart":
	default:
		path = "other"
	}
	if infoType != "" && !knownInfoTypes[infoType] {
		infoType = "other"
	}
	if source == "" {
		source = "none"
	}
	return metric.WithAttributes(
		attribute.String("endpoint", path),
		attribute.String("info.type", infoType),
		attribute.String("source", source),
	)
}
