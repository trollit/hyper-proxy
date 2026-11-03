// Package telemetry sets up OpenTelemetry tracing and metrics from the standard OTEL_* environment
// variables. Nothing is exported unless an exporter or OTLP endpoint is configured.
package telemetry

import (
	"context"
	"errors"
	"os"
	"strconv"

	"go.opentelemetry.io/contrib/exporters/autoexport"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.43.0"
)

// ServiceName is the default service.name, overridable with OTEL_SERVICE_NAME.
const ServiceName = "hyper-proxy"

// Setup installs the global tracer and meter providers. The returned function flushes and stops them.
func Setup(ctx context.Context, version string) (func(context.Context) error, error) {
	noop := func(context.Context) error { return nil }
	if disabled, _ := strconv.ParseBool(os.Getenv("OTEL_SDK_DISABLED")); disabled {
		return noop, nil
	}

	res, err := resource.Merge(resource.Default(), resource.NewSchemaless(
		semconv.ServiceName(ServiceName),
		semconv.ServiceVersion(version),
	))
	if err != nil {
		return noop, err
	}
	// OTEL_SERVICE_NAME and OTEL_RESOURCE_ATTRIBUTES take precedence.
	if res, err = resource.Merge(res, resource.Environment()); err != nil {
		return noop, err
	}

	var shutdowns []func(context.Context) error
	shutdown := func(ctx context.Context) error {
		errs := make([]error, 0, len(shutdowns))
		for _, fn := range shutdowns {
			errs = append(errs, fn(ctx))
		}
		return errors.Join(errs...)
	}

	if configured("TRACES") {
		exp, err := autoexport.NewSpanExporter(ctx)
		if err != nil {
			return noop, err
		}
		if !autoexport.IsNoneSpanExporter(exp) {
			tp := sdktrace.NewTracerProvider(sdktrace.WithBatcher(exp), sdktrace.WithResource(res))
			otel.SetTracerProvider(tp)
			shutdowns = append(shutdowns, tp.Shutdown)
		}
	}

	if configured("METRICS") {
		reader, err := autoexport.NewMetricReader(ctx)
		if err != nil {
			return noop, errors.Join(err, shutdown(ctx))
		}
		if !autoexport.IsNoneMetricReader(reader) {
			mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader), sdkmetric.WithResource(res))
			otel.SetMeterProvider(mp)
			shutdowns = append(shutdowns, mp.Shutdown)
		}
	}

	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(propagation.TraceContext{}, propagation.Baggage{}))
	return shutdown, nil
}

// configured reports whether a signal ("TRACES" or "METRICS") has an exporter or OTLP endpoint set.
// autoexport would otherwise default to OTLP on localhost and log export errors.
func configured(signal string) bool {
	for _, key := range []string{
		"OTEL_" + signal + "_EXPORTER",
		"OTEL_EXPORTER_OTLP_ENDPOINT",
		"OTEL_EXPORTER_OTLP_" + signal + "_ENDPOINT",
	} {
		if os.Getenv(key) != "" {
			return true
		}
	}
	return false
}
