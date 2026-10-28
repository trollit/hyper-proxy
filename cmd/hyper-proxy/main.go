package main

import (
	"context"
	"errors"
	"fmt"
	stdlog "log"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/jessevdk/go-flags"

	"github.com/trollit/hyper-proxy/internal/handler"
	"github.com/trollit/hyper-proxy/internal/logcache"
	"github.com/trollit/hyper-proxy/internal/service"
)

const testnetURL = "https://api.hyperliquid-testnet.xyz"

type Config struct {
	Verbose            []bool        `short:"v" long:"verbose" env:"HPX_VERBOSE" description:"Verbose (debug) output"`
	Port               int           `short:"p" long:"port" env:"HPX_PORT" description:"Port to which to bind" default:"8092"`
	Upstream           string        `short:"u" long:"upstream" env:"HPX_UPSTREAM" description:"Hyperliquid API base URL" default:"https://api.hyperliquid.xyz"`
	Testnet            bool          `long:"testnet" env:"HPX_TESTNET" description:"Use the Hyperliquid testnet API (overrides --upstream)"`
	DisableFakeCandles bool          `short:"c" long:"disable-fake-candles" env:"HPX_DISABLE_FAKE_CANDLES" description:"Disable generation of fake candles when no trade has opened the current period yet"`
	CandleCacheSize    int           `long:"candle-cache-size" env:"HPX_CANDLE_CACHE_SIZE" description:"Candles kept per coin and interval (max 5000, upstream keeps 5000)" default:"5000"`
	MetaTTL            time.Duration `long:"meta-ttl" env:"HPX_META_TTL" description:"Cache duration for meta, spotMeta and perpDexs (0 disables)" default:"60s"`
	AssetCtxTTL        time.Duration `long:"asset-ctx-ttl" env:"HPX_ASSET_CTX_TTL" description:"Cache duration for metaAndAssetCtxs and spotMetaAndAssetCtxs (0 disables)" default:"5s"`
	AlwaysShowForwards bool          `short:"a" long:"always-show-forwards" env:"HPX_ALWAYS_SHOW_FORWARDS" description:"Always show requests forwarded via REST even if verbose is disabled"`
}

var (
	Version   = "dev"
	Buildtime = "unknown"
)

func main() {
	var config Config
	if _, err := flags.NewParser(&config, flags.Default).Parse(); err != nil {
		var flagsErr *flags.Error
		if errors.As(err, &flagsErr) && flagsErr.Type == flags.ErrHelp {
			os.Exit(0)
		}
		os.Exit(1)
	}

	level := slog.LevelInfo
	if len(config.Verbose) > 0 {
		level = slog.LevelDebug
	}
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: level}))
	slog.SetDefault(logger)

	logcache.SetLoggerHook(func(level, msg string) {
		switch level {
		case "warn":
			logger.Warn(msg)
		case "error":
			logger.Error(msg)
		default:
			logger.Info(msg)
		}
	})
	logcache.SetWriterHook(func(msg string) {
		// net/http ErrorLog messages typically include trailing newlines
		if len(msg) > 0 && msg[len(msg)-1] == '\n' {
			msg = msg[:len(msg)-1]
		}
		logger.Warn("http request", "msg", msg)
	})

	logger.Info("Hyperliquid proxy version", "version", Version, "build", Buildtime)

	if config.Testnet {
		config.Upstream = testnetURL
	}
	if config.CandleCacheSize < 1 || config.CandleCacheSize > service.MaxCandles {
		logger.Error("candle cache size must be between 1 and 5000", "size", config.CandleCacheSize)
		os.Exit(1)
	}
	if !config.DisableFakeCandles {
		logger.Info("Fake candles are enabled for faster processing, the feature can be disabled with --disable-fake-candles or -c")
	}
	if config.AlwaysShowForwards {
		logger.Info("Always show forwards is enabled, all API requests, that can't be served from websockets cached will be logged.")
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGHUP, syscall.SIGINT, syscall.SIGTERM, syscall.SIGQUIT)
	err := run(ctx, stop, logger, config)
	stop()
	if err != nil {
		logger.Error("proxy failed", "error", err)
		os.Exit(1)
	}
}

func run(ctx context.Context, stop context.CancelFunc, logger *slog.Logger, config Config) error {
	up := service.NewUpstream(config.Upstream, service.NewBanDetector(logger))
	srv := service.NewService(ctx, logger, up, service.Config{
		CandleCacheSize: config.CandleCacheSize,
		MetaTTL:         config.MetaTTL,
		AssetCtxTTL:     config.AssetCtxTTL,
	})
	defer srv.Stop()

	h, err := handler.NewHandler(logger, srv, handler.Options{
		EnableFakeCandles:  !config.DisableFakeCandles,
		AlwaysShowForwards: config.AlwaysShowForwards,
	}, stop)
	if err != nil {
		return err
	}

	// Create an HTTP server with a custom ErrorLog that suppresses repeated lines
	server := &http.Server{
		Addr:              fmt.Sprintf(":%d", config.Port),
		Handler:           h,
		ReadTimeout:       30 * time.Second,
		ReadHeaderTimeout: 10 * time.Second,
		WriteTimeout:      75 * time.Second,
		IdleTimeout:       120 * time.Second,
		ErrorLog:          stdlog.New(logcache.NewSuppressingWriter(os.Stderr), "", stdlog.LstdFlags),
	}

	errC := make(chan error, 1)
	go func() {
		logger.Info("proxy starting", "port", config.Port, "upstream", config.Upstream)
		errC <- server.ListenAndServe()
	}()

	select {
	case err := <-errC:
		return err
	case <-ctx.Done():
	}

	logger.Info("shutting down ...")
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := server.Shutdown(shutdownCtx); err != nil && !errors.Is(err, http.ErrServerClosed) {
		return err
	}
	return nil
}
