// Command neutron-mail runs the mail connector service.
//
// It mirrors connected mailboxes into Nucleus and serves them over HTTP.
// Consumers — an inbox UI, an agent's toolset — read that mirror and never
// speak a mail protocol themselves.
package main

import (
	"context"
	"errors"
	"flag"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/neutron-build/neutron/mail"
	"github.com/neutron-build/neutron/mail/dialer"
)

func main() {
	var (
		addr     = flag.String("addr", envOr("MAIL_ADDR", ":8090"), "listen address")
		dbURL    = flag.String("database-url", os.Getenv("DATABASE_URL"), "Nucleus or PostgreSQL connection URL")
		logLevel = flag.String("log-level", envOr("LOG_LEVEL", "info"), "debug, info, warn, or error")
		interval = flag.Duration("sync-interval", 5*time.Minute, "how often to sync each account")
	)
	flag.Parse()
	syncInterval := *interval

	log := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: parseLevel(*logLevel)}))
	slog.SetDefault(log)

	if *dbURL == "" {
		log.Error("no database URL; set DATABASE_URL or pass -database-url")
		os.Exit(1)
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	store, err := mail.Open(ctx, *dbURL)
	if err != nil {
		log.Error("could not connect to the database", "err", err)
		os.Exit(1)
	}
	defer store.Close()

	if err := store.Migrate(ctx); err != nil {
		log.Error("migration failed", "err", err)
		os.Exit(1)
	}

	engine := mail.NewEngine(store, log)
	svc := mail.NewService(store, engine)

	// Adapters are built per request from the credential the caller sends.
	// The engine stores no credentials of its own: both consuming products
	// run Better Auth, which already holds and refreshes provider tokens,
	// and a second credential store here would mean a second thing to
	// secure and a second refresh implementation to keep correct.
	svc.Resolve = dialer.New()

	// Sync is caller-driven for the same reason. Background polling needs a
	// credential when no request is in flight, and this process has none —
	// so the app, which does, calls POST /sync on its own cadence. The
	// built-in scheduler stays available for a self-hosted deployment that
	// holds its own app password.
	log.Info("credentials are per-request; sync is driven by the caller",
		"sync_interval_flag_unused", syncInterval)

	server := &http.Server{
		Addr:              *addr,
		Handler:           svc.Handler(),
		ReadHeaderTimeout: 10 * time.Second,
	}

	go func() {
		log.Info("mail service listening", "addr", *addr, "version", mail.Version)
		if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Error("server failed", "err", err)
			stop()
		}
	}()

	<-ctx.Done()

	// The 30 second drain matches every other Neutron framework, so a
	// rolling deploy behaves the same whatever the service is written in.
	log.Info("shutting down")
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := server.Shutdown(shutdownCtx); err != nil {
		log.Error("graceful shutdown failed", "err", err)
	}
}

func envOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func parseLevel(s string) slog.Level {
	switch s {
	case "debug":
		return slog.LevelDebug
	case "warn":
		return slog.LevelWarn
	case "error":
		return slog.LevelError
	default:
		return slog.LevelInfo
	}
}
