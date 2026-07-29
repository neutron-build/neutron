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

	// Nothing syncs on its own without this: the mirror would only advance
	// when a client happened to call the sync endpoint, leaving it stale by
	// exactly as long as nobody looked.
	scheduler := mail.NewScheduler(store, engine, func(a mail.AccountID) (mail.Adapter, bool) {
		if svc.Adapters == nil {
			return nil, false
		}
		return svc.Adapters(a)
	}, log)
	scheduler.Interval = syncInterval
	go func() {
		if err := scheduler.Run(ctx); err != nil && !errors.Is(err, context.Canceled) {
			log.Error("scheduler stopped", "err", err)
		}
	}()

	// Adapters are not wired here. Constructing one needs a credential per
	// account, and credential custody is deployment-specific: an operator
	// self-hosting for one mailbox supplies an app password, while a
	// multi-tenant deployment resolves an OAuth token per account. Leaving
	// this nil serves the mirror read-only, which is a useful state on its
	// own and a safe default.
	svc.Adapters = nil

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
