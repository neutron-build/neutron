// A Neutron Go SDK service: typed handlers, RFC 7807 errors, OpenAPI 3.1,
// /health, and a graceful SIGTERM drain, all from the SDK. It listens on
// NEUTRON_HOST:NEUTRON_PORT, which the coordinator assigns.
package main

import (
	"context"
	"log/slog"
	"os"
	"sync"
	"time"

	"github.com/neutron-build/neutron/go/neutron"
)

type Item struct {
	ID   int    `json:"id"`
	Name string `json:"name"`
}

type CreateItemInput struct {
	Name string `json:"name" validate:"required,min=1,max=80"`
}

type SlowInput struct {
	Seconds int `query:"seconds"`
}

func main() {
	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))
	var mu sync.Mutex
	items := []Item{{ID: 1, Name: "Hydrogen"}, {ID: 2, Name: "Helium"}}

	app := neutron.New(
		neutron.WithLogger(logger),
		neutron.WithOpenAPIInfo("Example API", "1.0.0"),
		neutron.WithMiddleware(neutron.DefaultStack(neutron.DefaultStackConfig{Logger: logger})...),
	)
	api := app.Router().Group("/api")
	neutron.Get(api, "/items", func(ctx context.Context, _ neutron.Empty) ([]Item, error) {
		mu.Lock()
		defer mu.Unlock()
		return append([]Item(nil), items...), nil
	})
	neutron.Post(api, "/items", func(ctx context.Context, in CreateItemInput) (Item, error) {
		mu.Lock()
		defer mu.Unlock()
		item := Item{ID: len(items) + 1, Name: in.Name}
		items = append(items, item)
		return item, nil
	})
	// Holds a request open so shutdown draining can be observed.
	neutron.Get(api, "/slow", func(ctx context.Context, in SlowInput) (Item, error) {
		time.Sleep(time.Duration(in.Seconds) * time.Second)
		return Item{ID: 0, Name: "done"}, nil
	})

	if err := app.Run(""); err != nil {
		logger.Error("server stopped", "error", err)
		os.Exit(1)
	}
}
