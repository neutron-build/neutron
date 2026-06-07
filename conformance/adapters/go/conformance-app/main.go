// Canonical Neutron conformance app (Go SDK).
//
// Boots a Neutron Go server with NO database dependency so the cross-SDK
// conformance runner can assert FRAMEWORK_CONTRACT.md against it. Every contract
// dimension is wired to a deterministic endpoint:
//
//	GET  /health            → contract health shape {status, nucleus, version}
//	GET  /openapi.json      → OpenAPI 3.1 document (auto-registered by Run)
//	GET  /docs              → Swagger UI (auto-registered by Run)
//	GET  /api/items         → 200 OK list (compression / request-id probe)
//	POST /api/items         → 422 validation error on bad body (RFC 7807 + errors[])
//	GET  /errors/bad-request, /errors/unauthorized, … → forced standard errors
//
// Listen address comes from PORT (and HOST), so the runner can pin an ephemeral
// port. The DefaultStack wires the contract middleware order.
//
// Run: PORT=8081 go run .
package main

import (
	"context"
	"os"
	"strconv"

	"github.com/neutron-dev/neutron-go/neutron"
)

type Item struct {
	ID    int64  `json:"id"`
	Name  string `json:"name"`
	Price float64 `json:"price"`
}

type NewItem struct {
	Name  string  `json:"name" validate:"required,min=1,max=200"`
	Price float64 `json:"price" validate:"gte=0"`
}

func main() {
	app := neutron.New(
		neutron.WithOpenAPIInfo("Neutron Conformance API", "9.9.9"),
		// DefaultStack enforces the FRAMEWORK_CONTRACT.md middleware order:
		// RequestID → Logging → Recovery → CORS → Compression → RateLimit → Auth → Timeout → OTel.
		neutron.WithMiddleware(neutron.DefaultStack(neutron.DefaultStackConfig{
			CORS:     &neutron.CORSOptions{AllowOrigins: []string{"*"}},
			Compress: true,
		})...),
	)

	api := app.Router().Group("/api")

	// List — large-ish JSON body so gzip negotiation is observable.
	neutron.Get(api, "/items", func(ctx context.Context, _ neutron.Empty) ([]Item, error) {
		items := make([]Item, 0, 50)
		for i := int64(1); i <= 50; i++ {
			items = append(items, Item{ID: i, Name: "conformance-item-" + strconv.FormatInt(i, 10), Price: float64(i)})
		}
		return items, nil
	})

	// Create — typed input triggers validation; bad body → 422 with errors[].
	neutron.Post(api, "/items", func(ctx context.Context, in NewItem) (Item, error) {
		return Item{ID: 1, Name: in.Name, Price: in.Price}, nil
	})

	// Forced standard errors — one route per §2 standard error code.
	r := app.Router()
	neutron.Get(r, "/errors/bad-request", func(ctx context.Context, _ neutron.Empty) (neutron.Empty, error) {
		return neutron.Empty{}, neutron.ErrBadRequest("forced bad request")
	})
	neutron.Get(r, "/errors/unauthorized", func(ctx context.Context, _ neutron.Empty) (neutron.Empty, error) {
		return neutron.Empty{}, neutron.ErrUnauthorized("forced unauthorized")
	})
	neutron.Get(r, "/errors/forbidden", func(ctx context.Context, _ neutron.Empty) (neutron.Empty, error) {
		return neutron.Empty{}, neutron.ErrForbidden("forced forbidden")
	})
	neutron.Get(r, "/errors/not-found", func(ctx context.Context, _ neutron.Empty) (neutron.Empty, error) {
		return neutron.Empty{}, neutron.ErrNotFound("forced not found")
	})
	neutron.Get(r, "/errors/conflict", func(ctx context.Context, _ neutron.Empty) (neutron.Empty, error) {
		return neutron.Empty{}, neutron.ErrConflict("forced conflict")
	})
	neutron.Get(r, "/errors/rate-limited", func(ctx context.Context, _ neutron.Empty) (neutron.Empty, error) {
		return neutron.Empty{}, neutron.ErrRateLimited("forced rate limited")
	})
	neutron.Get(r, "/errors/internal", func(ctx context.Context, _ neutron.Empty) (neutron.Empty, error) {
		return neutron.Empty{}, neutron.ErrInternal("forced internal error")
	})

	addr := ":8081"
	if p := os.Getenv("PORT"); p != "" {
		host := os.Getenv("HOST")
		addr = host + ":" + p
	}
	if err := app.Run(addr); err != nil {
		os.Exit(1)
	}
}
