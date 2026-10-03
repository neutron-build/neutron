// Run with an application-owned DATABASE_URL. Configure HTTP tracing/exporters
// outside the ORM; ormotel receives the span from each operation's context.
package main

import (
	"context"
	"log"
	"net/http"
	"os"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/neutron-build/neutron/go/neutron"
	"github.com/neutron-build/neutron/go/orm"
	ormhttp "github.com/neutron-build/neutron/go/orm/http"
	ormotel "github.com/neutron-build/neutron/go/orm/otel"
)

func main() {
	pool, err := pgxpool.New(context.Background(), os.Getenv("DATABASE_URL"))
	if err != nil {
		log.Fatal("database pool configuration failed")
	}
	requests, err := ormhttp.NewTransactions(pool, ormhttp.Options{MaxResponseBytes: 4096, Observer: ormotel.Observe})
	if err != nil {
		pool.Close()
		log.Fatal("request transaction configuration failed")
	}
	handler, err := requests.Handler(func(ctx context.Context, session ormhttp.RequestSession, _ *http.Request) (ormhttp.Response, error) {
		// Application repositories receive this Executor or Scope explicitly. Neither
		// session nor connection is stored globally or retained across requests.
		if _, err := session.Executor.Exec(ctx, "SELECT 1"); err != nil {
			return ormhttp.Response{}, err
		}
		return ormhttp.Response{Body: []byte("ok\n")}, nil
	})
	if err != nil {
		pool.Close()
		log.Fatal("request handler configuration failed")
	}
	app := neutron.New(neutron.WithoutDefaultRoutes(), neutron.WithLifecycle(neutron.LifecycleHook{
		Name: "request database",
		OnStop: func(ctx context.Context) error {
			// App drains HTTP first. The request adapter also refuses new admission,
			// cancels any remaining scopes and waits for their bounded cleanup.
			if err := requests.Shutdown(ctx); err != nil {
				return err
			}
			pool.Close() // application closes its pool only after all scopes settle
			return nil
		},
	}))
	app.Router().Handle("GET /database", handler)
	if err := app.Run(""); err != nil {
		log.Fatal("application stopped with an error")
	}
}
