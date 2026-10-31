package nucleus

import (
	"context"
	"errors"
	"fmt"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/neutron-build/neutron/go/neutron"
)

type closeErrorRows struct {
	mockScanRows
	next, closes         int
	rowCount             int
	scanErr, terminalErr error
}

func (r *closeErrorRows) Next() bool { r.next++; return r.next <= r.rowCount }
func (r *closeErrorRows) Close()     { r.closes++; r.closed = true }
func (r *closeErrorRows) Err() error {
	if r.closed {
		return r.terminalErr
	}
	return nil
}
func (r *closeErrorRows) Scan(dest ...any) error {
	if r.scanErr != nil {
		return r.scanErr
	}
	return r.mockScanRows.Scan(dest...)
}

func TestQueryOneCloseErrorOwnership(t *testing.T) {
	late, scan := errors.New("late wire error"), errors.New("scan failure")
	for _, tc := range []struct {
		name           string
		count          int
		scan, terminal error
		want           bool
	}{
		{"success keeps first of several", 2, nil, nil, false},
		{"close discovers failure", 1, nil, late, true},
		{"scan and close failures", 1, scan, late, true},
		{"empty then close failure", 0, nil, late, true},
		{"empty is not found", 0, nil, nil, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			rows := &closeErrorRows{mockScanRows: mockScanRows{cols: []string{"value"}, vals: []*string{strPtr("first")}}, rowCount: tc.count, scanErr: tc.scan, terminalErr: tc.terminal}
			model := &SQLModel{pool: &mockCDCQuerier{queryFn: func(context.Context, string, ...any) (pgx.Rows, error) { return rows, nil }}}
			type result struct {
				Value string `db:"value"`
			}
			got, err := QueryOne[result](context.Background(), model, "SELECT value")
			if (err != nil) != tc.want || rows.closes != 1 {
				t.Fatalf("got=%+v error=%v closes=%d", got, err, rows.closes)
			}
			if tc.count == 0 && tc.terminal != nil {
				var app *neutron.AppError
				if errors.As(err, &app) {
					t.Fatal("terminal failure mislabeled not found")
				}
			}
			if tc.terminal != nil && !errors.Is(err, tc.terminal) {
				t.Fatal("terminal error lost")
			}
			if tc.scan != nil && !errors.Is(err, tc.scan) {
				t.Fatal("scan error lost")
			}
			if tc.want && got.Value != "" {
				t.Fatal("partial row returned with failure")
			}
			if !tc.want && (got.Value != "first" || rows.next != 1) {
				t.Fatal("first-row semantics changed")
			}
			if tc.count == 0 && tc.terminal == nil {
				var app *neutron.AppError
				if !errors.As(err, &app) || app.Status != 404 {
					t.Fatalf("not-found error=%v", err)
				}
			}
		})
	}
}

func TestPoolConfigurationRefusalIsSecretSafe(t *testing.T) {
	const secret = "FAKE_ONLY_SECRET"
	for _, dsn := range []string{
		"postgres://localhost/db?password=" + secret + "&pool_max_conns=0",
		"host=localhost password = " + secret + " pool_max_conns=0",
		"host=localhost password='FAKE_ONLY_SECRET' pool_health_check_period=invalid",
		"postgres://%zz:" + secret + "@localhost/db",
		"host='x\\",
		"host=localhost pool_health_check_period=0s",
		"host=localhost pool_health_check_period=-1s",
		"host=localhost pool_min_conns=-1",
		"host=localhost pool_min_idle_conns=-1",
		"host=localhost pool_max_conns=1 pool_min_conns=2",
	} {
		_, err := preparePoolConfig(dsn, nil, time.Second)
		if !errors.Is(err, ErrInvalidConfig) || strings.Contains(fmt.Sprintf("%+v", err), secret) {
			t.Fatalf("unsafe error: %v", err)
		}
	}
}

func TestProgrammaticPoolValidationCopiesConfigAndCallbacks(t *testing.T) {
	cfg, err := pgxpool.ParseConfig("postgres://localhost/db?sslmode=disable")
	if err != nil {
		t.Fatal(err)
	}
	for _, mutate := range []func(*pgxpool.Config){
		func(c *pgxpool.Config) { c.HealthCheckPeriod = 0 },
		func(c *pgxpool.Config) { c.MinConns = -1 },
		func(c *pgxpool.Config) { c.MinConns = c.MaxConns + 1 },
		func(c *pgxpool.Config) { c.MaxConnLifetimeJitter = -1 },
	} {
		bad := cfg.Copy()
		mutate(bad)
		if _, err := preparePoolConfig("", bad, time.Second); !errors.Is(err, ErrInvalidConfig) {
			t.Fatalf("invalid config: %v", err)
		}
	}
	called := false
	cfg.BeforeConnect = func(ctx context.Context, c *pgx.ConnConfig) error {
		called = true
		if _, ok := ctx.Deadline(); !ok {
			t.Fatal("callback lacks deadline")
		}
		c.RuntimeParams["channel_binding"] = "require"
		return nil
	}
	prepared, err := preparePoolConfig("", cfg, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.ConnConfig.ConnectTimeout != 0 {
		t.Fatal("caller config mutated")
	}
	cc := prepared.ConnConfig.Copy()
	if err := prepared.BeforeConnect(context.Background(), cc); !errors.Is(err, ErrChannelBindingUnsupported) || !called {
		t.Fatalf("callback enforcement: %v", err)
	}
	if _, present := cfg.ConnConfig.RuntimeParams["channel_binding"]; present {
		t.Fatal("runtime parameter map aliased")
	}
}

func TestRequiredBindingRefusedBeforeDial(t *testing.T) {
	for _, ssl := range []string{"disable", "prefer", "require", "verify-full"} {
		cfg, err := pgxpool.ParseConfig("postgres://localhost/db?sslmode=" + ssl + "&channel_binding=require")
		if err != nil {
			t.Fatal(err)
		}
		dialed := false
		cfg.ConnConfig.DialFunc = func(context.Context, string, string) (net.Conn, error) {
			dialed = true
			return nil, errors.New("unexpected dial")
		}
		_, err = Connect(context.Background(), "", WithPoolConfig(cfg))
		if !errors.Is(err, ErrChannelBindingUnsupported) || dialed {
			t.Fatalf("ssl=%s err=%v dialed=%v", ssl, err, dialed)
		}
	}
}

// A local fake PostgreSQL peer drives the actual pgx connection and row
// reader. It does not stand in for Nucleus compatibility acceptance.
func fakePostgres(t *testing.T, serve func(net.Conn, *pgproto3.Backend)) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan struct{})
	connections := make(chan net.Conn, 1)
	go func() {
		defer close(done)
		conn, err := ln.Accept()
		if err != nil {
			return
		}
		connections <- conn
		defer conn.Close()
		_ = conn.SetDeadline(time.Now().Add(2 * time.Second))
		serve(conn, pgproto3.NewBackend(conn, conn))
	}()
	t.Cleanup(func() {
		_ = ln.Close()
		select {
		case conn := <-connections:
			_ = conn.Close()
		default:
		}
		select {
		case <-done:
		case <-time.After(3 * time.Second):
			t.Error("fake peer did not stop")
		}
	})
	return "postgres://test@" + ln.Addr().String() + "/db?sslmode=disable"
}
func fakeStartup(b *pgproto3.Backend) error {
	if _, err := b.ReceiveStartupMessage(); err != nil {
		return err
	}
	b.Send(&pgproto3.AuthenticationOk{})
	b.Send(&pgproto3.ParameterStatus{Name: "client_encoding", Value: "UTF8"})
	b.Send(&pgproto3.ParameterStatus{Name: "standard_conforming_strings", Value: "on"})
	b.Send(&pgproto3.ReadyForQuery{TxStatus: 'I'})
	return b.Flush()
}
func TestQueryOneActualDriverLateError(t *testing.T) {
	dsn := fakePostgres(t, func(_ net.Conn, b *pgproto3.Backend) {
		if fakeStartup(b) != nil {
			return
		}
		msg, err := b.Receive()
		if err != nil {
			return
		}
		if _, ok := msg.(*pgproto3.Query); !ok {
			return
		}
		b.Send(&pgproto3.RowDescription{Fields: []pgproto3.FieldDescription{{Name: []byte("value"), DataTypeOID: 25, DataTypeSize: -1, TypeModifier: -1}}})
		b.Send(&pgproto3.DataRow{Values: [][]byte{[]byte("first")}})
		b.Send(&pgproto3.ErrorResponse{Severity: "ERROR", Code: "57014", Message: "late fixture failure"})
		b.Send(&pgproto3.ReadyForQuery{TxStatus: 'I'})
		_ = b.Flush()
		_, _ = b.Receive()
	})
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	type row struct {
		Value string `db:"value"`
	}
	got, err := QueryOne[row](ctx, &SQLModel{pool: pool}, "SELECT value")
	var pgErr *pgconn.PgError
	if !errors.As(err, &pgErr) || pgErr.Code != "57014" || got.Value != "" {
		t.Fatalf("row=%+v err=%v", got, err)
	}
}
func TestConnectBudgetIncludesStartupAndFeatureProbe(t *testing.T) {
	for _, startup := range []bool{false, true} {
		t.Run(fmt.Sprintf("startup_complete=%v", startup), func(t *testing.T) {
			dsn := fakePostgres(t, func(conn net.Conn, b *pgproto3.Backend) {
				if startup {
					if fakeStartup(b) != nil {
						return
					}
					_, _ = b.Receive()
				} else {
					_, _ = b.ReceiveStartupMessage()
				}
				// Wait on socket closure, without sending the missing reply.
				var buf [1]byte
				_, _ = conn.Read(buf[:])
			})
			before := time.Now()
			client, err := Connect(context.Background(), dsn, WithConnectTimeout(50*time.Millisecond))
			if client != nil {
				client.Close()
				t.Fatal("stalled peer accepted")
			}
			if err == nil || time.Since(before) > time.Second {
				t.Fatalf("budget not respected: %v elapsed=%v", err, time.Since(before))
			}
		})
	}
}
func TestCloseContextHonorsBorrowedConnection(t *testing.T) {
	dsn := fakePostgres(t, func(_ net.Conn, b *pgproto3.Backend) {
		if fakeStartup(b) != nil {
			return
		}
		_, _ = b.Receive()
	})
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	conn, err := pool.Acquire(ctx)
	if err != nil {
		pool.Close()
		t.Fatal(err)
	}
	defer conn.Release()
	c := &Client{pool: pool}
	expired, stop := context.WithCancel(context.Background())
	stop()
	if err := c.LifecycleHook().OnStop(expired); !errors.Is(err, context.Canceled) {
		t.Fatalf("borrowed pool shutdown=%v", err)
	}
	done := c.closeDone
	for i := 0; i < 3; i++ {
		if c.closeDone != done {
			t.Fatal("repeated close created another shutdown owner")
		}
		if err := c.CloseContext(expired); !errors.Is(err, context.Canceled) {
			t.Fatal(err)
		}
	}
	conn.Release()
	if err := c.CloseContext(ctx); err != nil {
		t.Fatal(err)
	}
}

func TestUnparsedPoolConfigReturnsError(t *testing.T) {
	cc, err := pgx.ParseConfig("postgres://localhost/db?sslmode=disable")
	if err != nil {
		t.Fatal(err)
	}
	cfg := &pgxpool.Config{ConnConfig: cc, MaxConns: 1, HealthCheckPeriod: time.Second, MaxConnLifetime: time.Minute, MaxConnIdleTime: time.Minute}
	_, err = Connect(context.Background(), "", WithPoolConfig(cfg))
	if !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("unparsed config: %v", err)
	}
}

func TestConnectionCallbackBudgetIsCooperative(t *testing.T) {
	cfg, err := pgxpool.ParseConfig("postgres://localhost/db?sslmode=disable")
	if err != nil {
		t.Fatal(err)
	}
	cfg.BeforeConnect = func(ctx context.Context, _ *pgx.ConnConfig) error { <-ctx.Done(); return nil }
	prepared, err := preparePoolConfig("", cfg, time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	err = prepared.BeforeConnect(context.Background(), prepared.ConnConfig.Copy())
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("expired callback was accepted: %v", err)
	}
}
