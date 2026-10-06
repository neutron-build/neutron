package orm

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

// commitAckProxy is a tiny plaintext loopback wire fixture. It forwards actual
// PostgreSQL frames and drops exactly one COMMIT CommandComplete response after
// the server generated it. No production hooks or fabricated server states.
type commitAckProxy struct {
	listener    net.Listener
	target      string
	drop        atomic.Bool
	commits     atomic.Int32
	observed    chan struct{}
	mu          sync.Mutex
	connections map[net.Conn]bool
	closing     bool
	workers     sync.WaitGroup
}

func startCommitAckProxy(t *testing.T, target string) *commitAckProxy {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal("loopback proxy unavailable")
	}
	p := &commitAckProxy{listener: listener, target: target, observed: make(chan struct{}), connections: map[net.Conn]bool{}}
	p.drop.Store(true)
	p.workers.Add(1)
	go func() {
		defer p.workers.Done()
		for {
			client, err := listener.Accept()
			if err != nil {
				return
			}
			p.workers.Add(1)
			go p.handle(client)
		}
	}()
	t.Cleanup(p.Close)
	return p
}
func (p *commitAckProxy) Close() {
	_ = p.listener.Close()
	p.mu.Lock()
	p.closing = true
	for conn := range p.connections {
		_ = conn.Close()
	}
	p.mu.Unlock()
	p.workers.Wait()
}
func (p *commitAckProxy) handle(client net.Conn) {
	defer p.workers.Done()
	defer client.Close()
	p.mu.Lock()
	if p.closing {
		p.mu.Unlock()
		return
	}
	p.connections[client] = true
	p.mu.Unlock()
	defer func() { p.mu.Lock(); delete(p.connections, client); p.mu.Unlock() }()
	server, err := net.DialTimeout("tcp", p.target, 5*time.Second)
	if err != nil {
		return
	}
	defer server.Close()
	p.mu.Lock()
	if p.closing {
		p.mu.Unlock()
		return
	}
	p.connections[server] = true
	p.mu.Unlock()
	defer func() { p.mu.Lock(); delete(p.connections, client); delete(p.connections, server); p.mu.Unlock() }()
	startup := make([]byte, 4)
	if _, err := io.ReadFull(client, startup); err != nil {
		return
	}
	length := int(binary.BigEndian.Uint32(startup))
	if length < 8 || length > 16*1024*1024 {
		return
	}
	body := make([]byte, length-4)
	if _, err := io.ReadFull(client, body); err != nil {
		return
	}
	if err := writeFrame(server, append(startup, body...)); err != nil {
		return
	}
	var committing atomic.Bool
	frontDone := make(chan struct{})
	go func() {
		defer close(frontDone)
		defer server.Close()
		for {
			kind, frame, err := readWireFrame(client)
			if err != nil {
				return
			}
			if kind == 'Q' {
				sql := strings.ToLower(strings.TrimSpace(strings.TrimRight(string(frame[5:]), "\x00;")))
				if sql == "commit" {
					committing.Store(true)
					p.commits.Add(1)
				}
			}
			if err := writeFrame(server, frame); err != nil {
				return
			}
		}
	}()
	for {
		kind, frame, err := readWireFrame(server)
		if err != nil {
			break
		}
		if committing.Load() && kind == 'C' && string(frame[5:]) == "COMMIT\x00" && p.drop.CompareAndSwap(true, false) {
			close(p.observed)
			_ = client.Close()
			break
		}
		if err := writeFrame(client, frame); err != nil {
			break
		}
	}
	_ = client.Close()
	_ = server.Close()
	<-frontDone
}
func readWireFrame(reader io.Reader) (byte, []byte, error) {
	header := make([]byte, 5)
	if _, err := io.ReadFull(reader, header); err != nil {
		return 0, nil, err
	}
	length := int(binary.BigEndian.Uint32(header[1:]))
	if length < 4 || length > 16*1024*1024 {
		return 0, nil, errors.New("invalid fixture frame length")
	}
	body := make([]byte, length-4)
	if _, err := io.ReadFull(reader, body); err != nil {
		return 0, nil, err
	}
	return header[0], append(header, body...), nil
}
func writeFrame(writer io.Writer, frame []byte) error {
	for len(frame) > 0 {
		n, err := writer.Write(frame)
		if err != nil {
			return err
		}
		if n == 0 {
			return io.ErrShortWrite
		}
		frame = frame[n:]
	}
	return nil
}

func TestPostgresLostCommitAcknowledgment(t *testing.T) {
	ctx, _, admin, table := liveTransactionSetup(t)
	config, err := pgxpool.ParseConfig(os.Getenv("NEUTRON_ORM_TEST_DATABASE_URL"))
	if err != nil {
		t.Fatal("invalid test config")
	}
	if strings.HasPrefix(config.ConnConfig.Host, "/") {
		t.Fatal("lost-ack fixture requires TCP PostgreSQL test profile")
	}
	proxy := startCommitAckProxy(t, net.JoinHostPort(config.ConnConfig.Host, strconv.Itoa(int(config.ConnConfig.Port))))
	config.ConnConfig.Host = "127.0.0.1"
	config.ConnConfig.Port = uint16(proxy.listener.Addr().(*net.TCPAddr).Port)
	config.ConnConfig.TLSConfig = nil
	config.ConnConfig.Fallbacks = nil
	config.MaxConns = 1
	config.MinConns = 0
	pool, err := pgxpool.NewWithConfig(ctx, config)
	if err != nil {
		t.Fatal("proxy pool creation failed")
	}
	t.Cleanup(pool.Close)
	err = WithTransaction(ctx, pool, TransactionOptions{}, func(s *Scope) error {
		_, err := s.Exec(ctx, "INSERT INTO "+table+" VALUES (1,'durable despite lost ack')")
		return err
	})
	var txErr *TransactionError
	if !errors.As(err, &txErr) || txErr.Outcome != CommitUnknown || !errors.Is(err, ErrCommitAmbiguous) {
		t.Fatal("lost ack not classified indeterminate", err)
	}
	select {
	case <-proxy.observed:
	default:
		t.Fatal("fixture never observed actual COMMIT server completion")
	}
	ids, err := nativeIDs(ctx, admin, table)
	if err != nil || len(ids) != 1 || ids[0] != 1 {
		t.Fatal("independent durable row missing", ids, err)
	}
	if proxy.commits.Load() != 1 {
		t.Fatal("COMMIT automatically replayed", proxy.commits.Load())
	}
	requirePoolReuse(t, ctx, pool)
}

func TestPostgresDeferredCommitRejection(t *testing.T) {
	ctx, pool, admin, table := liveTransactionSetup(t)
	deferred := strings.TrimSuffix(table, ".records") + ".deferred_rows"
	if _, err := admin.Exec(ctx, "CREATE TABLE "+deferred+" (id bigint UNIQUE DEFERRABLE INITIALLY DEFERRED)"); err != nil {
		t.Fatal(err)
	}
	err := WithTransaction(ctx, pool, TransactionOptions{}, func(s *Scope) error { _, err := s.Exec(ctx, "INSERT INTO "+deferred+" VALUES (1),(1)"); return err })
	var txErr *TransactionError
	var state *pgconn.PgError
	if !errors.As(err, &txErr) || txErr.Outcome != CommitRejected || !errors.As(err, &state) || state.Code != "23505" {
		t.Fatal("deferred rejection misclassified", err)
	}
	var count int
	if err := admin.QueryRow(ctx, "SELECT count(*) FROM "+deferred).Scan(&count); err != nil || count != 0 {
		t.Fatal("rejected COMMIT persisted", count, err)
	}
	requirePoolReuse(t, ctx, pool)
}

func TestPostgresSerializationCommitRejection(t *testing.T) {
	ctx, pool, admin, table := liveTransactionSetup(t)
	accounts := strings.TrimSuffix(table, ".records") + ".serial_accounts"
	if _, err := admin.Exec(ctx, "CREATE TABLE "+accounts+" (id bigint PRIMARY KEY,balance integer NOT NULL);INSERT INTO "+accounts+" VALUES (1,1),(2,1)"); err != nil {
		t.Fatal(err)
	}
	other, err := admin.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.Serializable})
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		cleanup, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = other.Rollback(cleanup)
	}()
	var sum int64
	if err := other.QueryRow(ctx, "SELECT sum(balance) FROM "+accounts).Scan(&sum); err != nil || sum != 2 {
		t.Fatal("native serial snapshot", err)
	}
	err = WithTransaction(ctx, pool, TransactionOptions{Isolation: pgx.Serializable}, func(s *Scope) error {
		if value, err := scopeSingleInt(ctx, s, "SELECT sum(balance) FROM "+accounts); err != nil || value != 2 {
			return fmt.Errorf("owned serial snapshot: %d %w", value, err)
		}
		if _, err := s.Exec(ctx, "UPDATE "+accounts+" SET balance=0 WHERE id=1"); err != nil {
			return err
		}
		if _, err := other.Exec(ctx, "UPDATE "+accounts+" SET balance=0 WHERE id=2"); err != nil {
			return err
		}
		return other.Commit(ctx)
	})
	var txErr *TransactionError
	var state *pgconn.PgError
	if !errors.As(err, &txErr) || txErr.Outcome != CommitRejected || !errors.As(err, &state) || state.Code != "40001" {
		t.Fatal("serialization COMMIT rejection not proved", err)
	}
	var first, second int
	if err := admin.QueryRow(ctx, "SELECT balance FROM "+accounts+" WHERE id=1").Scan(&first); err != nil {
		t.Fatal(err)
	}
	if err := admin.QueryRow(ctx, "SELECT balance FROM "+accounts+" WHERE id=2").Scan(&second); err != nil {
		t.Fatal(err)
	}
	if first != 1 || second != 0 {
		t.Fatal("native serialization atomicity", first, second)
	}
	requirePoolReuse(t, ctx, pool)
}
