package db

import (
	"context"
	"encoding/binary"
	"errors"
	"io"
	"net"
	"net/url"
	"os"
	"os/exec"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// commitDropProxy forwards native PostgreSQL messages and drops the actual
// server's COMMIT CommandComplete before the worker receives its acknowledgement.
// It changes no production executor path and never logs connection credentials.
func commitDropProxy(t *testing.T, target string) (string, *atomic.Bool) {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	armed := &atomic.Bool{}
	var mu sync.Mutex
	connections := map[net.Conn]bool{}
	track := func(c net.Conn) { mu.Lock(); connections[c] = true; mu.Unlock() }
	t.Cleanup(func() {
		_ = listener.Close()
		mu.Lock()
		defer mu.Unlock()
		for c := range connections {
			_ = c.Close()
		}
	})
	go func() {
		for {
			client, err := listener.Accept()
			if err != nil {
				return
			}
			track(client)
			go func() {
				defer client.Close()
				server, err := net.DialTimeout("tcp", target, 5*time.Second)
				if err != nil {
					return
				}
				track(server)
				defer server.Close()
				go func() { _, _ = io.Copy(server, client); _ = server.Close() }()
				for {
					header := make([]byte, 5)
					if _, err := io.ReadFull(server, header); err != nil {
						return
					}
					length := binary.BigEndian.Uint32(header[1:])
					if length < 4 || length > 64<<20 {
						return
					}
					body := make([]byte, int(length)-4)
					if _, err := io.ReadFull(server, body); err != nil {
						return
					}
					if header[0] == 'C' && string(body) == "COMMIT\x00" && armed.CompareAndSwap(true, false) {
						return
					}
					if _, err := client.Write(append(header, body...)); err != nil {
						return
					}
				}
			}()
		}
	}()
	return listener.Addr().String(), armed
}
func TestBackfillNativeLostCommitAcknowledgement(t *testing.T) {
	h, job := newBackfillHarness(t)
	h.exec(`INSERT INTO public.source VALUES(1,'committed',NULL)`)
	cfg := h.client.pool.Config().ConnConfig
	address, armed := commitDropProxy(t, net.JoinHostPort(cfg.Host, strconv.Itoa(int(cfg.Port))))
	u, err := url.Parse(h.client.url)
	if err != nil {
		t.Fatal("fixture URL invalid")
	}
	u.Host = address
	q := u.Query()
	q.Set("sslmode", "disable")
	u.RawQuery = q.Encode()
	proxied, err := Connect(context.Background(), u.String())
	if err != nil {
		t.Fatal("fault proxy connection failed")
	}
	defer proxied.Close()
	armed.Store(true)
	result, err := RunBackfillChunk(context.Background(), proxied, job)
	var failure *BackfillError
	if result.Status != "indeterminate" || !errors.As(err, &failure) || !failure.Indeterminate {
		t.Fatalf("lost COMMIT acknowledgement did not remain indeterminate: %+v %v", result, err)
	}
	// The independent native connection proves the commit actually happened.
	if h.queryOne(`SELECT dst FROM public.source WHERE id=1`) != "committed" || h.queryOne(`SELECT updated_rows::text FROM public.progress`) != "1" {
		t.Fatal("lost acknowledgement fixture did not commit data and progress")
	}
	resumed, err := RunBackfillChunk(context.Background(), h.client, job)
	if err != nil || resumed.Status != "idle" || resumed.TotalUpdatedRows != 1 {
		t.Fatalf("fresh reconciliation replayed work: %+v %v", resumed, err)
	}
}

func TestBackfillWorkerProcess(t *testing.T) {
	endpoint := os.Getenv("NEUTRON_BACKFILL_CRASH_CHILD_URL")
	if endpoint == "" {
		return
	}
	client, err := Connect(context.Background(), endpoint)
	if err != nil {
		t.Fatal("child connection failed")
	}
	defer client.Close()
	spec := backfillFixtureSpec()
	spec.TimeoutMilliseconds = 60000
	job, err := InspectBackfillJob(context.Background(), client, spec)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := RunBackfillChunk(context.Background(), client, job); err != nil {
		t.Fatal(err)
	}
}
func TestBackfillNativeKilledWorkerRollsBackAndResumes(t *testing.T) {
	h := newQ07Harness(t, "backfillkill")
	// A native CHECK pauses the actual UPDATE inside the executor transaction.
	// Admission refuses user triggers; no executor test-hook or fake transaction.
	h.exec(`CREATE TABLE public.source(id bigint PRIMARY KEY,src text,dst text CHECK(dst IS NULL OR pg_catalog.pg_sleep(5) IS NOT NULL))`)
	h.exec(`CREATE TABLE public.progress(job_id text PRIMARY KEY,job_digest text NOT NULL,format text NOT NULL,chunks bigint NOT NULL,updated_rows bigint NOT NULL)`)
	h.exec(`INSERT INTO public.source VALUES(1,'recover',NULL)`)
	spec := backfillFixtureSpec()
	spec.TimeoutMilliseconds = 60000
	job, err := InspectBackfillJob(context.Background(), h.client, spec)
	if err != nil {
		t.Fatal(err)
	}
	executable, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	u, err := url.Parse(h.client.url)
	if err != nil {
		t.Fatal("fixture URL invalid")
	}
	q := u.Query()
	name := "backfill_crash_" + strconv.FormatInt(time.Now().UnixNano(), 10)
	q.Set("application_name", name)
	u.RawQuery = q.Encode()
	child := exec.Command(executable, "-test.run=^TestBackfillWorkerProcess$", "-test.timeout=30s")
	child.Env = append(os.Environ(), "NEUTRON_BACKFILL_CRASH_CHILD_URL="+u.String())
	child.Stdout = io.Discard
	child.Stderr = io.Discard
	if err := child.Start(); err != nil {
		t.Fatal(err)
	}
	waited := false
	defer func() {
		if !waited {
			_ = child.Process.Kill()
			_ = child.Wait()
		}
	}()
	deadline := time.Now().Add(10 * time.Second)
	paused := false
	for time.Now().Before(deadline) {
		var count int
		err := h.client.QueryRow(context.Background(), `SELECT pg_catalog.count(*) FROM pg_catalog.pg_stat_activity WHERE application_name=$1 AND wait_event='PgSleep'`, name).Scan(&count)
		if err != nil {
			t.Fatal(err)
		}
		if count > 0 {
			paused = true
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	if !paused {
		t.Fatal("actual backfill UPDATE did not reach crash window")
	}
	if err := child.Process.Kill(); err != nil {
		t.Fatal(err)
	}
	_ = child.Wait()
	waited = true
	// PostgreSQL may finish the sleeping statement before observing closed TCP;
	// wait for that backend to disappear before testing recovery on fresh session.
	deadline = time.Now().Add(10 * time.Second)
	closed := false
	for time.Now().Before(deadline) {
		var count int
		if err := h.client.QueryRow(context.Background(), `SELECT pg_catalog.count(*) FROM pg_catalog.pg_stat_activity WHERE application_name=$1`, name).Scan(&count); err != nil {
			t.Fatal(err)
		}
		if count == 0 {
			closed = true
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	if !closed {
		t.Fatal("killed worker backend did not release transaction")
	}
	if h.queryOne(`SELECT count(*)::text FROM public.source WHERE dst IS NOT NULL`) != "0" || h.queryOne(`SELECT count(*)::text FROM public.progress`) != "0" {
		t.Fatal("killed transaction left partial data/progress")
	}
	result, err := RunBackfillChunk(context.Background(), h.client, job)
	if err != nil || result.UpdatedRows != 1 {
		t.Fatalf("fresh worker resume %+v %v", result, err)
	}
	if h.queryOne(`SELECT dst FROM public.source WHERE id=1`) != "recover" || h.queryOne(`SELECT updated_rows::text FROM public.progress`) != "1" {
		t.Fatal("resume did not reconcile native data/progress")
	}
}
