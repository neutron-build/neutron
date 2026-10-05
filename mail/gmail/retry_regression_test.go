package gmail

import (
	"context"
	"errors"
	"fmt"
	"github.com/neutron-build/neutron/mail"
	"io"
	"log/slog"
	"net/http"
	"strconv"
	"strings"
	"testing"
	"time"

	"google.golang.org/api/option"
)

type retryTransportFunc func(*http.Request) (*http.Response, error)

func (f retryTransportFunc) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }
func retryResponse(status int, body string) *http.Response {
	return &http.Response{StatusCode: status, Header: http.Header{"Content-Type": []string{"application/json"}}, Body: io.NopCloser(strings.NewReader(body))}
}

// No listener, mailbox or token is used. A single metadata throttle used to
// discard the entire page and force the next run to start the listing again.
func TestInitialPageRecoversOneMetadataThrottle(t *testing.T) {
	gets := map[string]int{}
	lists := 0
	tr := retryTransportFunc(func(r *http.Request) (*http.Response, error) {
		switch r.URL.Path {
		case "/gmail/v1/users/me/profile":
			return retryResponse(200, `{"historyId":"42"}`), nil
		case "/gmail/v1/users/me/messages":
			lists++
			return retryResponse(200, `{"messages":[{"id":"a"},{"id":"b"}],"nextPageToken":"next"}`), nil
		default:
			id := r.URL.Path[strings.LastIndex(r.URL.Path, "/")+1:]
			gets[id]++
			if id == "b" && gets[id] == 1 {
				return retryResponse(429, `{"error":{"code":429,"message":"try later"}}`), nil
			}
			return retryResponse(200, fmt.Sprintf(`{"id":%q,"labelIds":["INBOX"]}`, id)), nil
		}
	})
	ad, err := New(context.Background(), option.WithHTTPClient(&http.Client{Transport: tr}))
	if err != nil {
		t.Fatal(err)
	}
	fakeReadClock(ad)
	page, err := ad.Sync(context.Background(), "INBOX", "")
	if err != nil || page == nil {
		t.Fatalf("metadata throttle discarded page: page=%v err=%v", page, err)
	}
	if lists != 1 || gets["a"] != 1 || gets["b"] != 2 || len(page.Changes) != 2 || !page.More {
		t.Fatalf("page=%+v lists=%d gets=%v", page, lists, gets)
	}
}

func fakeReadClock(ad *Adapter) (*time.Time, *[]time.Duration) {
	now := time.Now()
	waits := []time.Duration{}
	ad.reads.now = func() time.Time { return now }
	ad.reads.wait = func(ctx context.Context, d time.Duration) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		if d < 0 {
			d = 0
		}
		if deadline, ok := ctx.Deadline(); ok && now.Add(d).After(deadline) {
			now = deadline
			return context.DeadlineExceeded
		}
		now = now.Add(d)
		waits = append(waits, d)
		return nil
	}
	ad.retryJitter = func() time.Duration { return 0 }
	return &now, &waits
}

func testReadAdapter(t *testing.T, tr retryTransportFunc) *Adapter {
	t.Helper()
	ad, err := New(context.Background(), option.WithHTTPClient(&http.Client{Transport: tr}))
	if err != nil {
		t.Fatal(err)
	}
	fakeReadClock(ad)
	return ad
}

func TestRepeatedThrottleNeverReturnsIncompletePage(t *testing.T) {
	gets := map[string]int{}
	lists := 0
	ad := testReadAdapter(t, func(r *http.Request) (*http.Response, error) {
		switch r.URL.Path {
		case "/gmail/v1/users/me/profile":
			return retryResponse(200, `{"historyId":"42"}`), nil
		case "/gmail/v1/users/me/messages":
			lists++
			return retryResponse(200, `{"messages":[{"id":"a"},{"id":"b"}],"nextPageToken":"next"}`), nil
		default:
			id := r.URL.Path[strings.LastIndex(r.URL.Path, "/")+1:]
			gets[id]++
			if id == "b" {
				return retryResponse(429, `{"error":{"code":429}}`), nil
			}
			return retryResponse(200, fmt.Sprintf(`{"id":%q}`, id)), nil
		}
	})
	for range 2 {
		page, err := ad.Sync(context.Background(), "INBOX", "")
		if page != nil || !errors.Is(err, mail.ErrRateLimited) {
			t.Fatalf("incomplete page escaped: %+v / %v", page, err)
		}
	}
	if lists != 2 || gets["a"] != 2 || gets["b"] != 6 {
		t.Fatalf("unbounded/restarted read retries: lists=%d gets=%v", lists, gets)
	}
}

func TestPacedInitialPageFitsThirtySecondBudget(t *testing.T) {
	requests := 0
	ad := testReadAdapter(t, func(r *http.Request) (*http.Response, error) {
		requests++
		switch r.URL.Path {
		case "/gmail/v1/users/me/profile":
			return retryResponse(200, `{"historyId":"42"}`), nil
		case "/gmail/v1/users/me/messages":
			n, err := strconv.Atoi(r.URL.Query().Get("maxResults"))
			if err != nil || n != 100 {
				t.Fatalf("page bound=%q, want 100", r.URL.Query().Get("maxResults"))
			}
			var ids []string
			for i := 0; i < n; i++ {
				ids = append(ids, fmt.Sprintf(`{"id":"m%d"}`, i))
			}
			return retryResponse(200, `{"messages":[`+strings.Join(ids, ",")+`],"nextPageToken":"next"}`), nil
		default:
			id := r.URL.Path[strings.LastIndex(r.URL.Path, "/")+1:]
			return retryResponse(200, fmt.Sprintf(`{"id":%q,"labelIds":["INBOX"]}`, id)), nil
		}
	})
	now, _ := fakeReadClock(ad)
	start := *now
	ctx, cancel := context.WithDeadline(context.Background(), start.Add(30*time.Second))
	defer cancel()
	page, err := ad.Sync(ctx, "INBOX", "")
	if err != nil || page == nil || len(page.Changes) != 100 || !page.More {
		t.Fatalf("bounded page failed budget: %+v %v", page, err)
	}
	if elapsed := now.Sub(start); elapsed != 101*readSpacing {
		t.Fatalf("virtual duration=%s requests=%d", elapsed, requests)
	}
}

func TestReadRetryWaitHonorsCancellationAndDeadline(t *testing.T) {
	for _, kind := range []string{"cancel", "deadline"} {
		t.Run(kind, func(t *testing.T) {
			calls := 0
			ad := testReadAdapter(t, func(r *http.Request) (*http.Response, error) {
				calls++
				return retryResponse(429, `{"error":{"code":429}}`), nil
			})
			now, _ := fakeReadClock(ad)
			ctx, cancel := context.WithDeadline(context.Background(), now.Add(500*time.Millisecond))
			defer cancel()
			if kind == "cancel" {
				oldWait := ad.reads.wait
				ad.reads.wait = func(ctx context.Context, d time.Duration) error {
					if d >= time.Second {
						cancel()
					}
					return oldWait(ctx, d)
				}
			}
			_, err := ad.Envelopes(ctx, []mail.MessageID{"n:gmail:a"})
			want := context.DeadlineExceeded
			if kind == "cancel" {
				want = context.Canceled
			}
			if !errors.Is(err, want) || calls != 1 {
				t.Fatalf("calls=%d error=%v want=%v", calls, err, want)
			}
		})
	}
}

func TestRetryAfterIsRespectedOrReturnedWithoutEarlyRetry(t *testing.T) {
	for _, header := range []string{"7", "120"} {
		t.Run(header, func(t *testing.T) {
			calls := 0
			ad := testReadAdapter(t, func(r *http.Request) (*http.Response, error) {
				calls++
				if calls > 1 {
					return retryResponse(200, `{"id":"a"}`), nil
				}
				res := retryResponse(429, `{"error":{"code":429}}`)
				res.Header.Set("Retry-After", header)
				return res, nil
			})
			now, _ := fakeReadClock(ad)
			start := *now
			_, err := ad.Envelopes(context.Background(), []mail.MessageID{"n:gmail:a"})
			if header == "7" {
				if err != nil || calls != 2 || now.Sub(start) != 7*time.Second {
					t.Fatalf("calls=%d err=%v elapsed=%s", calls, err, now.Sub(start))
				}
			} else if !errors.Is(err, mail.ErrRateLimited) || calls != 1 {
				t.Fatalf("long Retry-After shortened: calls=%d err=%v", calls, err)
			}
		})
	}
}

func TestReadDoesNotRetryForbiddenAndMutationDoesNotRetryThrottle(t *testing.T) {
	calls := 0
	ad := testReadAdapter(t, func(r *http.Request) (*http.Response, error) {
		calls++
		if r.Method == http.MethodGet {
			return retryResponse(403, `{"error":{"code":403}}`), nil
		}
		return retryResponse(429, `{"error":{"code":429}}`), nil
	})
	if _, err := ad.Envelopes(context.Background(), []mail.MessageID{"n:gmail:a"}); !errors.Is(err, mail.ErrReauthRequired) || calls != 1 {
		t.Fatalf("read calls=%d err=%v", calls, err)
	}
	if err := ad.Apply(context.Background(), mail.Operation{Kind: mail.OpAddKeyword, IDs: []mail.MessageID{"n:gmail:a"}, Keyword: "seen"}); !errors.Is(err, mail.ErrRateLimited) || calls != 2 {
		t.Fatalf("mutation calls=%d err=%v", calls, err)
	}
}

func TestRepeatedLabelEnumerationRefetchesChangedMetadata(t *testing.T) {
	label := ""
	gets := 0
	ad := testReadAdapter(t, func(r *http.Request) (*http.Response, error) {
		switch r.URL.Path {
		case "/gmail/v1/users/me/profile":
			return retryResponse(200, `{"historyId":"42"}`), nil
		case "/gmail/v1/users/me/messages":
			label = r.URL.Query().Get("labelIds")
			return retryResponse(200, `{"messages":[{"id":"a"}]}`), nil
		default:
			gets++
			if label == "INBOX" {
				return retryResponse(200, `{"id":"a","labelIds":["INBOX","UNREAD"]}`), nil
			}
			return retryResponse(200, `{"id":"a","labelIds":["INBOX","Label_1","STARRED"]}`), nil
		}
	})
	first, err := ad.Sync(context.Background(), "INBOX", "")
	if err != nil {
		t.Fatal(err)
	}
	second, err := ad.Sync(context.Background(), "Label_1", "")
	if err != nil {
		t.Fatal(err)
	}
	env := second.Changes[0].Envelope
	if gets != 2 || first.Changes[0].Envelope.Keywords.Seen || !env.Keywords.Seen || !env.Keywords.Flagged || len(env.MailboxIDs) != 3 {
		t.Fatalf("stale repeated-label metadata: gets=%d envelope=%+v", gets, env)
	}
}

// This fake records the staged engine contract, not PostgreSQL transaction
// behavior. No provider page is admitted until every metadata read succeeds.
type retryScanStore struct {
	mail.Store
	mail.ScanStore
	scan     *mail.Scan
	staged   map[mail.MessageID]mail.Envelope
	seen     map[mail.MessageID]bool
	cursor   mail.Cursor
	applied  int
	finished bool
}

func (s *retryScanStore) RunningScan(context.Context, mail.AccountID, mail.MailboxID) (*mail.Scan, error) {
	if s.scan == nil {
		return nil, nil
	}
	c := *s.scan
	return &c, nil
}
func (s *retryScanStore) Cursor(context.Context, mail.AccountID, mail.MailboxID) (mail.Cursor, error) {
	return s.cursor, nil
}
func (s *retryScanStore) ApplyScanPage(_ context.Context, id mail.ScanID, upsert []mail.Envelope, seen, destroy []mail.MessageID, next mail.Cursor) error {
	if s.scan == nil || s.scan.ID != id {
		return fmt.Errorf("retired scan")
	}
	s.applied++
	for _, env := range upsert {
		s.staged[env.ID] = env
	}
	for _, id := range seen {
		s.seen[id] = true
	}
	for _, id := range destroy {
		delete(s.staged, id)
		delete(s.seen, id)
	}
	s.scan.Continuation = next
	return nil
}
func (s *retryScanStore) ApplyFinalScanPage(ctx context.Context, id mail.ScanID, upsert []mail.Envelope, seen, destroy []mail.MessageID, next mail.Cursor) (int, error) {
	if err := s.ApplyScanPage(ctx, id, upsert, seen, destroy, next); err != nil {
		return 0, err
	}
	s.finished = true
	s.cursor = next
	s.scan = nil
	return 0, nil
}

func TestPacedGmailScanPreservesContinuationAndSeenOnThrottle(t *testing.T) {
	throttled := false
	var listed []string
	ad := testReadAdapter(t, func(r *http.Request) (*http.Response, error) {
		switch r.URL.Path {
		case "/gmail/v1/users/me/profile":
			return retryResponse(200, `{"historyId":"42"}`), nil
		case "/gmail/v1/users/me/messages":
			page := r.URL.Query().Get("pageToken")
			listed = append(listed, page)
			if page == "" {
				return retryResponse(200, `{"messages":[{"id":"a"}],"nextPageToken":"page2"}`), nil
			}
			if page != "page2" {
				t.Fatalf("unexpected continuation %q", page)
			}
			return retryResponse(200, `{"messages":[{"id":"b"}]}`), nil
		default:
			id := r.URL.Path[strings.LastIndex(r.URL.Path, "/")+1:]
			if id == "b" && throttled {
				return retryResponse(429, `{"error":{"code":429}}`), nil
			}
			return retryResponse(200, fmt.Sprintf(`{"id":%q,"labelIds":["INBOX","Label_1"]}`, id)), nil
		}
	})
	store := &retryScanStore{scan: &mail.Scan{ID: "generation-7", Generation: 7}, staged: map[mail.MessageID]mail.Envelope{}, seen: map[mail.MessageID]bool{}}
	eng := mail.NewEngine(store, slog.New(slog.NewTextHandler(io.Discard, nil)))
	eng.MaxPages = 1
	rep, err := eng.SyncMailbox(context.Background(), "synthetic", "INBOX", ad)
	if err != nil || rep.Pages != 1 || store.applied != 1 || !store.seen["n:gmail:a"] || store.scan.Generation != 7 {
		t.Fatalf("first staged page: rep=%+v err=%v store=%+v", rep, err, store)
	}
	checkpoint := store.scan.Continuation
	throttled = true
	if _, err = eng.SyncMailbox(context.Background(), "synthetic", "INBOX", ad); !errors.Is(err, mail.ErrRateLimited) {
		t.Fatalf("throttle error=%v", err)
	}
	if store.applied != 1 || store.scan.Continuation != checkpoint || store.scan.Generation != 7 || store.finished || len(store.seen) != 1 {
		t.Fatalf("incomplete page advanced scan: %+v", store)
	}
	throttled = false
	if _, err = eng.SyncMailbox(context.Background(), "synthetic", "INBOX", ad); err != nil {
		t.Fatal(err)
	}
	if !store.finished || store.cursor != "42" || len(store.seen) != 2 || store.applied != 2 || strings.Join(listed, ",") != ",page2,page2" {
		t.Fatalf("resume lost progress: listed=%v store=%+v", listed, store)
	}
}

func TestRetryAfterCooldownPersistsAcrossAdaptersAndRuns(t *testing.T) {
	calls := 0
	first := testReadAdapter(t, func(r *http.Request) (*http.Response, error) {
		calls++
		res := retryResponse(429, `{"error":{"code":429}}`)
		res.Header.Set("Retry-After", "120")
		return res, nil
	})
	now, _ := fakeReadClock(first)
	start := *now
	_, err := first.Envelopes(context.Background(), []mail.MessageID{"n:gmail:a"})
	if !errors.Is(err, mail.ErrRateLimited) || calls != 1 {
		t.Fatalf("first calls=%d err=%v", calls, err)
	}
	second := testReadAdapter(t, func(r *http.Request) (*http.Response, error) { calls++; return retryResponse(200, `{"id":"a"}`), nil })
	second.reads = first.reads
	*now = start.Add(60 * time.Second)
	_, err = second.Envelopes(context.Background(), []mail.MessageID{"n:gmail:a"})
	if !errors.Is(err, mail.ErrRateLimited) || calls != 1 || !now.Equal(start.Add(60*time.Second)) {
		t.Fatalf("later run ignored provider cooldown: calls=%d err=%v elapsed=%s", calls, err, now.Sub(start))
	}
	*now = start.Add(119 * time.Second)
	_, err = second.Envelopes(context.Background(), []mail.MessageID{"n:gmail:a"})
	if err != nil || calls != 2 || !now.Equal(start.Add(120*time.Second)) {
		t.Fatalf("cooldown recovery: calls=%d err=%v elapsed=%s", calls, err, now.Sub(start))
	}
}

func TestReadPacingRechecksNewConcurrentCooldown(t *testing.T) {
	calls := 0
	ad := testReadAdapter(t, func(r *http.Request) (*http.Response, error) { calls++; return retryResponse(200, `{"id":"a"}`), nil })
	now, _ := fakeReadClock(ad)
	start := *now
	if _, err := ad.Envelopes(context.Background(), []mail.MessageID{"n:gmail:a"}); err != nil {
		t.Fatal(err)
	}
	oldWait := ad.reads.wait
	published := false
	ad.reads.wait = func(ctx context.Context, d time.Duration) error {
		err := oldWait(ctx, d)
		if !published {
			published = true
			ad.reads.pause(7 * time.Second)
		}
		return err
	}
	if _, err := ad.Envelopes(context.Background(), []mail.MessageID{"n:gmail:a"}); err != nil {
		t.Fatal(err)
	}
	if calls != 2 || now.Sub(start) != 7*time.Second+readSpacing {
		t.Fatalf("new cooldown ignored: calls=%d elapsed=%s", calls, now.Sub(start))
	}
}

func TestReadLimiterContentionHonorsCancellation(t *testing.T) {
	limiter := NewReadLimiter()
	limiter.gate <- struct{}{}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := limiter.Wait(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("queued waiter=%v", err)
	}
	<-limiter.gate
}

func TestRetryAfterParsesLongAndOverflowDelaysConservatively(t *testing.T) {
	now := time.Unix(1_700_000_000, 0)
	for _, tc := range []struct {
		raw   string
		min   time.Duration
		valid bool
	}{
		{"120", 120 * time.Second, true}, {"999999999999999999999999999", 365 * 24 * time.Hour, true},
		{now.Add(2 * time.Minute).UTC().Format(http.TimeFormat), 2 * time.Minute, true}, {"-5", 0, false}, {"invalid", 0, false},
	} {
		delay, valid := readRetryAfter(tc.raw, now)
		if valid != tc.valid || valid && delay < tc.min {
			t.Fatalf("Retry-After=%q delay=%s valid=%v", tc.raw, delay, valid)
		}
	}
}
