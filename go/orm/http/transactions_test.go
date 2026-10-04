package ormhttp

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/neutron-build/neutron/go/orm"
)

func TestResponseBoundsSnapshotBeforeCommit(t *testing.T) {
	body := []byte("ok")
	header := http.Header{"X-Value": []string{"one"}}
	response, err := snapshotResponse(Response{Body: body, Header: header}, 2)
	if err != nil {
		t.Fatal(err)
	}
	body[0] = 'x'
	header["X-Value"][0] = "changed"
	if string(response.Body) != "ok" || response.Header.Get("X-Value") != "one" || response.Status != 200 {
		t.Fatal("mutable response snapshot")
	}
	for _, response := range []Response{{Body: []byte("long")}, {Status: 199}, {Status: 204, Body: []byte("x")}, {Header: http.Header{"Bad\rKey": []string{"x"}}}, {Header: http.Header{"X-Value": []string{"secret\nheader"}}}} {
		if _, err := snapshotResponse(response, 2); !errors.Is(err, ErrResponse) {
			t.Fatal("response refusal", err)
		}
	}
}
func TestRequestShutdownStopsAdmissionCancelsAndRetainsBudget(t *testing.T) {
	lifetime, err := NewTransactions(&pgxpool.Pool{}, Options{MaxResponseBytes: 8})
	if err != nil {
		t.Fatal(err)
	}
	request, release, ok := lifetime.acquire(context.Background())
	if !ok {
		t.Fatal("admission")
	}
	budget, cancel := context.WithTimeout(context.Background(), time.Millisecond)
	defer cancel()
	if err := lifetime.Shutdown(budget); !errors.Is(err, context.DeadlineExceeded) || !errors.Is(request.Err(), context.Canceled) {
		t.Fatal("bounded noncooperative shutdown", err)
	}
	if _, _, ok := lifetime.acquire(context.Background()); ok {
		t.Fatal("new request admitted after shutdown")
	}
	release()
	if err := lifetime.Shutdown(context.Background()); err != nil {
		t.Fatal("settled shutdown retry", err)
	}
	if err := lifetime.Shutdown(context.Background()); err != nil {
		t.Fatal("idempotent shutdown", err)
	}
}

// fakeTransaction substitutes the owned transaction runner. It reproduces only
// the lifecycle contract the adapter relies on: a callback error or panic rolls
// back and returns a TransactionError, success commits. Native atomicity is
// established by the PostgreSQL tests, not here.
type fakeTransaction struct {
	committed, rolledBack int
	outcome               orm.CommitOutcome
	extra                 error
	dispatchErr           error
	session               *orm.WriteSession
	afterCallback         func()
}

func (f *fakeTransaction) run(_ context.Context, callback func(*orm.Scope, *orm.WriteSession) error) error {
	completed := false
	defer func() {
		if !completed {
			f.rolledBack++
		}
	}()
	err := callback(nil, f.session)
	completed = true
	if f.afterCallback != nil {
		f.afterCallback()
	}
	if err != nil {
		f.rolledBack++
		outcome := f.outcome
		if outcome == "" {
			outcome = orm.CommitNotAttempted
		}
		return &orm.TransactionError{Outcome: outcome, Cause: errors.Join(err, f.extra)}
	}
	f.committed++
	return f.dispatchErr
}

func fakeHandler(t *testing.T, fake *fakeTransaction, callback Handler) (*Transactions, http.Handler) {
	t.Helper()
	lifetime, err := NewTransactions(&pgxpool.Pool{}, Options{MaxResponseBytes: 16})
	if err != nil {
		t.Fatal(err)
	}
	lifetime.transact = fake.run
	handler, err := lifetime.Handler(callback)
	if err != nil {
		t.Fatal(err)
	}
	return lifetime, handler
}

func serve(handler http.Handler) *httptest.ResponseRecorder {
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/", nil))
	return recorder
}

const genericFailure = "database request did not complete\n"

func TestApplicationErrorRollsBackAndAnswersExactBoundedResponse(t *testing.T) {
	refusal := Response{Status: http.StatusConflict, Header: http.Header{"Content-Type": []string{"application/json"}}, Body: []byte(`{"code":409}`)}
	cases := map[string]error{
		"direct":  &ApplicationError{Response: refusal},
		"wrapped": fmt.Errorf("step failed: %w", &ApplicationError{Response: refusal}),
		"joined":  errors.Join(errors.New("internal_secret_canary"), &ApplicationError{Response: refusal}),
	}
	for name, returned := range cases {
		t.Run(name, func(t *testing.T) {
			fake := &fakeTransaction{}
			_, handler := fakeHandler(t, fake, func(context.Context, RequestSession, *http.Request) (Response, error) {
				return Response{Status: http.StatusOK, Body: []byte("ignored")}, returned
			})
			recorder := serve(handler)
			if recorder.Code != http.StatusConflict || recorder.Body.String() != `{"code":409}` || recorder.Header().Get("Content-Type") != "application/json" {
				t.Fatal("application response not delivered exactly", recorder.Code, recorder.Body.String())
			}
			if strings.Contains(recorder.Body.String(), "canary") || fake.rolledBack != 1 || fake.committed != 0 {
				t.Fatal("rollback or leak", fake.rolledBack, fake.committed)
			}
		})
	}
}

func TestApplicationErrorSnapshotsResponseBeforeRollback(t *testing.T) {
	body := []byte("conflict")
	// Mutation after the callback returned but during rollback cannot alter the answer.
	fake := &fakeTransaction{afterCallback: func() { body[0] = 'X' }}
	_, handler := fakeHandler(t, fake, func(context.Context, RequestSession, *http.Request) (Response, error) {
		return Response{}, &ApplicationError{Response: Response{Status: 409, Body: body}}
	})
	if recorder := serve(handler); recorder.Body.String() != "conflict" {
		t.Fatal("application response aliased caller memory", recorder.Body.String())
	}
}

func TestInvalidApplicationResponsesFailClosedToGeneric503(t *testing.T) {
	cases := map[string]Response{
		"oversize body": {Status: 422, Body: []byte(strings.Repeat("x", 17))},
		"zero status":   {Body: []byte("x")},
		"success":       {Status: 200},
		"redirect":      {Status: 302},
		"server error":  {Status: 500},
		"below range":   {Status: 399},
		"above range":   {Status: 600},
		"bad header":    {Status: 400, Header: http.Header{"X-Value": []string{"a\nb"}}},
		"bad key":       {Status: 400, Header: http.Header{"Bad Key": []string{"a"}}},
	}
	for name, response := range cases {
		t.Run(name, func(t *testing.T) {
			fake := &fakeTransaction{}
			_, handler := fakeHandler(t, fake, func(context.Context, RequestSession, *http.Request) (Response, error) {
				return Response{}, &ApplicationError{Response: response}
			})
			recorder := serve(handler)
			if recorder.Code != http.StatusServiceUnavailable || recorder.Body.String() != genericFailure || fake.rolledBack != 1 || fake.committed != 0 {
				t.Fatal("invalid application response not refused", recorder.Code, recorder.Body.String())
			}
		})
	}
}

func TestNilApplicationErrorIsAnOrdinaryFailure(t *testing.T) {
	fake := &fakeTransaction{}
	_, handler := fakeHandler(t, fake, func(context.Context, RequestSession, *http.Request) (Response, error) {
		var application *ApplicationError
		return Response{}, application
	})
	if recorder := serve(handler); recorder.Code != http.StatusServiceUnavailable || recorder.Body.String() != genericFailure || fake.rolledBack != 1 {
		t.Fatal("typed nil application error", recorder.Code)
	}
}

func TestOrdinaryErrorStaysGeneric503WithoutLeak(t *testing.T) {
	fake := &fakeTransaction{}
	_, handler := fakeHandler(t, fake, func(context.Context, RequestSession, *http.Request) (Response, error) {
		return Response{Status: 200, Body: []byte("success")}, errors.New("internal_secret_canary")
	})
	recorder := serve(handler)
	if recorder.Code != http.StatusServiceUnavailable || recorder.Body.String() != genericFailure || fake.rolledBack != 1 || fake.committed != 0 {
		t.Fatal("ordinary error mapping", recorder.Code, recorder.Body.String())
	}
}

func TestApplicationResponseRefusedWithoutCleanRollback(t *testing.T) {
	refusal := &ApplicationError{Response: Response{Status: 409, Body: []byte("conflict")}}
	cases := map[string]*fakeTransaction{
		"leak":        {extra: orm.ErrScopeLeak},
		"hook leak":   {extra: orm.ErrHookLeak},
		"broken":      {extra: orm.ErrTransactionBroken},
		"ambiguous":   {extra: orm.ErrCommitAmbiguous},
		"not rolled":  {outcome: orm.CommitUnknown},
		"rejected":    {outcome: orm.CommitRejected},
		"wrapped bad": {extra: fmt.Errorf("cleanup: %w", orm.ErrTransactionBroken)},
	}
	for name, fake := range cases {
		t.Run(name, func(t *testing.T) {
			_, handler := fakeHandler(t, fake, func(context.Context, RequestSession, *http.Request) (Response, error) { return Response{}, refusal })
			if recorder := serve(handler); recorder.Code != http.StatusServiceUnavailable || recorder.Body.String() != genericFailure {
				t.Fatal("application response sent after unclean rollback", recorder.Code, recorder.Body.String())
			}
		})
	}
	// A hook workflow failure the application translated into its own 4xx is
	// still a clean rollback and the application's decision.
	fake := &fakeTransaction{extra: orm.ErrHookWorkflow}
	_, handler := fakeHandler(t, fake, func(context.Context, RequestSession, *http.Request) (Response, error) { return Response{}, refusal })
	if recorder := serve(handler); recorder.Code != http.StatusConflict {
		t.Fatal("clean rollback with failed hook workflow refused", recorder.Code)
	}
}

func TestApplicationResponseRefusedAfterRequestCancellation(t *testing.T) {
	fake := &fakeTransaction{}
	request, cancel := context.WithCancel(context.Background())
	defer cancel()
	_, handler := fakeHandler(t, fake, func(context.Context, RequestSession, *http.Request) (Response, error) {
		cancel()
		return Response{}, &ApplicationError{Response: Response{Status: 409}}
	})
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/", nil).WithContext(request))
	if recorder.Code != http.StatusServiceUnavailable || recorder.Body.String() != genericFailure {
		t.Fatal("canceled request answered with application response", recorder.Code)
	}
}

func TestCommittedResponsesAndWriteSessionPassThrough(t *testing.T) {
	session := &orm.WriteSession{}
	cases := []struct {
		response Response
		status   int
	}{{Response{Body: []byte("ok")}, 200}, {Response{Status: 404, Body: []byte("none")}, 404}}
	for _, c := range cases {
		fake := &fakeTransaction{session: session}
		_, handler := fakeHandler(t, fake, func(_ context.Context, request RequestSession, _ *http.Request) (Response, error) {
			if request.WriteSession != session {
				t.Fatal("write session not supplied")
			}
			return c.response, nil
		})
		if recorder := serve(handler); recorder.Code != c.status || recorder.Body.String() != string(c.response.Body) || fake.committed != 1 || fake.rolledBack != 0 {
			t.Fatal("committed response", recorder.Code)
		}
	}
	fake := &fakeTransaction{}
	_, handler := fakeHandler(t, fake, func(context.Context, RequestSession, *http.Request) (Response, error) {
		return Response{Body: []byte(strings.Repeat("x", 17))}, nil
	})
	if recorder := serve(handler); recorder.Code != http.StatusServiceUnavailable || fake.rolledBack != 1 || fake.committed != 0 {
		t.Fatal("oversize committed response admitted", recorder.Code)
	}
}

func TestCommittedDispatchFailureIsDistinctAndGeneric(t *testing.T) {
	fake := &fakeTransaction{dispatchErr: &orm.CommittedDispatchError{Cause: errors.New("internal_secret_canary")}}
	_, handler := fakeHandler(t, fake, func(context.Context, RequestSession, *http.Request) (Response, error) {
		return Response{Body: []byte("ok")}, nil
	})
	recorder := serve(handler)
	if recorder.Code != http.StatusInternalServerError || strings.Contains(recorder.Body.String(), "canary") || strings.Contains(recorder.Body.String(), "ok") || fake.committed != 1 {
		t.Fatal("committed dispatch failure mapping", recorder.Code, recorder.Body.String())
	}
}

func TestCallbackPanicPropagatesAfterRollbackAndRelease(t *testing.T) {
	fake := &fakeTransaction{}
	lifetime, handler := fakeHandler(t, fake, func(context.Context, RequestSession, *http.Request) (Response, error) {
		panic("callback panic")
	})
	var caught any
	func() {
		defer func() { caught = recover() }()
		serve(handler)
	}()
	if caught != "callback panic" || fake.rolledBack != 1 || fake.committed != 0 {
		t.Fatal("panic behavior changed", caught, fake.rolledBack)
	}
	budget, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := lifetime.Shutdown(budget); err != nil {
		t.Fatal("panicking request leaked its admission", err)
	}
}

func TestNegativeHookDispatchTimeoutRefused(t *testing.T) {
	if _, err := NewTransactions(&pgxpool.Pool{}, Options{MaxResponseBytes: 8, HookDispatchTimeout: -time.Second}); err == nil {
		t.Fatal("negative dispatch timeout admitted")
	}
}
