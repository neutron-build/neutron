package graph

// Identity-stability machinery for the consumer's identity contract
// (docs/identity-stability.md in the lullmail repo, §3 and Phase 4): a
// verified mailbox key and a batched Exchange-ID translator, plus the
// opt-in immutable-ID preference that makes the translator necessary.
//
// These are additional methods on Adapter, deliberately NOT part of
// mail.Adapter: they are Graph-only capabilities a host probes for with a
// type assertion — the same optional-capability shape as the engine's
// IdentityReplaceStore (mail/hooks.go). The other three providers have no
// equivalent, and the interface must keep compiling for adapters that
// never grow one.

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
)

// GraphMailboxKey returns the raw /me id of the mailbox this adapter is
// authenticated to — the verified mailbox key of the identity contract.
// An email-derived key is not a mailbox identity (the address can be
// reassigned), so binding verification asks the provider, authenticated,
// to name the mailbox itself. A missing or empty id is an error: callers
// refuse to bind on anything less.
func (a *Adapter) GraphMailboxKey(ctx context.Context) (string, error) {
	var out struct {
		ID string `json:"id"`
	}
	if err := a.get(ctx, "/me?$select=id", &out); err != nil {
		return "", err
	}
	if out.ID == "" {
		return "", errors.New("graph: /me carried no mailbox id")
	}
	return out.ID, nil
}

// translateBatchLimit is Microsoft's documented ceiling for one
// translateExchangeIds request: the inputIds collection has a "Maximum
// size of this collection is 1,000 strings". Batching here keeps every
// request this package issues bounded, the same discipline as
// providerJSONLimit and the 100-page pagination caps.
const translateBatchLimit = 1000

// TranslateExchangeIds converts mutable Graph REST ids (restId) into
// immutable ones (restImmutableEntryId) for the mailbox this adapter is
// authenticated to, POSTing /me/translateExchangeIds in batches of at most
// translateBatchLimit. The caller wraps each batch with its own /me
// verification (GraphMailboxKey) — the adapter provides the translation;
// the migration protocol lives in the consumer.
//
// Every id comes back or the call fails: a conversion Microsoft reports
// as failed (a convertIdResult carrying errorDetails instead of a
// targetId) surfaces as an error naming that id, as does an id the
// response omits entirely. A silently dropped translation would strand
// every retained product reference to it, which is the exact loss this
// machinery exists to prevent. All ids must belong to one mailbox
// (Microsoft requires it) — one adapter is bound to one mailbox.
func (a *Adapter) TranslateExchangeIds(ctx context.Context, ids []string) (map[string]string, error) {
	out := make(map[string]string, len(ids))
	if len(ids) == 0 {
		return out, nil
	}
	for start := 0; start < len(ids); start += translateBatchLimit {
		end := min(start+translateBatchLimit, len(ids))
		if err := a.translateExchangeIdsBatch(ctx, ids[start:end], out); err != nil {
			return nil, err
		}
	}
	return out, nil
}

func (a *Adapter) translateExchangeIdsBatch(ctx context.Context, batch []string, out map[string]string) error {
	raw, err := json.Marshal(struct {
		InputIDs     []string `json:"inputIds"`
		SourceIDType string   `json:"sourceIdType"`
		TargetIDType string   `json:"targetIdType"`
	}{
		InputIDs:     batch,
		SourceIDType: "restId",
		TargetIDType: "restImmutableEntryId",
	})
	if err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost,
		baseURL+"/me/translateExchangeIds", strings.NewReader(string(raw)))
	if err != nil {
		return err
	}
	req.Header.Set("Accept", "application/json")
	req.Header.Set("Content-Type", "application/json")

	resp, err := a.http.Do(req)
	if err != nil {
		return fmt.Errorf("graph: request: %w", err)
	}
	defer resp.Body.Close()
	if err := statusError(resp); err != nil {
		return err
	}
	var page struct {
		Value []struct {
			SourceID string `json:"sourceId"`
			TargetID string `json:"targetId"`
			// ErrorDetails is present exactly when the conversion
			// failed (convertIdResult); any decodes to nil for both
			// an absent and a null property.
			ErrorDetails any `json:"errorDetails"`
		} `json:"value"`
	}
	if err := json.NewDecoder(io.LimitReader(resp.Body, providerJSONLimit)).Decode(&page); err != nil {
		return fmt.Errorf("graph: decode: %w", err)
	}

	translated := make(map[string]bool, len(batch))
	for _, entry := range page.Value {
		if entry.ErrorDetails != nil || entry.TargetID == "" {
			return fmt.Errorf("graph: translateExchangeIds could not translate id %q", entry.SourceID)
		}
		out[entry.SourceID] = entry.TargetID
		translated[entry.SourceID] = true
	}
	for _, id := range batch {
		if !translated[id] {
			return fmt.Errorf("graph: translateExchangeIds returned no translation for id %q", id)
		}
	}
	return nil
}
