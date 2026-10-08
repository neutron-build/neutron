package graph

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strings"

	"github.com/neutron-build/neutron/mail"
)

// NewWithIdentityFormat is for a resolver that read the durable account policy.
// Never pass immutable mode based on credential presence or a global default.
func NewWithIdentityFormat(hc *http.Client, format string) (*Adapter, error) {
	if format != mail.GraphLegacyIDs && format != mail.GraphImmutableIDs {
		return nil, errors.New("graph: invalid identity format")
	}
	a := newAdapter(hc)
	clone := *a.http
	base := clone.Transport
	if base == nil {
		base = http.DefaultTransport
	}
	clone.Transport = identityTransport{base: base, format: format}
	previous := clone.CheckRedirect
	clone.CheckRedirect = func(req *http.Request, via []*http.Request) error {
		if len(via) >= 5 {
			return errors.New("graph: too many redirects")
		}
		if _, err := graphEndpoint(baseURL, req.URL.String()); err != nil {
			return err
		}
		if previous != nil {
			return previous(req, via)
		}
		return nil
	}
	a.http = &clone
	a.identityFormat = format
	return a, nil
}
func (a *Adapter) GraphIdentityFormat() string {
	if a.identityFormat == "" {
		return mail.GraphLegacyIDs
	}
	return a.identityFormat
}

// WithIdentityPreference restricts the authenticated client to the Graph API
// origin/path and merges Prefer. Do not use it for preauthenticated upload URLs.
func WithIdentityPreference(base http.RoundTripper, format string) (http.RoundTripper, error) {
	if format != mail.GraphLegacyIDs && format != mail.GraphImmutableIDs {
		return nil, errors.New("graph: invalid identity format")
	}
	if base == nil {
		base = http.DefaultTransport
	}
	return identityTransport{base: base, format: format}, nil
}

type identityTransport struct {
	base   http.RoundTripper
	format string
}

func (t identityTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	if _, err := graphEndpoint(baseURL, req.URL.String()); err != nil {
		return nil, err
	}
	copy := req.Clone(req.Context())
	copy.Header = req.Header.Clone()
	var preferences []string
	for _, line := range copy.Header.Values("Prefer") {
		for _, value := range strings.Split(line, ",") {
			if value = strings.TrimSpace(value); value != "" && !strings.HasPrefix(strings.ToLower(value), "idtype=") {
				preferences = append(preferences, value)
			}
		}
	}
	if t.format == mail.GraphImmutableIDs {
		preferences = append(preferences, `IdType="ImmutableId"`)
	}
	copy.Header.Del("Prefer")
	if len(preferences) > 0 {
		copy.Header.Set("Prefer", strings.Join(preferences, ", "))
	}
	return t.base.RoundTrip(copy)
}

func (a *Adapter) GraphMailboxKey(ctx context.Context) (string, error) {
	var result struct {
		ID string `json:"id"`
	}
	if err := a.get(ctx, "/me?$select=id", &result); err != nil {
		return "", err
	}
	if result.ID == "" {
		return "", errors.New("graph: missing authenticated mailbox ID")
	}
	return result.ID, nil
}
func (a *Adapter) TranslateGraphIDs(ctx context.Context, ids []mail.MessageID) ([]mail.IdentityPair, error) {
	if len(ids) < 1 || len(ids) > 1000 {
		return nil, errors.New("graph: translation batch must contain 1..1000 IDs")
	}
	inputs := make([]string, len(ids))
	wanted := map[string]mail.MessageID{}
	for i, id := range ids {
		if !strings.HasPrefix(string(id), "n:graph:") || nativeID(id) == "" {
			return nil, errors.New("graph: translation requires Graph native IDs")
		}
		native := nativeID(id)
		if _, exists := wanted[native]; exists {
			return nil, errors.New("graph: duplicate translation input")
		}
		inputs[i] = native
		wanted[native] = id
	}
	raw, err := json.Marshal(map[string]any{"inputIds": inputs, "sourceIdType": "restId", "targetIdType": "restImmutableEntryId"})
	if err != nil {
		return nil, err
	}
	endpoint, err := graphEndpoint(baseURL, "/me/translateExchangeIds")
	if err != nil {
		return nil, err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewReader(raw))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := a.http.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if err := statusError(resp); err != nil {
		return nil, err
	}
	var out struct {
		Value []struct {
			SourceID string          `json:"sourceId"`
			TargetID string          `json:"targetId"`
			Error    json.RawMessage `json:"errorDetails"`
		} `json:"value"`
	}
	if err := json.NewDecoder(io.LimitReader(resp.Body, providerJSONLimit)).Decode(&out); err != nil {
		return nil, err
	}
	if len(out.Value) != len(ids) {
		return nil, errors.New("graph: incomplete translation response")
	}
	targets := map[string]bool{}
	pairs := make([]mail.IdentityPair, 0, len(ids))
	for _, v := range out.Value {
		old, ok := wanted[v.SourceID]
		if !ok || v.TargetID == "" || targets[v.TargetID] || len(v.Error) > 0 && string(v.Error) != "null" {
			return nil, errors.New("graph: missing, duplicate or failed translation")
		}
		pairs = append(pairs, mail.IdentityPair{OldID: old, NewID: mail.NativeMessageID(mail.ProviderGraph, v.TargetID)})
		delete(wanted, v.SourceID)
		targets[v.TargetID] = true
	}
	return pairs, nil
}
