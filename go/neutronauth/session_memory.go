package neutronauth

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"sync"
	"time"
)

// MemorySessionStore is a bounded, revision-fenced single-process store.
// Use SQLSessionStore for shared sessions across processes/restarts.
type MemorySessionStore struct {
	mu       sync.Mutex
	records  map[string]SessionRecord
	capacity int
}

func NewMemorySessionStore(capacity ...int) *MemorySessionStore {
	n := 10000
	if len(capacity) > 0 && capacity[0] > 0 {
		n = capacity[0]
	}
	return &MemorySessionStore{records: make(map[string]SessionRecord), capacity: n}
}
func cloneSessionData(data map[string]any) (map[string]any, error) {
	if _, err := json.Marshal(data); err != nil {
		return nil, err
	}
	value, err := cloneSessionValue(reflect.ValueOf(data))
	if err != nil {
		return nil, err
	}
	return value.Interface().(map[string]any), nil
}
func cloneSessionValue(v reflect.Value) (reflect.Value, error) {
	switch v.Kind() {
	case reflect.Interface, reflect.Pointer:
		if v.IsNil() {
			return reflect.Zero(v.Type()), nil
		}
		inner, err := cloneSessionValue(v.Elem())
		if err != nil {
			return reflect.Value{}, err
		}
		if v.Kind() == reflect.Interface {
			out := reflect.New(v.Type()).Elem()
			out.Set(inner)
			return out, nil
		}
		out := reflect.New(v.Type().Elem())
		out.Elem().Set(inner)
		return out, nil
	case reflect.Map:
		if v.IsNil() {
			return reflect.Zero(v.Type()), nil
		}
		if v.Type().Key().Kind() != reflect.String {
			return reflect.Value{}, fmt.Errorf("neutronauth: session maps require string keys")
		}
		out := reflect.MakeMap(v.Type())
		iter := v.MapRange()
		for iter.Next() {
			item, err := cloneSessionValue(iter.Value())
			if err != nil {
				return reflect.Value{}, err
			}
			out.SetMapIndex(iter.Key(), item)
		}
		return out, nil
	case reflect.Slice, reflect.Array:
		if v.Kind() == reflect.Slice && v.IsNil() {
			return reflect.Zero(v.Type()), nil
		}
		var out reflect.Value
		if v.Kind() == reflect.Slice {
			out = reflect.MakeSlice(v.Type(), v.Len(), v.Len())
		} else {
			out = reflect.New(v.Type()).Elem()
		}
		for i := 0; i < v.Len(); i++ {
			item, err := cloneSessionValue(v.Index(i))
			if err != nil {
				return reflect.Value{}, err
			}
			out.Index(i).Set(item)
		}
		return out, nil
	case reflect.Bool, reflect.String, reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64, reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Float32, reflect.Float64:
		return v, nil
	case reflect.Struct:
		if v.Type() == reflect.TypeFor[time.Time]() {
			return v, nil
		}
	}
	return reflect.Value{}, fmt.Errorf("neutronauth: unsupported session data type %s", v.Type())
}

func (m *MemorySessionStore) load(id string) *SessionRecord {
	r, ok := m.records[id]
	if !ok {
		return nil
	}
	if !r.ExpiresAt.IsZero() && !r.ExpiresAt.After(time.Now()) {
		delete(m.records, id)
		return nil
	}
	return &r
}
func (m *MemorySessionStore) LoadSession(_ context.Context, id string) (*SessionRecord, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	r := m.load(id)
	if r == nil {
		return nil, nil
	}
	data, err := cloneSessionData(r.Data)
	if err != nil {
		return nil, err
	}
	r.Data = data
	return r, nil
}
func (m *MemorySessionStore) Get(ctx context.Context, id string) (map[string]any, error) {
	r, e := m.LoadSession(ctx, id)
	if r == nil {
		return nil, e
	}
	return r.Data, e
}

// Set is an administrative unconditional replacement. Middleware uses CommitSession.
func (m *MemorySessionStore) Set(_ context.Context, id string, data map[string]any, ttl time.Duration) error {
	clone, err := cloneSessionData(data)
	if err != nil {
		return err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.put(id, clone, ttl)
	return nil
}
func (m *MemorySessionStore) Delete(_ context.Context, id string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.records, id)
	return nil
}
func (m *MemorySessionStore) put(id string, data map[string]any, ttl time.Duration) string {
	version := generateSessionID()
	expiry := time.Time{}
	if ttl > 0 {
		expiry = time.Now().Add(ttl)
	}
	if _, ok := m.records[id]; !ok && len(m.records) >= m.capacity {
		for k := range m.records {
			delete(m.records, k)
			break
		}
	}
	m.records[id] = SessionRecord{Data: data, Version: version, ExpiresAt: expiry}
	return version
}
func (m *MemorySessionStore) CommitSession(_ context.Context, id, expected string, next *SessionReplacement, ttl time.Duration) (string, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	current := m.load(id)
	version := ""
	if current != nil {
		version = current.Version
	}
	if version != expected {
		return "", ErrSessionConflict
	}
	if next == nil {
		delete(m.records, id)
		return "", nil
	}
	if next.ID != id && m.load(next.ID) != nil {
		return "", ErrSessionConflict
	}
	data, err := cloneSessionData(next.Data)
	if err != nil {
		return "", err
	}
	delete(m.records, id)
	return m.put(next.ID, data, ttl), nil
}
