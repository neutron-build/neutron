package nucleus

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"
)

// KVModel provides Redis-compatible key-value operations over Nucleus SQL functions.
type KVModel struct {
	pool   querier
	client *Client
}

// KVEntry represents a key-value pair returned by Scan.
type KVEntry struct {
	Key   string
	Value []byte
}

// KVOption configures KV operations.
type KVOption func(*kvOpts)

type kvOpts struct {
	ttl       *time.Duration
	namespace string
}

// WithTTL sets a time-to-live on the key.
func WithTTL(d time.Duration) KVOption {
	return func(o *kvOpts) { o.ttl = &d }
}

// WithNamespace prefixes the key with a namespace.
func WithNamespace(ns string) KVOption {
	return func(o *kvOpts) { o.namespace = ns }
}

func (o *kvOpts) resolveKey(key string) string {
	if o.namespace != "" {
		return o.namespace + ":" + key
	}
	return key
}

func applyKVOpts(opts []KVOption) kvOpts {
	var o kvOpts
	for _, fn := range opts {
		fn(&o)
	}
	return o
}

// --- Base Operations ---

// Get retrieves a raw value by key. Returns nil if the key does not exist.
func (kv *KVModel) Get(ctx context.Context, key string) ([]byte, error) {
	if err := kv.client.requireNucleus("KV.Get"); err != nil {
		return nil, err
	}
	var val *string
	err := kv.pool.QueryRow(ctx, "SELECT KV_GET($1)", key).Scan(&val)
	if err != nil {
		return nil, fmt.Errorf("nucleus: kv get: %w", err)
	}
	if val == nil {
		return nil, nil
	}
	return []byte(*val), nil
}

// KVGetTyped retrieves a value and JSON-decodes it into T.
func KVGetTyped[T any](ctx context.Context, kv *KVModel, key string) (T, error) {
	var result T
	data, err := kv.Get(ctx, key)
	if err != nil {
		return result, err
	}
	if data == nil {
		return result, fmt.Errorf("nucleus: kv key %q not found", key)
	}
	if err := json.Unmarshal(data, &result); err != nil {
		return result, fmt.Errorf("nucleus: kv unmarshal: %w", err)
	}
	return result, nil
}

// Set stores a raw value. Supports WithTTL option.
func (kv *KVModel) Set(ctx context.Context, key string, value []byte, opts ...KVOption) error {
	if err := kv.client.requireNucleus("KV.Set"); err != nil {
		return err
	}
	o := applyKVOpts(opts)
	key = o.resolveKey(key)
	if o.ttl != nil {
		ttlSecs := int64(o.ttl.Seconds())
		_, err := kv.pool.Exec(ctx, "SELECT KV_SET($1, $2, $3)", key, string(value), ttlSecs)
		return wrapErr("kv set", err)
	}
	_, err := kv.pool.Exec(ctx, "SELECT KV_SET($1, $2)", key, string(value))
	return wrapErr("kv set", err)
}

// KVSetTyped JSON-encodes a value and stores it.
func KVSetTyped[T any](ctx context.Context, kv *KVModel, key string, value T, opts ...KVOption) error {
	data, err := json.Marshal(value)
	if err != nil {
		return fmt.Errorf("nucleus: kv marshal: %w", err)
	}
	return kv.Set(ctx, key, data, opts...)
}

// SetNX sets the key only if it does not already exist. Returns true if set.
func (kv *KVModel) SetNX(ctx context.Context, key string, value []byte) (bool, error) {
	if err := kv.client.requireNucleus("KV.SetNX"); err != nil {
		return false, err
	}
	var ok bool
	err := kv.pool.QueryRow(ctx, "SELECT KV_SETNX($1, $2)", key, string(value)).Scan(&ok)
	return ok, wrapErr("kv setnx", err)
}

// Delete removes a key. Returns true if the key existed.
func (kv *KVModel) Delete(ctx context.Context, key string) (bool, error) {
	if err := kv.client.requireNucleus("KV.Delete"); err != nil {
		return false, err
	}
	var ok bool
	err := kv.pool.QueryRow(ctx, "SELECT KV_DEL($1)", key).Scan(&ok)
	return ok, wrapErr("kv del", err)
}

// Exists checks whether a key exists.
func (kv *KVModel) Exists(ctx context.Context, key string) (bool, error) {
	if err := kv.client.requireNucleus("KV.Exists"); err != nil {
		return false, err
	}
	var ok bool
	err := kv.pool.QueryRow(ctx, "SELECT KV_EXISTS($1)", key).Scan(&ok)
	return ok, wrapErr("kv exists", err)
}

// Incr atomically increments a key's integer value and returns the new value.
func (kv *KVModel) Incr(ctx context.Context, key string, amount ...int64) (int64, error) {
	if err := kv.client.requireNucleus("KV.Incr"); err != nil {
		return 0, err
	}
	// Requires nucleus >= the #22/#23 pgwire fix (KV_INCR is described as
	// BIGINT, so the value scans natively).
	var val int64
	var err error
	if len(amount) > 0 {
		err = kv.pool.QueryRow(ctx, "SELECT KV_INCR($1, $2)", key, amount[0]).Scan(&val)
	} else {
		err = kv.pool.QueryRow(ctx, "SELECT KV_INCR($1)", key).Scan(&val)
	}
	return val, wrapErr("kv incr", err)
}

// TTL returns the remaining TTL in seconds. -1 means no TTL, -2 means key missing.
func (kv *KVModel) TTL(ctx context.Context, key string) (int64, error) {
	if err := kv.client.requireNucleus("KV.TTL"); err != nil {
		return 0, err
	}
	var val int64
	err := kv.pool.QueryRow(ctx, "SELECT KV_TTL($1)", key).Scan(&val)
	return val, wrapErr("kv ttl", err)
}

// Expire sets a TTL on an existing key.
func (kv *KVModel) Expire(ctx context.Context, key string, ttl time.Duration) (bool, error) {
	if err := kv.client.requireNucleus("KV.Expire"); err != nil {
		return false, err
	}
	var ok bool
	err := kv.pool.QueryRow(ctx, "SELECT KV_EXPIRE($1, $2)", key, int64(ttl.Seconds())).Scan(&ok)
	return ok, wrapErr("kv expire", err)
}

// DBSize returns the total number of keys.
func (kv *KVModel) DBSize(ctx context.Context) (int64, error) {
	if err := kv.client.requireNucleus("KV.DBSize"); err != nil {
		return 0, err
	}
	var n int64
	err := kv.pool.QueryRow(ctx, "SELECT KV_DBSIZE()").Scan(&n)
	return n, wrapErr("kv dbsize", err)
}

// FlushDB deletes all keys.
func (kv *KVModel) FlushDB(ctx context.Context) error {
	if err := kv.client.requireNucleus("KV.FlushDB"); err != nil {
		return err
	}
	_, err := kv.pool.Exec(ctx, "SELECT KV_FLUSHDB()")
	return wrapErr("kv flushdb", err)
}

// --- List Operations ---

// LPush prepends a value to a list. Returns the new list length.
func (kv *KVModel) LPush(ctx context.Context, key string, value string) (int64, error) {
	if err := kv.client.requireNucleus("KV.LPush"); err != nil {
		return 0, err
	}
	var n int64
	err := kv.pool.QueryRow(ctx, "SELECT KV_LPUSH($1, $2)", key, value).Scan(&n)
	return n, wrapErr("kv lpush", err)
}

// RPush appends a value to a list. Returns the new list length.
func (kv *KVModel) RPush(ctx context.Context, key string, value string) (int64, error) {
	if err := kv.client.requireNucleus("KV.RPush"); err != nil {
		return 0, err
	}
	var n int64
	err := kv.pool.QueryRow(ctx, "SELECT KV_RPUSH($1, $2)", key, value).Scan(&n)
	return n, wrapErr("kv rpush", err)
}

// LPop removes and returns the first element of a list.
func (kv *KVModel) LPop(ctx context.Context, key string) (*string, error) {
	if err := kv.client.requireNucleus("KV.LPop"); err != nil {
		return nil, err
	}
	var val *string
	err := kv.pool.QueryRow(ctx, "SELECT KV_LPOP($1)", key).Scan(&val)
	return val, wrapErr("kv lpop", err)
}

// RPop removes and returns the last element of a list.
func (kv *KVModel) RPop(ctx context.Context, key string) (*string, error) {
	if err := kv.client.requireNucleus("KV.RPop"); err != nil {
		return nil, err
	}
	var val *string
	err := kv.pool.QueryRow(ctx, "SELECT KV_RPOP($1)", key).Scan(&val)
	return val, wrapErr("kv rpop", err)
}

// LRange returns elements from a list between start and stop (inclusive).
func (kv *KVModel) LRange(ctx context.Context, key string, start, stop int64) ([]string, error) {
	if err := kv.client.requireNucleus("KV.LRange"); err != nil {
		return nil, err
	}
	var raw string
	err := kv.pool.QueryRow(ctx, "SELECT KV_LRANGE($1, $2, $3)", key, start, stop).Scan(&raw)
	if err != nil {
		return nil, wrapErr("kv lrange", err)
	}
	if raw == "" {
		return nil, nil
	}
	var out []string
	if err := json.Unmarshal([]byte(raw), &out); err != nil {
		return nil, wrapErr("kv lrange decode", err)
	}
	return out, nil
}

// LLen returns the length of a list.
func (kv *KVModel) LLen(ctx context.Context, key string) (int64, error) {
	if err := kv.client.requireNucleus("KV.LLen"); err != nil {
		return 0, err
	}
	var n int64
	err := kv.pool.QueryRow(ctx, "SELECT KV_LLEN($1)", key).Scan(&n)
	return n, wrapErr("kv llen", err)
}

// LIndex returns the element at the given index in a list.
func (kv *KVModel) LIndex(ctx context.Context, key string, index int64) (*string, error) {
	if err := kv.client.requireNucleus("KV.LIndex"); err != nil {
		return nil, err
	}
	var val *string
	err := kv.pool.QueryRow(ctx, "SELECT KV_LINDEX($1, $2)", key, index).Scan(&val)
	return val, wrapErr("kv lindex", err)
}

// --- Hash Operations ---

// HSet sets a field in a hash.
func (kv *KVModel) HSet(ctx context.Context, key, field string, value string) (bool, error) {
	if err := kv.client.requireNucleus("KV.HSet"); err != nil {
		return false, err
	}
	var ok bool
	err := kv.pool.QueryRow(ctx, "SELECT KV_HSET($1, $2, $3)", key, field, value).Scan(&ok)
	return ok, wrapErr("kv hset", err)
}

// HGet returns a field value from a hash.
func (kv *KVModel) HGet(ctx context.Context, key, field string) (*string, error) {
	if err := kv.client.requireNucleus("KV.HGet"); err != nil {
		return nil, err
	}
	var val *string
	err := kv.pool.QueryRow(ctx, "SELECT KV_HGET($1, $2)", key, field).Scan(&val)
	return val, wrapErr("kv hget", err)
}

// HDel removes a field from a hash.
func (kv *KVModel) HDel(ctx context.Context, key, field string) (bool, error) {
	if err := kv.client.requireNucleus("KV.HDel"); err != nil {
		return false, err
	}
	var ok bool
	err := kv.pool.QueryRow(ctx, "SELECT KV_HDEL($1, $2)", key, field).Scan(&ok)
	return ok, wrapErr("kv hdel", err)
}

// HExists checks if a field exists in a hash.
func (kv *KVModel) HExists(ctx context.Context, key, field string) (bool, error) {
	if err := kv.client.requireNucleus("KV.HExists"); err != nil {
		return false, err
	}
	var ok bool
	err := kv.pool.QueryRow(ctx, "SELECT KV_HEXISTS($1, $2)", key, field).Scan(&ok)
	return ok, wrapErr("kv hexists", err)
}

// HGetAll returns all fields and values from a hash as a map.
func (kv *KVModel) HGetAll(ctx context.Context, key string) (map[string]string, error) {
	if err := kv.client.requireNucleus("KV.HGetAll"); err != nil {
		return nil, err
	}
	var raw string
	err := kv.pool.QueryRow(ctx, "SELECT KV_HGETALL($1)", key).Scan(&raw)
	if err != nil {
		return nil, wrapErr("kv hgetall", err)
	}
	result := make(map[string]string)
	if raw == "" {
		return result, nil
	}
	var pairs [][]string
	if err := json.Unmarshal([]byte(raw), &pairs); err != nil {
		return nil, wrapErr("kv hgetall decode", err)
	}
	for _, pair := range pairs {
		if len(pair) == 2 {
			result[pair[0]] = pair[1]
		}
	}
	return result, nil
}

// HLen returns the number of fields in a hash.
func (kv *KVModel) HLen(ctx context.Context, key string) (int64, error) {
	if err := kv.client.requireNucleus("KV.HLen"); err != nil {
		return 0, err
	}
	var n int64
	err := kv.pool.QueryRow(ctx, "SELECT KV_HLEN($1)", key).Scan(&n)
	return n, wrapErr("kv hlen", err)
}

// --- Set Operations ---

// SAdd adds a member to a set.
func (kv *KVModel) SAdd(ctx context.Context, key, member string) (bool, error) {
	if err := kv.client.requireNucleus("KV.SAdd"); err != nil {
		return false, err
	}
	var ok bool
	err := kv.pool.QueryRow(ctx, "SELECT KV_SADD($1, $2)", key, member).Scan(&ok)
	return ok, wrapErr("kv sadd", err)
}

// SRem removes a member from a set.
func (kv *KVModel) SRem(ctx context.Context, key, member string) (bool, error) {
	if err := kv.client.requireNucleus("KV.SRem"); err != nil {
		return false, err
	}
	var ok bool
	err := kv.pool.QueryRow(ctx, "SELECT KV_SREM($1, $2)", key, member).Scan(&ok)
	return ok, wrapErr("kv srem", err)
}

// SMembers returns all members of a set.
// Keys returns every non-expired key matching a glob pattern, sorted.
//
// `*` is the only wildcard, so `Keys(ctx, "srcmap:site-1:*")` enumerates one
// prefix. Intended for administrative work — retention sweeps, operator
// tooling, migrations — not for request paths: the engine walks the whole
// keyspace and materialises every match, so cost scales with the store, not
// with the number of hits.
func (kv *KVModel) Keys(ctx context.Context, pattern string) ([]string, error) {
	if err := kv.client.requireNucleus("KV.Keys"); err != nil {
		return nil, err
	}
	var raw string
	err := kv.pool.QueryRow(ctx, "SELECT KV_KEYS($1)", pattern).Scan(&raw)
	if err != nil {
		return nil, wrapErr("kv keys", err)
	}
	if raw == "" {
		return nil, nil
	}
	var out []string
	if err := json.Unmarshal([]byte(raw), &out); err != nil {
		return nil, wrapErr("kv keys decode", err)
	}
	return out, nil
}

func (kv *KVModel) SMembers(ctx context.Context, key string) ([]string, error) {
	if err := kv.client.requireNucleus("KV.SMembers"); err != nil {
		return nil, err
	}
	var raw string
	err := kv.pool.QueryRow(ctx, "SELECT KV_SMEMBERS($1)", key).Scan(&raw)
	if err != nil {
		return nil, wrapErr("kv smembers", err)
	}
	if raw == "" {
		return nil, nil
	}
	var out []string
	if err := json.Unmarshal([]byte(raw), &out); err != nil {
		return nil, wrapErr("kv smembers decode", err)
	}
	return out, nil
}

// SIsMember checks if a member exists in a set.
func (kv *KVModel) SIsMember(ctx context.Context, key, member string) (bool, error) {
	if err := kv.client.requireNucleus("KV.SIsMember"); err != nil {
		return false, err
	}
	var ok bool
	err := kv.pool.QueryRow(ctx, "SELECT KV_SISMEMBER($1, $2)", key, member).Scan(&ok)
	return ok, wrapErr("kv sismember", err)
}

// SCard returns the number of members in a set.
func (kv *KVModel) SCard(ctx context.Context, key string) (int64, error) {
	if err := kv.client.requireNucleus("KV.SCard"); err != nil {
		return 0, err
	}
	var n int64
	err := kv.pool.QueryRow(ctx, "SELECT KV_SCARD($1)", key).Scan(&n)
	return n, wrapErr("kv scard", err)
}

// --- Sorted Set Operations ---

// ZAdd adds a member with a score to a sorted set.
func (kv *KVModel) ZAdd(ctx context.Context, key string, score float64, member string) (bool, error) {
	if err := kv.client.requireNucleus("KV.ZAdd"); err != nil {
		return false, err
	}
	var ok bool
	err := kv.pool.QueryRow(ctx, "SELECT KV_ZADD($1, $2, $3)", key, score, member).Scan(&ok)
	return ok, wrapErr("kv zadd", err)
}

// ZRange returns members in a sorted set between start and stop ranks.
func (kv *KVModel) ZRange(ctx context.Context, key string, start, stop int64) ([]string, error) {
	if err := kv.client.requireNucleus("KV.ZRange"); err != nil {
		return nil, err
	}
	var raw string
	err := kv.pool.QueryRow(ctx, "SELECT KV_ZRANGE($1, $2, $3)", key, start, stop).Scan(&raw)
	if err != nil {
		return nil, wrapErr("kv zrange", err)
	}
	return decodeZMembers("kv zrange", raw)
}

// ZRangeByScore returns members with scores between min and max.
func (kv *KVModel) ZRangeByScore(ctx context.Context, key string, min, max float64) ([]string, error) {
	if err := kv.client.requireNucleus("KV.ZRangeByScore"); err != nil {
		return nil, err
	}
	var raw string
	err := kv.pool.QueryRow(ctx, "SELECT KV_ZRANGEBYSCORE($1, $2, $3)", key, min, max).Scan(&raw)
	if err != nil {
		return nil, wrapErr("kv zrangebyscore", err)
	}
	return decodeZMembers("kv zrangebyscore", raw)
}

// decodeZMembers parses the engine's JSON [member, score] pair array into the
// "member:score" string slice shape the sorted-set methods expose. An empty
// collection ("" or "[]") yields a nil slice. Scores are rendered with the
// minimum digits needed to represent them (strconv 'g'), matching the prior
// integer-style "member:1" output for whole numbers.
func decodeZMembers(op, raw string) ([]string, error) {
	if raw == "" {
		return nil, nil
	}
	var pairs []struct {
		Member string
		Score  float64
	}
	tuples := make([][2]json.RawMessage, 0)
	if err := json.Unmarshal([]byte(raw), &tuples); err != nil {
		return nil, wrapErr(op+" decode", err)
	}
	for _, t := range tuples {
		var p struct {
			Member string
			Score  float64
		}
		if err := json.Unmarshal(t[0], &p.Member); err != nil {
			return nil, wrapErr(op+" decode member", err)
		}
		if err := json.Unmarshal(t[1], &p.Score); err != nil {
			return nil, wrapErr(op+" decode score", err)
		}
		pairs = append(pairs, p)
	}
	if len(pairs) == 0 {
		return nil, nil
	}
	// Members only, like Redis ZRANGE without WITHSCORES.
	//
	// This returned Member + ":" + Score. KV_ZRANGE answers with clean JSON
	// pairs, so the ENGINE never had the delimiter problem the conformance note
	// blamed it for — this loop reintroduced it, and a member containing ':'
	// became indistinguishable from the separator.
	out := make([]string, len(pairs))
	for i, p := range pairs {
		out[i] = p.Member
	}
	return out, nil
}

// ZRem removes a member from a sorted set.
func (kv *KVModel) ZRem(ctx context.Context, key, member string) (bool, error) {
	if err := kv.client.requireNucleus("KV.ZRem"); err != nil {
		return false, err
	}
	var ok bool
	err := kv.pool.QueryRow(ctx, "SELECT KV_ZREM($1, $2)", key, member).Scan(&ok)
	return ok, wrapErr("kv zrem", err)
}

// ZCard returns the number of members in a sorted set.
func (kv *KVModel) ZCard(ctx context.Context, key string) (int64, error) {
	if err := kv.client.requireNucleus("KV.ZCard"); err != nil {
		return 0, err
	}
	var n int64
	err := kv.pool.QueryRow(ctx, "SELECT KV_ZCARD($1)", key).Scan(&n)
	return n, wrapErr("kv zcard", err)
}

// --- HyperLogLog ---

// PFAdd adds an element to a HyperLogLog.
func (kv *KVModel) PFAdd(ctx context.Context, key, element string) (bool, error) {
	if err := kv.client.requireNucleus("KV.PFAdd"); err != nil {
		return false, err
	}
	var ok bool
	err := kv.pool.QueryRow(ctx, "SELECT KV_PFADD($1, $2)", key, element).Scan(&ok)
	return ok, wrapErr("kv pfadd", err)
}

// PFCount returns the approximate cardinality of a HyperLogLog.
func (kv *KVModel) PFCount(ctx context.Context, key string) (int64, error) {
	if err := kv.client.requireNucleus("KV.PFCount"); err != nil {
		return 0, err
	}
	var n int64
	err := kv.pool.QueryRow(ctx, "SELECT KV_PFCOUNT($1)", key).Scan(&n)
	return n, wrapErr("kv pfcount", err)
}

// PFMerge merges the source HyperLogLogs into dest (their set union). Use this
// for a unique count across buckets — summing per-bucket PFCounts over-counts
// elements seen in more than one bucket.
func (kv *KVModel) PFMerge(ctx context.Context, dest string, sources ...string) error {
	if err := kv.client.requireNucleus("KV.PFMerge"); err != nil {
		return err
	}
	if len(sources) == 0 {
		return nil
	}
	args := make([]any, 0, len(sources)+1)
	args = append(args, dest)
	placeholders := make([]string, 0, len(sources)+1)
	placeholders = append(placeholders, "$1")
	for i, s := range sources {
		args = append(args, s)
		placeholders = append(placeholders, fmt.Sprintf("$%d", i+2))
	}
	_, err := kv.pool.Exec(ctx, "SELECT KV_PFMERGE("+strings.Join(placeholders, ", ")+")", args...)
	return wrapErr("kv pfmerge", err)
}

// --- helpers ---

func wrapErr(op string, err error) error {
	if err == nil {
		return nil
	}
	return fmt.Errorf("nucleus: %s: %w", op, err)
}
