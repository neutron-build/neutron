package nucleus

import (
	"context"
	"errors"
	"fmt"
	"regexp"
	"strings"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/neutron-build/neutron/go/neutron"
)

// identifierRe validates SQL identifiers (table names, column names).
// Only allows alphanumeric characters and underscores, must start with a letter or underscore.
var identifierRe = regexp.MustCompile(`^[a-zA-Z_][a-zA-Z0-9_]*$`)

// isValidIdentifier checks whether a string is a safe SQL identifier.
func isValidIdentifier(name string) bool {
	return identifierRe.MatchString(name)
}

// Client is the Nucleus database client. It wraps a pgx connection pool and
// auto-detects whether the target is a plain PostgreSQL instance or Nucleus.
// A Client must not be copied after use.
type Client struct {
	pool      *pgxpool.Pool
	features  Features
	closeOnce sync.Once
	closeDone chan struct{}
}

// Features describes capabilities detected on the connected database.
type Features struct {
	IsNucleus   bool
	HasKV       bool
	HasVector   bool
	HasTS       bool
	HasDocument bool
	HasGraph    bool
	HasFTS      bool
	HasGeo      bool
	HasBlob     bool
	HasStreams  bool
	HasColumnar bool
	HasDatalog  bool
	HasCDC      bool
	HasPubSub   bool
	Version     string
}

// Option configures the Client.
type Option func(*clientOpts)

type clientOpts struct {
	poolConfig     *pgxpool.Config
	connectTimeout time.Duration
}

// WithPoolConfig provides a custom pgxpool.Config.
func WithPoolConfig(cfg *pgxpool.Config) Option {
	return func(o *clientOpts) { o.poolConfig = cfg }
}

// Connect creates a new Client, establishing a connection pool and
// auto-detecting Nucleus features via SELECT VERSION().
func Connect(ctx context.Context, url string, opts ...Option) (*Client, error) {
	o := clientOpts{connectTimeout: 30 * time.Second}
	for _, opt := range opts {
		opt(&o)
	}

	if o.connectTimeout <= 0 {
		return nil, ErrInvalidConfig
	}
	ctx, cancel := context.WithTimeout(ctx, o.connectTimeout)
	defer cancel()
	cfg, err := preparePoolConfig(url, o.poolConfig, o.connectTimeout)
	if err != nil {
		return nil, err
	}
	pool, err := newPool(ctx, cfg)
	if err != nil {
		return nil, fmt.Errorf("nucleus: connect: %w", err)
	}

	// Auto-detect features
	features, err := detectFeatures(ctx, pool)
	if err != nil {
		// A stalled driver cleanup or BeforeClose callback must not extend
		// the caller's startup wait. The same single shutdown worker continues
		// owning this pool until its borrowed resources/callbacks finish.
		cleanup := &Client{pool: pool}
		_ = cleanup.CloseContext(ctx)
		return nil, fmt.Errorf("nucleus: detect features: %w", err)
	}

	return &Client{pool: pool, features: features}, nil
}

// Pool returns the underlying pgx connection pool.
func (c *Client) Pool() *pgxpool.Pool {
	return c.pool
}

// Features returns the detected database capabilities.
func (c *Client) Features() Features {
	return c.features
}

// IsNucleus returns true if the connected database is Nucleus.
// Satisfies the neutron.NucleusChecker interface.
func (c *Client) IsNucleus() bool {
	return c.features.IsNucleus
}

// Close closes the connection pool.
func (c *Client) Close() {
	c.beginClose()
	<-c.closeDone
}

// CloseContext starts pool shutdown once and waits until completion or ctx
// expiry. One shutdown worker is shared by all calls. Borrowed rows/transactions
// must still be released by their owners; BeforeClose callbacks must return;
// a timeout does not claim the underlying pool has finished closing.
func (c *Client) CloseContext(ctx context.Context) error {
	c.beginClose()
	select {
	case <-c.closeDone:
		return nil
	default:
	}
	select {
	case <-c.closeDone:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (c *Client) beginClose() {
	c.closeOnce.Do(func() {
		c.closeDone = make(chan struct{})
		go func() { c.pool.Close(); close(c.closeDone) }()
	})
}

// SQL returns the SQL model for type-safe queries.
func (c *Client) SQL() *SQLModel {
	return &SQLModel{pool: c.pool}
}

// KV returns the key-value model.
func (c *Client) KV() *KVModel {
	return &KVModel{pool: c.pool, client: c}
}

// Vector returns the vector search model.
func (c *Client) Vector() *VectorModel {
	return &VectorModel{pool: c.pool, client: c}
}

// TimeSeries returns the time-series model.
func (c *Client) TimeSeries() *TimeSeriesModel {
	return &TimeSeriesModel{pool: c.pool, client: c}
}

// Document returns the document/JSON model.
func (c *Client) Document() *DocumentModel {
	return &DocumentModel{pool: c.pool, client: c}
}

// Graph returns the graph model.
func (c *Client) Graph() *GraphModel {
	return &GraphModel{pool: c.pool, client: c}
}

// FTS returns the full-text search model.
func (c *Client) FTS() *FTSModel {
	return &FTSModel{pool: c.pool, client: c}
}

// Geo returns the geospatial model.
func (c *Client) Geo() *GeoModel {
	return &GeoModel{pool: c.pool, client: c}
}

// Blob returns the blob storage model.
func (c *Client) Blob() *BlobModel {
	return &BlobModel{pool: c.pool, client: c}
}

// Streams returns the Redis Streams model.
func (c *Client) Streams() *StreamModel {
	return &StreamModel{pool: c.pool, client: c}
}

// Columnar returns the columnar analytics model.
func (c *Client) Columnar() *ColumnarModel {
	return &ColumnarModel{pool: c.pool, client: c}
}

// Datalog returns the Datalog reasoning model.
func (c *Client) Datalog() *DatalogModel {
	return &DatalogModel{pool: c.pool, client: c}
}

// CDC returns the Change Data Capture model.
func (c *Client) CDC() *CDCModel {
	return &CDCModel{pool: c.pool, client: c}
}

// PubSub returns the PubSub model.
func (c *Client) PubSub() *PubSubModel {
	return &PubSubModel{pool: c.pool, client: c}
}

// Ping verifies the database connection.
func (c *Client) Ping(ctx context.Context) error {
	return c.pool.Ping(ctx)
}

// LifecycleHook returns a neutron.LifecycleHook that manages the connection pool.
func (c *Client) LifecycleHook() neutron.LifecycleHook {
	return neutron.LifecycleHook{
		Name: "nucleus",
		OnStart: func(ctx context.Context) error {
			return c.pool.Ping(ctx)
		},
		OnStop: func(ctx context.Context) error {
			return c.CloseContext(ctx)
		},
	}
}

// requireNucleus returns an error if the connected database is not Nucleus.
func (c *Client) requireNucleus(feature string) error {
	if !c.features.IsNucleus {
		return &neutron.AppError{
			Status: 501,
			Code:   "https://neutron.dev/errors/nucleus-required",
			Title:  "Nucleus Required",
			Detail: fmt.Sprintf("%s requires Nucleus database, but connected to plain PostgreSQL", feature),
		}
	}
	return nil
}

func detectFeatures(ctx context.Context, pool *pgxpool.Pool) (Features, error) {
	var version string
	err := pool.QueryRow(ctx, "SELECT VERSION()").Scan(&version)
	if err != nil {
		return Features{}, err
	}

	f := Features{Version: version}

	if strings.Contains(version, "Nucleus") {
		f.IsNucleus = true
		f.HasKV = true
		f.HasVector = true
		f.HasTS = true
		f.HasDocument = true
		f.HasGraph = true
		f.HasFTS = true
		f.HasGeo = true
		f.HasBlob = true
		f.HasStreams = true
		f.HasColumnar = true
		f.HasDatalog = true
		f.HasCDC = true
		f.HasPubSub = true
	}

	return f, nil
}

// ErrInvalidConfig deliberately carries no DSN or parser cause: driver parse
// errors and panic values may contain passwords in supported DSN spellings.
var ErrInvalidConfig = errors.New("nucleus: invalid connection configuration")

// ErrChannelBindingUnsupported means this SDK's driver cannot enforce the
// requested SCRAM channel-binding contract. Refusal precedes network I/O.
var ErrChannelBindingUnsupported = errors.New("nucleus: required channel binding is unsupported by the current driver")

// WithConnectTimeout bounds initial pool acquisition and feature detection.
// The default is 30 seconds. The caller's earlier deadline still applies.
func WithConnectTimeout(timeout time.Duration) Option {
	return func(o *clientOpts) { o.connectTimeout = timeout }
}

func preparePoolConfig(dsn string, supplied *pgxpool.Config, budget time.Duration) (cfg *pgxpool.Config, err error) {
	// Recover only at the configuration boundary, before any pool goroutines.
	defer func() {
		if recover() != nil {
			cfg = nil
			err = ErrInvalidConfig
		}
	}()
	if supplied != nil {
		if supplied.ConnConfig == nil {
			return nil, ErrInvalidConfig
		}
		cfg = supplied.Copy()
	} else {
		cfg, err = pgxpool.ParseConfig(dsn)
		if err != nil {
			return nil, ErrInvalidConfig
		}
	}
	if cfg.MaxConns <= 0 || cfg.MinConns < 0 || cfg.MinConns > cfg.MaxConns ||
		cfg.HealthCheckPeriod <= 0 || cfg.MaxConnLifetime <= 0 || cfg.MaxConnIdleTime <= 0 ||
		cfg.MaxConnLifetimeJitter < 0 || cfg.MaxConnLifetimeJitter > cfg.MaxConnLifetime {
		return nil, ErrInvalidConfig
	}
	// No connection constructor may silently remove the declared requirement.
	if err := prepareConnConfig(cfg.ConnConfig, budget); err != nil {
		return nil, err
	}
	before := cfg.BeforeConnect
	cfg.BeforeConnect = func(ctx context.Context, cc *pgx.ConnConfig) error {
		ctx, cancel := context.WithTimeout(ctx, budget)
		defer cancel()
		if before != nil {
			if err := before(ctx, cc); err != nil {
				return err
			}
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		return prepareConnConfig(cc, budget)
	}
	if after := cfg.AfterConnect; after != nil {
		cfg.AfterConnect = func(ctx context.Context, conn *pgx.Conn) error {
			ctx, cancel := context.WithTimeout(ctx, budget)
			defer cancel()
			if err := after(ctx, conn); err != nil {
				return err
			}
			return ctx.Err()
		}
	}
	return cfg, nil
}

func prepareConnConfig(cc *pgx.ConnConfig, budget time.Duration) error {
	if cc == nil || cc.ConnectTimeout < 0 {
		return ErrInvalidConfig
	}
	// This driver predates MinIdleConns; reject the unsupported pool option
	// before it can be transmitted as a server setting.
	if _, unsupported := cc.RuntimeParams["pool_min_idle_conns"]; unsupported {
		return ErrInvalidConfig
	}
	// pgx v5.7.2 has no SCRAM-PLUS dispatch. Do not send this libpq option
	// as a server runtime parameter or claim TLS alone provides binding.
	switch cc.RuntimeParams["channel_binding"] {
	case "", "disable", "prefer":
		delete(cc.RuntimeParams, "channel_binding")
	case "require":
		return ErrChannelBindingUnsupported
	default:
		return ErrInvalidConfig
	}
	if cc.ConnectTimeout == 0 || cc.ConnectTimeout > budget {
		cc.ConnectTimeout = budget
	}
	return nil
}

// pgx enforces ParseConfig provenance with a synchronous panic. Convert that
// configuration-only panic before NewWithConfig can start background work.
func newPool(ctx context.Context, cfg *pgxpool.Config) (pool *pgxpool.Pool, err error) {
	defer func() {
		if recover() != nil {
			pool = nil
			err = ErrInvalidConfig
		}
	}()
	return pgxpool.NewWithConfig(ctx, cfg)
}
