package nucleus

import (
	"context"
	"fmt"
	"regexp"
	"strings"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/neutron-dev/neutron-go/neutron"
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
type Client struct {
	pool     *pgxpool.Pool
	features Features
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
	Version     string
}

// Option configures the Client.
type Option func(*clientOpts)

type clientOpts struct {
	poolConfig *pgxpool.Config
}

// WithPoolConfig provides a custom pgxpool.Config.
func WithPoolConfig(cfg *pgxpool.Config) Option {
	return func(o *clientOpts) { o.poolConfig = cfg }
}

// Connect creates a new Client, establishing a connection pool and
// auto-detecting Nucleus features via SELECT VERSION().
func Connect(ctx context.Context, url string, opts ...Option) (*Client, error) {
	var o clientOpts
	for _, opt := range opts {
		opt(&o)
	}

	var pool *pgxpool.Pool
	var err error

	if o.poolConfig != nil {
		pool, err = pgxpool.NewWithConfig(ctx, o.poolConfig)
	} else {
		pool, err = pgxpool.New(ctx, url)
	}
	if err != nil {
		return nil, fmt.Errorf("nucleus: connect: %w", err)
	}

	// Auto-detect features
	features, err := detectFeatures(ctx, pool)
	if err != nil {
		pool.Close()
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
	c.pool.Close()
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
			c.pool.Close()
			return nil
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
	}

	return f, nil
}
