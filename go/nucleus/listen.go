package nucleus

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5/pgxpool"
)

// Notification represents a PostgreSQL LISTEN/NOTIFY notification.
type Notification struct {
	Channel string
	Payload string
}

// Listen subscribes to a PostgreSQL channel and delivers notifications to the
// callback. It acquires a dedicated connection from the pool and blocks until
// the context is cancelled.
func (c *Client) Listen(ctx context.Context, channel string, handler func(Notification)) error {
	conn, err := c.pool.Acquire(ctx)
	if err != nil {
		return fmt.Errorf("nucleus: listen acquire: %w", err)
	}
	defer conn.Release()

	_, err = conn.Exec(ctx, "LISTEN "+quoteIdent(channel))
	if err != nil {
		return fmt.Errorf("nucleus: listen: %w", err)
	}

	for {
		n, err := conn.Conn().WaitForNotification(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return nil // context cancelled, clean exit
			}
			return fmt.Errorf("nucleus: wait for notification: %w", err)
		}
		handler(Notification{Channel: n.Channel, Payload: n.Payload})
	}
}

// Notify sends a notification on a channel.
func (c *Client) Notify(ctx context.Context, channel, payload string) error {
	_, err := c.pool.Exec(ctx, "SELECT pg_notify($1, $2)", channel, payload)
	return wrapErr("notify", err)
}

// Publish sends a message on a PubSub channel using Nucleus's PUBSUB_PUBLISH.
// Deprecated: Use client.PubSub().Publish() instead.
func (c *Client) Publish(ctx context.Context, channel, message string) (int64, error) {
	if err := c.requireNucleus("Publish"); err != nil {
		return 0, err
	}
	var n int64
	err := c.pool.QueryRow(ctx, "SELECT PUBSUB_PUBLISH($1, $2)", channel, message).Scan(&n)
	return n, wrapErr("pubsub publish", err)
}

// Channels returns active PubSub channels matching an optional pattern.
// Deprecated: Use client.PubSub().Channels() instead.
func (c *Client) Channels(ctx context.Context, pattern string) (string, error) {
	if err := c.requireNucleus("Channels"); err != nil {
		return "", err
	}
	var raw string
	var err error
	if pattern != "" {
		err = c.pool.QueryRow(ctx, "SELECT PUBSUB_CHANNELS($1)", pattern).Scan(&raw)
	} else {
		err = c.pool.QueryRow(ctx, "SELECT PUBSUB_CHANNELS()").Scan(&raw)
	}
	return raw, wrapErr("pubsub channels", err)
}

// Subscribers returns the number of subscribers on a channel.
// Deprecated: Use client.PubSub().Subscribers() instead.
func (c *Client) Subscribers(ctx context.Context, channel string) (int64, error) {
	if err := c.requireNucleus("Subscribers"); err != nil {
		return 0, err
	}
	var n int64
	err := c.pool.QueryRow(ctx, "SELECT PUBSUB_SUBSCRIBERS($1)", channel).Scan(&n)
	return n, wrapErr("pubsub subscribers", err)
}

// ListenPool returns the underlying pool for custom LISTEN/NOTIFY usage.
func (c *Client) ListenPool() *pgxpool.Pool {
	return c.pool
}

func quoteIdent(s string) string {
	// Simple identifier quoting — no special chars expected for channel names
	return `"` + s + `"`
}
