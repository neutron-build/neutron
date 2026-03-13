package nucleus

import (
	"context"
)

// PubSubModel provides PubSub operations over Nucleus SQL functions.
// This is separate from the LISTEN/NOTIFY helpers in listen.go.
type PubSubModel struct {
	pool   querier
	client *Client
}

// Publish sends a message on a channel. Returns the number of subscribers reached.
func (p *PubSubModel) Publish(ctx context.Context, channel, message string) (int64, error) {
	if err := p.client.requireNucleus("PubSub.Publish"); err != nil {
		return 0, err
	}
	var n int64
	err := p.pool.QueryRow(ctx, "SELECT PUBSUB_PUBLISH($1, $2)", channel, message).Scan(&n)
	return n, wrapErr("pubsub publish", err)
}

// Channels returns active PubSub channels matching an optional pattern.
// If pattern is empty, all channels are returned.
func (p *PubSubModel) Channels(ctx context.Context, pattern string) (string, error) {
	if err := p.client.requireNucleus("PubSub.Channels"); err != nil {
		return "", err
	}
	var raw string
	var err error
	if pattern != "" {
		err = p.pool.QueryRow(ctx, "SELECT PUBSUB_CHANNELS($1)", pattern).Scan(&raw)
	} else {
		err = p.pool.QueryRow(ctx, "SELECT PUBSUB_CHANNELS()").Scan(&raw)
	}
	return raw, wrapErr("pubsub channels", err)
}

// Subscribers returns the number of subscribers on a channel.
func (p *PubSubModel) Subscribers(ctx context.Context, channel string) (int64, error) {
	if err := p.client.requireNucleus("PubSub.Subscribers"); err != nil {
		return 0, err
	}
	var n int64
	err := p.pool.QueryRow(ctx, "SELECT PUBSUB_SUBSCRIBERS($1)", channel).Scan(&n)
	return n, wrapErr("pubsub subscribers", err)
}
