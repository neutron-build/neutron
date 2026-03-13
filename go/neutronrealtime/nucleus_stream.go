package neutronrealtime

import (
	"context"
	"encoding/json"

	"github.com/neutron-dev/neutron-go/nucleus"
)

// NucleusStream returns an SSE stream function that bridges Nucleus
// LISTEN/NOTIFY to Server-Sent Events. Each notification is forwarded
// as an SSE event.
func NucleusStream(client *nucleus.Client, channel string) func(ctx interface{ Done() <-chan struct{} }, send func(string, []byte) error) error {
	return func(ctx interface{ Done() <-chan struct{} }, send func(string, []byte) error) error {
		// We need a real context.Context for the nucleus Listen call
		stdCtx, ok := ctx.(context.Context)
		if !ok {
			return nil
		}

		return client.Listen(stdCtx, channel, func(n nucleus.Notification) {
			data, _ := json.Marshal(map[string]string{
				"channel": n.Channel,
				"payload": n.Payload,
			})
			_ = send(channel, data)
		})
	}
}

// NucleusHubBridge listens on a Nucleus channel and broadcasts
// notifications to a Hub room. It blocks until the context is cancelled.
func NucleusHubBridge(ctx context.Context, client *nucleus.Client, channel string, hub *Hub, room string) error {
	return client.Listen(ctx, channel, func(n nucleus.Notification) {
		hub.Broadcast(room, []byte(n.Payload))
	})
}
