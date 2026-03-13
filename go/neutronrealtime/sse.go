package neutronrealtime

import (
	"fmt"
	"net/http"
)

// SSEHandler returns an http.Handler that serves Server-Sent Events.
// The stream function receives a send callback; it should block until done.
func SSEHandler(stream func(ctx interface{ Done() <-chan struct{} }, send func(event string, data []byte) error) error) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		flusher, ok := w.(http.Flusher)
		if !ok {
			http.Error(w, "streaming not supported", http.StatusInternalServerError)
			return
		}

		w.Header().Set("Content-Type", "text/event-stream")
		w.Header().Set("Cache-Control", "no-cache")
		w.Header().Set("Connection", "keep-alive")
		w.Header().Set("X-Accel-Buffering", "no")
		w.WriteHeader(http.StatusOK)
		flusher.Flush()

		send := func(event string, data []byte) error {
			if event != "" {
				if _, err := fmt.Fprintf(w, "event: %s\n", event); err != nil {
					return err
				}
			}
			if _, err := fmt.Fprintf(w, "data: %s\n\n", data); err != nil {
				return err
			}
			flusher.Flush()
			return nil
		}

		_ = stream(r.Context(), send)
	})
}
