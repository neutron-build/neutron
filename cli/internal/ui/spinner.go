package ui

import (
	"fmt"
	"time"
)

// Spinner provides a simple animated spinner for long-running operations.
type Spinner struct {
	message string
	done    chan struct{}
}

// NewSpinner creates and starts a spinner with the given message.
func NewSpinner(message string) *Spinner {
	s := &Spinner{
		message: message,
		done:    make(chan struct{}),
	}
	go s.run()
	return s
}

func (s *Spinner) run() {
	frames := []string{"⠋", "⠙", "⠹", "⠸", "⠼", "⠴", "⠦", "⠧", "⠇", "⠏"}
	i := 0
	ticker := time.NewTicker(80 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-s.done:
			fmt.Printf("\r\033[K")
			return
		case <-ticker.C:
			fmt.Printf("\r%s %s", DimStyle.Render(frames[i%len(frames)]), s.message)
			i++
		}
	}
}

// Stop stops the spinner and clears the line.
func (s *Spinner) Stop() {
	close(s.done)
	// Small delay to let the goroutine clear the line
	time.Sleep(100 * time.Millisecond)
}

// StopWithMessage stops the spinner and prints a final message.
func (s *Spinner) StopWithMessage(icon, message string) {
	close(s.done)
	time.Sleep(100 * time.Millisecond)
	fmt.Printf("%s %s\n", icon, message)
}
