package studio

import (
	"context"
	"encoding/json"
	"fmt"
	"io/fs"
	"log"
	"net"
	"net/http"
	"os/exec"
	"runtime"
	"strings"
	"sync"
	"time"

	"github.com/neutron-build/neutron/cli/internal/db"
)

// Server is the embedded Studio HTTP server.
type Server struct {
	port    int
	store   *connectionStore
	saved   *savedQueryStore
	clients map[string]*db.Client
	mu      sync.RWMutex
	srv     *http.Server
}

// NewServer creates and configures the Studio server on the given port.
func NewServer(port int) (*Server, error) {
	store, err := newConnectionStore()
	if err != nil {
		return nil, fmt.Errorf("connection store: %w", err)
	}
	saved, err := newSavedQueryStore()
	if err != nil {
		return nil, fmt.Errorf("saved query store: %w", err)
	}
	s := &Server{
		port:    port,
		store:   store,
		saved:   saved,
		clients: map[string]*db.Client{},
	}
	return s, nil
}

// Start begins listening. Blocks until the context is cancelled.
func (s *Server) Start(ctx context.Context) error {
	mux := http.NewServeMux()

	// API routes
	mux.HandleFunc("/api/connections", s.handleConnections)
	mux.HandleFunc("/api/connections/test", s.handleTest)
	mux.HandleFunc("/api/connections/", s.handleConnection) // /:id and /:id/connect
	mux.HandleFunc("/api/query", s.handleQuery)
	mux.HandleFunc("/api/schema", s.handleSchema)
	mux.HandleFunc("/api/features", s.handleFeatures)
	mux.HandleFunc("/api/table", s.handleTable)
	mux.HandleFunc("/api/columns", s.handleColumns)
	mux.HandleFunc("/api/ddl", s.handleDDL)
	mux.HandleFunc("/api/codegen", s.handleCodegen)
	mux.HandleFunc("/api/saved-queries", s.handleSavedQueries)
	mux.HandleFunc("/api/saved-queries/", s.handleSavedQuery)
	mux.HandleFunc("/api/blob/upload", s.handleBlobUpload)
	mux.HandleFunc("/api/blob/", s.handleBlob)

	// SPA static files
	distFS, err := fs.Sub(Dist, "dist")
	if err != nil {
		return fmt.Errorf("embed sub: %w", err)
	}
	fileServer := http.FileServer(http.FS(distFS))
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		// Try the file; on 404 serve index.html for SPA routing
		path := strings.TrimPrefix(r.URL.Path, "/")
		if path == "" {
			path = "index.html"
		}
		if _, err := fs.Stat(distFS, path); err != nil {
			r.URL.Path = "/"
		}
		fileServer.ServeHTTP(w, r)
	})

	s.srv = &http.Server{
		Addr:    fmt.Sprintf("127.0.0.1:%d", s.port),
		Handler: s.corsMiddleware(mux),
	}

	ln, err := net.Listen("tcp", s.srv.Addr)
	if err != nil {
		return fmt.Errorf("listen on port %d: %w", s.port, err)
	}

	go func() {
		<-ctx.Done()
		shutCtx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		s.srv.Shutdown(shutCtx) //nolint
	}()

	log.Printf("Studio running at http://localhost:%d\n", s.port)
	if err := s.srv.Serve(ln); err != nil && err != http.ErrServerClosed {
		return err
	}
	return nil
}

// URL returns the local URL for the Studio server.
func (s *Server) URL() string {
	return fmt.Sprintf("http://localhost:%d", s.port)
}

// OpenBrowser opens the given URL in the default browser.
func OpenBrowser(url string) {
	var cmd string
	var args []string
	switch runtime.GOOS {
	case "darwin":
		cmd, args = "open", []string{url}
	case "windows":
		cmd, args = "cmd", []string{"/c", "start", url}
	default:
		cmd, args = "xdg-open", []string{url}
	}
	exec.Command(cmd, args...).Start() //nolint
}

// corsMiddleware restricts cross-origin requests to localhost origins only.
func (s *Server) corsMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		origin := r.Header.Get("Origin")
		if origin != "" {
			if !isLocalhostOrigin(origin) {
				http.Error(w, "Forbidden", http.StatusForbidden)
				return
			}
			w.Header().Set("Access-Control-Allow-Origin", origin)
			w.Header().Set("Access-Control-Allow-Methods", "GET, POST, PUT, DELETE, OPTIONS")
			w.Header().Set("Access-Control-Allow-Headers", "Content-Type")
			if r.Method == "OPTIONS" {
				w.WriteHeader(204)
				return
			}
		}
		next.ServeHTTP(w, r)
	})
}

func isLocalhostOrigin(origin string) bool {
	return strings.HasPrefix(origin, "http://localhost:") ||
		strings.HasPrefix(origin, "http://127.0.0.1:") ||
		strings.HasPrefix(origin, "http://[::1]:") ||
		origin == "http://localhost" || origin == "http://127.0.0.1"
}

// --- Helpers ---

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(v) //nolint
}

func writeError(w http.ResponseWriter, status int, msg string) {
	writeJSON(w, status, map[string]string{"error": msg})
}

func (s *Server) clientFor(id string) (*db.Client, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	c, ok := s.clients[id]
	return c, ok
}

func (s *Server) setClient(id string, c *db.Client) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if old, ok := s.clients[id]; ok {
		old.Close()
	}
	s.clients[id] = c
}
