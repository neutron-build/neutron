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
	// epochs holds a fresh random value per established client: row
	// identities are bound to it, so an identity read before a reconnect
	// can never address rows through the replacement connection.
	epochs map[string]string
	mu     sync.RWMutex
	srv    *http.Server
	// sessionToken is the per-launch CSRF-class token required on every
	// mutating endpoint (see session.go). Empty only for hand-constructed
	// Servers in tests that never call requireMutationAuth.
	sessionToken string
	// outcomes records commit results for operation-ID deduplication and
	// retry resolution (S02, see commit_v2.go). Lazily initialized with
	// default retention for hand-constructed test servers.
	outcomes *outcomeStore
	// maxCommitOps / maxMutBody bound one commit request. Defaults come from
	// the package constants; NEUTRON_STUDIO_MAX_COMMIT_OPERATIONS and
	// NEUTRON_STUDIO_MAX_MUTATION_BYTES can lower them at launch (values
	// above the default clamp to the default — configurable downward only).
	maxCommitOps int
	maxMutBody   int64
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
	token, err := newSessionToken()
	if err != nil {
		return nil, fmt.Errorf("session token: %w", err)
	}
	s := &Server{
		port:         port,
		store:        store,
		saved:        saved,
		clients:      map[string]*db.Client{},
		epochs:       map[string]string{},
		sessionToken: token,
		outcomes:     newOutcomeStore(defaultOutcomeCapacity, defaultOutcomeTTL, defaultStaleReservation),
		maxCommitOps: clampDownwardInt(envInt("NEUTRON_STUDIO_MAX_COMMIT_OPERATIONS"), maxCommitOperations),
		maxMutBody:   clampDownwardInt64(envInt64("NEUTRON_STUDIO_MAX_MUTATION_BYTES"), maxMutationBody),
	}
	return s, nil
}

// Start begins listening. Blocks until the context is cancelled.
func (s *Server) Start(ctx context.Context) error {
	mux, err := s.routes()
	if err != nil {
		return err
	}
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

// routes builds the full route table: every API endpoint plus the embedded
// SPA static handler. Extracted from Start so tests can drive the exact
// production routing (through corsMiddleware) without binding a listener.
func (s *Server) routes() (*http.ServeMux, error) {
	mux := http.NewServeMux()

	// API routes
	mux.HandleFunc("/api/session", s.handleSession)
	mux.HandleFunc("/api/connections", s.handleConnections)
	mux.HandleFunc("/api/connections/test", s.handleTest)
	mux.HandleFunc("/api/connections/", s.handleConnection) // /:id and /:id/connect
	mux.HandleFunc("/api/query", s.handleQuery)
	mux.HandleFunc("/api/schema", s.handleSchema)
	mux.HandleFunc("/api/features", s.handleFeatures)
	mux.HandleFunc("/api/table", s.handleTable)
	mux.HandleFunc("/api/table/v2/meta", s.handleTableRowMetaV2)
	mux.HandleFunc("/api/table/v2/insert", s.handleTableRowInsertV2)
	mux.HandleFunc("/api/table/v2/update", s.handleTableRowUpdateV2)
	mux.HandleFunc("/api/table/v2/delete", s.handleTableRowDeleteV2)
	mux.HandleFunc("/api/table/v2/commit", s.handleTableCommitV2)
	mux.HandleFunc("/api/table/v2/preview", s.handleTablePreviewV2)
	mux.HandleFunc("/api/table/v2/search", s.handleTableSearchV2)
	mux.HandleFunc("/api/table/v2/outcome", s.handleTableOutcomeV2)
	mux.HandleFunc("/api/table/v2/revert", s.handleTableRevertV2)
	mux.HandleFunc("/api/table/update", s.handleTableRowUpdate)
	mux.HandleFunc("/api/table/delete", s.handleTableRowDelete)
	mux.HandleFunc("/api/table/fks", s.handleTableFKs)
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
		return nil, fmt.Errorf("embed sub: %w", err)
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
	return mux, nil
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

// corsMiddleware enforces the loopback boundary for every request: the Host
// header must name the loopback listener (DNS-rebinding defense), a present
// Origin must be one of this launch's exact allowed origins (other localhost
// ports are other applications, not Studio), and every state-changing /api/
// request must carry the session token. CORS headers are echoed only for the
// exact allowed origins.
func (s *Server) corsMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !s.hostAllowed(r.Host) {
			http.Error(w, "Forbidden", http.StatusForbidden)
			return
		}
		origin := r.Header.Get("Origin")
		if origin != "" {
			if !s.originAllowed(origin) {
				writeAuthError(w, "origin", "origin not allowed")
				return
			}
			w.Header().Set("Access-Control-Allow-Origin", origin)
			w.Header().Set("Vary", "Origin")
			w.Header().Set("Access-Control-Allow-Methods", "GET, POST, PUT, DELETE, OPTIONS")
			w.Header().Set("Access-Control-Allow-Headers", "Content-Type, "+sessionHeader)
			if r.Method == http.MethodOptions {
				w.WriteHeader(http.StatusNoContent)
				return
			}
		}
		if strings.HasPrefix(r.URL.Path, "/api/") && isMutatingMethod(r.Method) && !s.requireMutationAuth(w, r) {
			return
		}
		next.ServeHTTP(w, r)
	})
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

// connectionEpoch returns the binding epoch of the connection's current
// client. Hand-constructed test servers without epochs report "0".
func (s *Server) connectionEpoch(id string) string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if e, ok := s.epochs[id]; ok {
		return e
	}
	return "0"
}

func (s *Server) setClient(id string, c *db.Client) {
	epoch, err := newSessionToken()
	if err != nil {
		// crypto/rand failure: fall back to a value that still changes per
		// client instance so stale identities are never rebound.
		epoch = fmt.Sprintf("t%d", time.Now().UnixNano())
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if old, ok := s.clients[id]; ok {
		old.Close()
	}
	s.clients[id] = c
	if s.epochs == nil {
		s.epochs = map[string]string{}
	}
	s.epochs[id] = epoch[:16]
}
