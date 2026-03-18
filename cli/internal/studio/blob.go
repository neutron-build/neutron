package studio

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"
)

// NOTE: pgLiteral was removed — all SQL now uses parameterized queries ($1, $2, ...).

// --- /api/blob/upload ---

func (s *Server) handleBlobUpload(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeError(w, http.StatusMethodNotAllowed, "POST required")
		return
	}

	// Limit upload size to 100 MB
	r.Body = http.MaxBytesReader(w, r.Body, 100<<20)

	if err := r.ParseMultipartForm(32 << 20); err != nil {
		writeError(w, http.StatusBadRequest, fmt.Sprintf("parse multipart: %s", err))
		return
	}

	connectionID := r.FormValue("connectionId")
	store := r.FormValue("store")
	if connectionID == "" || store == "" {
		writeError(w, http.StatusBadRequest, "connectionId and store are required")
		return
	}

	// Validate store name (must be a safe identifier)
	if !isValidIdent(store) {
		writeError(w, http.StatusBadRequest, "invalid store name")
		return
	}

	client, ok := s.clientFor(connectionID)
	if !ok {
		writeError(w, http.StatusBadRequest, "not connected — call /api/connections/:id/connect first")
		return
	}

	file, header, err := r.FormFile("file")
	if err != nil {
		writeError(w, http.StatusBadRequest, fmt.Sprintf("file field required: %s", err))
		return
	}
	defer file.Close()

	data, err := io.ReadAll(file)
	if err != nil {
		writeError(w, http.StatusInternalServerError, fmt.Sprintf("read file: %s", err))
		return
	}

	contentType := header.Header.Get("Content-Type")
	if contentType == "" {
		contentType = "application/octet-stream"
	}

	// Generate a blob key from hash + filename
	h := sha256.Sum256(data)
	blobID := hex.EncodeToString(h[:])

	// Check if this is a Nucleus connection
	saved, _ := s.store.Get(connectionID)

	if saved.IsNucleus {
		// Nucleus: use BLOB_STORE(key, data_hex, content_type) with parameterized queries
		dataHex := hex.EncodeToString(data)
		if err := client.Exec(r.Context(), "SELECT BLOB_STORE($1, $2, $3)", blobID, dataHex, contentType); err != nil {
			writeError(w, http.StatusInternalServerError, fmt.Sprintf("blob_store: %s", err))
			return
		}
	} else {
		// PostgreSQL: INSERT INTO <store> (id, data, content_type, size, hash, created_at)
		sql := fmt.Sprintf(
			`INSERT INTO %s (id, data, content_type, size, hash, created_at) VALUES ($1, $2, $3, $4, $5, $6)
			 ON CONFLICT (id) DO UPDATE SET data = EXCLUDED.data, content_type = EXCLUDED.content_type, size = EXCLUDED.size, hash = EXCLUDED.hash`,
			quoteIdent(store),
		)
		if err := client.Exec(r.Context(), sql, blobID, data, contentType, len(data), blobID, time.Now()); err != nil {
			writeError(w, http.StatusInternalServerError, fmt.Sprintf("insert blob: %s", err))
			return
		}
	}

	writeJSON(w, http.StatusOK, map[string]any{
		"id":          blobID,
		"size":        len(data),
		"contentType": contentType,
	})
}

// --- /api/blob/{id}/data?connectionId=...&store=... ---

func (s *Server) handleBlobData(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeError(w, http.StatusMethodNotAllowed, "GET required")
		return
	}

	// Path: /api/blob/{id}/data
	path := strings.TrimPrefix(r.URL.Path, "/api/blob/")
	// path should be "{id}/data"
	parts := strings.SplitN(path, "/", 2)
	if len(parts) != 2 || parts[1] != "data" {
		writeError(w, http.StatusNotFound, "not found")
		return
	}
	blobID := parts[0]
	if blobID == "" {
		writeError(w, http.StatusBadRequest, "blob id required")
		return
	}

	q := r.URL.Query()
	connectionID := q.Get("connectionId")
	store := q.Get("store")
	if connectionID == "" || store == "" {
		writeError(w, http.StatusBadRequest, "connectionId and store are required")
		return
	}

	if !isValidIdent(store) {
		writeError(w, http.StatusBadRequest, "invalid store name")
		return
	}

	client, ok := s.clientFor(connectionID)
	if !ok {
		writeError(w, http.StatusBadRequest, "not connected — call /api/connections/:id/connect first")
		return
	}

	saved, _ := s.store.Get(connectionID)

	if saved.IsNucleus {
		// Nucleus: BLOB_GET(key) returns hex-encoded data
		row := client.QueryRow(r.Context(), "SELECT BLOB_GET($1)", blobID)
		var hexData *string
		if err := row.Scan(&hexData); err != nil {
			writeError(w, http.StatusInternalServerError, fmt.Sprintf("blob_get: %s", err))
			return
		}
		if hexData == nil {
			writeError(w, http.StatusNotFound, "blob not found")
			return
		}

		data, err := hex.DecodeString(*hexData)
		if err != nil {
			writeError(w, http.StatusInternalServerError, fmt.Sprintf("hex decode: %s", err))
			return
		}

		// Try to get content type from BLOB_META
		contentType := "application/octet-stream"
		metaRow := client.QueryRow(r.Context(), "SELECT BLOB_META($1)", blobID)
		var metaJSON *string
		if err := metaRow.Scan(&metaJSON); err == nil && metaJSON != nil {
			// Parse content_type from the JSON string
			if ct := extractJSONField(*metaJSON, "content_type"); ct != "" {
				contentType = ct
			}
		}

		w.Header().Set("Content-Type", contentType)
		w.Header().Set("Content-Length", fmt.Sprintf("%d", len(data)))
		w.WriteHeader(http.StatusOK)
		w.Write(data) //nolint
	} else {
		// PostgreSQL: SELECT data, content_type FROM <store> WHERE id = $1
		sql := fmt.Sprintf(`SELECT data, content_type FROM %s WHERE id = $1`, quoteIdent(store))
		row := client.QueryRow(r.Context(), sql, blobID)

		var data []byte
		var contentType *string
		if err := row.Scan(&data, &contentType); err != nil {
			writeError(w, http.StatusNotFound, "blob not found")
			return
		}

		ct := "application/octet-stream"
		if contentType != nil && *contentType != "" {
			ct = *contentType
		}

		w.Header().Set("Content-Type", ct)
		w.Header().Set("Content-Length", fmt.Sprintf("%d", len(data)))
		w.WriteHeader(http.StatusOK)
		w.Write(data) //nolint
	}
}

// --- /api/blob/ router ---

func (s *Server) handleBlob(w http.ResponseWriter, r *http.Request) {
	path := strings.TrimPrefix(r.URL.Path, "/api/blob")

	// POST /api/blob/upload
	if path == "/upload" && r.Method == http.MethodPost {
		s.handleBlobUpload(w, r)
		return
	}

	// GET /api/blob/{id}/data
	if strings.HasSuffix(path, "/data") && r.Method == http.MethodGet {
		s.handleBlobData(w, r)
		return
	}

	writeError(w, http.StatusNotFound, "not found")
}

// isValidIdent checks that a string is a safe SQL identifier (alphanumeric + underscores).
func isValidIdent(s string) bool {
	if s == "" {
		return false
	}
	for _, c := range s {
		if !((c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9') || c == '_') {
			return false
		}
	}
	return true
}

// extractJSONField does a simple string extraction of a field value from a JSON object string.
// This avoids pulling in encoding/json for a single lightweight lookup.
func extractJSONField(jsonStr, field string) string {
	key := `"` + field + `":`
	idx := strings.Index(jsonStr, key)
	if idx < 0 {
		return ""
	}
	rest := strings.TrimSpace(jsonStr[idx+len(key):])
	if len(rest) == 0 {
		return ""
	}
	if rest[0] == '"' {
		// String value — find closing quote (handle escaped quotes)
		end := 1
		for end < len(rest) {
			if rest[end] == '\\' {
				end += 2
				continue
			}
			if rest[end] == '"' {
				return rest[1:end]
			}
			end++
		}
	}
	return ""
}
