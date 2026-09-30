package studio

import (
	"context"
	"fmt"
	"os"
	"path/filepath"

	"github.com/neutron-build/neutron/cli/internal/db"
)

// SetSchemaSource binds designer plans to the ownership declarations in a v2
// schema file. Each preview and locked apply reads it again; losing the file
// cannot silently remove an ownership boundary.
func (s *Server) SetSchemaSource(path string) error {
	absolute, err := filepath.Abs(path)
	if err != nil {
		return err
	}
	if _, err := readOwnershipDocument(absolute); err != nil {
		return err
	}
	s.mu.Lock()
	s.schemaSource = absolute
	s.mu.Unlock()
	return nil
}

func readOwnershipDocument(path string) (*db.V2Document, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("Studio schema ownership: %w", err)
	}
	doc, err := db.ParseV2Document(raw)
	if err != nil {
		return nil, fmt.Errorf("Studio schema ownership: %w", err)
	}
	return doc, nil
}

func (s *Server) planSchemaChanges(ctx context.Context, client *db.Client, changes []SchemaChange) (*StudioPlan, error) {
	s.mu.RLock()
	path := s.schemaSource
	s.mu.RUnlock()
	var ownership *db.V2Document
	if path != "" {
		var err error
		ownership, err = readOwnershipDocument(path)
		if err != nil {
			return nil, err
		}
	}
	return planSchemaChanges(ctx, client, changes, ownership)
}
