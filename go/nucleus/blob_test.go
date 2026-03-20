package nucleus

import (
	"context"
	"testing"
)

func TestBlobOptionWithContentType(t *testing.T) {
	var o blobOpts
	WithContentType("image/png")(&o)
	if o.contentType != "image/png" {
		t.Errorf("contentType = %q", o.contentType)
	}
}

func TestBlobOptionWithMetadata(t *testing.T) {
	meta := map[string]string{"author": "alice"}
	var o blobOpts
	WithBlobMetadata(meta)(&o)
	if o.metadata["author"] != "alice" {
		t.Errorf("metadata = %v", o.metadata)
	}
}

func TestBlobMetaStruct(t *testing.T) {
	m := BlobMeta{
		Key:         "test/file.txt",
		Size:        1024,
		ContentType: "text/plain",
	}
	if m.Size != 1024 {
		t.Errorf("Size = %d", m.Size)
	}
}

func TestBlobRequiresNucleus(t *testing.T) {
	q := &mockCDCQuerier{}
	client := plainPGClient()
	b := &BlobModel{pool: q, client: client}

	tests := []struct {
		name string
		fn   func() error
	}{
		{"Put", func() error { return b.Put(context.Background(), "bkt", "k", nil) }},
		{"Get", func() error { _, _, err := b.Get(context.Background(), "bkt", "k"); return err }},
		{"Delete", func() error { _, err := b.Delete(context.Background(), "bkt", "k"); return err }},
		{"Meta", func() error { _, err := b.Meta(context.Background(), "bkt", "k"); return err }},
		{"Tag", func() error { _, err := b.Tag(context.Background(), "bkt", "k", "tk", "tv"); return err }},
		{"List", func() error { _, err := b.List(context.Background(), "bkt", "pre"); return err }},
		{"Exists", func() error { _, err := b.Exists(context.Background(), "bkt", "k"); return err }},
		{"BlobCount", func() error { _, err := b.BlobCount(context.Background()); return err }},
		{"DedupRatio", func() error { _, err := b.DedupRatio(context.Background()); return err }},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			err := tc.fn()
			if err == nil {
				t.Fatal("expected error for non-Nucleus database")
			}
		})
	}
}

func TestBlobDefaultContentType(t *testing.T) {
	var o blobOpts
	// Default content type should be empty until Put sets it
	if o.contentType != "" {
		t.Errorf("default contentType = %q, want empty", o.contentType)
	}
}

func TestBlobMetaAllFields(t *testing.T) {
	m := BlobMeta{
		Key:         "bucket/key",
		Size:        2048,
		ContentType: "application/json",
		Metadata:    map[string]string{"env": "prod"},
	}
	if m.Key != "bucket/key" {
		t.Errorf("Key = %q", m.Key)
	}
	if m.ContentType != "application/json" {
		t.Errorf("ContentType = %q", m.ContentType)
	}
	if m.Metadata["env"] != "prod" {
		t.Errorf("Metadata = %v", m.Metadata)
	}
}
