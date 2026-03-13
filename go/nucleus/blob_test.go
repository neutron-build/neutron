package nucleus

import (
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
