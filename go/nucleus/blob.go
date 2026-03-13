package nucleus

import (
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"time"
)

// BlobModel provides binary object storage over Nucleus SQL functions.
type BlobModel struct {
	pool   querier
	client *Client
}

// BlobMeta holds metadata about a stored blob.
type BlobMeta struct {
	Key         string            `json:"key"`
	Size        int64             `json:"size"`
	ContentType string            `json:"content_type"`
	CreatedAt   time.Time         `json:"created_at"`
	Metadata    map[string]string `json:"metadata,omitempty"`
}

// BlobOption configures blob operations.
type BlobOption func(*blobOpts)

type blobOpts struct {
	contentType string
	metadata    map[string]string
}

// WithContentType sets the content type of the blob.
func WithContentType(ct string) BlobOption {
	return func(o *blobOpts) { o.contentType = ct }
}

// WithBlobMetadata sets custom metadata on the blob.
func WithBlobMetadata(meta map[string]string) BlobOption {
	return func(o *blobOpts) { o.metadata = meta }
}

// Put stores a blob from a reader. The key includes the bucket prefix.
func (b *BlobModel) Put(ctx context.Context, bucket, key string, reader io.Reader, opts ...BlobOption) error {
	if err := b.client.requireNucleus("Blob.Put"); err != nil {
		return err
	}
	o := blobOpts{contentType: "application/octet-stream"}
	for _, fn := range opts {
		fn(&o)
	}

	data, err := io.ReadAll(reader)
	if err != nil {
		return fmt.Errorf("nucleus: blob read: %w", err)
	}

	fullKey := bucket + "/" + key
	hexData := hex.EncodeToString(data)

	_, err = b.pool.Exec(ctx, "SELECT BLOB_STORE($1, $2, $3)", fullKey, hexData, o.contentType)
	if err != nil {
		return wrapErr("blob store", err)
	}

	// Apply tags if metadata provided
	for k, v := range o.metadata {
		_, err := b.pool.Exec(ctx, "SELECT BLOB_TAG($1, $2, $3)", fullKey, k, v)
		if err != nil {
			return wrapErr("blob tag", err)
		}
	}

	return nil
}

// Get retrieves a blob as a ReadCloser with its metadata.
func (b *BlobModel) Get(ctx context.Context, bucket, key string) (io.ReadCloser, *BlobMeta, error) {
	if err := b.client.requireNucleus("Blob.Get"); err != nil {
		return nil, nil, err
	}
	fullKey := bucket + "/" + key

	var hexData *string
	err := b.pool.QueryRow(ctx, "SELECT BLOB_GET($1)", fullKey).Scan(&hexData)
	if err != nil {
		return nil, nil, wrapErr("blob get", err)
	}
	if hexData == nil {
		return nil, nil, nil
	}

	data, err := hex.DecodeString(*hexData)
	if err != nil {
		return nil, nil, fmt.Errorf("nucleus: blob decode hex: %w", err)
	}

	meta, _ := b.Meta(ctx, bucket, key)

	return io.NopCloser(bytes.NewReader(data)), meta, nil
}

// Delete removes a blob.
func (b *BlobModel) Delete(ctx context.Context, bucket, key string) (bool, error) {
	if err := b.client.requireNucleus("Blob.Delete"); err != nil {
		return false, err
	}
	fullKey := bucket + "/" + key
	var ok bool
	err := b.pool.QueryRow(ctx, "SELECT BLOB_DELETE($1)", fullKey).Scan(&ok)
	return ok, wrapErr("blob delete", err)
}

// Meta returns metadata for a blob.
func (b *BlobModel) Meta(ctx context.Context, bucket, key string) (*BlobMeta, error) {
	if err := b.client.requireNucleus("Blob.Meta"); err != nil {
		return nil, err
	}
	fullKey := bucket + "/" + key
	var raw *string
	err := b.pool.QueryRow(ctx, "SELECT BLOB_META($1)", fullKey).Scan(&raw)
	if err != nil {
		return nil, wrapErr("blob meta", err)
	}
	if raw == nil {
		return nil, nil
	}
	var meta BlobMeta
	if err := json.Unmarshal([]byte(*raw), &meta); err != nil {
		return nil, fmt.Errorf("nucleus: blob meta unmarshal: %w", err)
	}
	return &meta, nil
}

// Tag sets a metadata tag on a blob.
func (b *BlobModel) Tag(ctx context.Context, bucket, key, tagKey, tagValue string) (bool, error) {
	if err := b.client.requireNucleus("Blob.Tag"); err != nil {
		return false, err
	}
	fullKey := bucket + "/" + key
	var ok bool
	err := b.pool.QueryRow(ctx, "SELECT BLOB_TAG($1, $2, $3)", fullKey, tagKey, tagValue).Scan(&ok)
	return ok, wrapErr("blob tag", err)
}

// List returns metadata for all blobs matching a prefix.
func (b *BlobModel) List(ctx context.Context, bucket, prefix string) ([]BlobMeta, error) {
	if err := b.client.requireNucleus("Blob.List"); err != nil {
		return nil, err
	}
	fullPrefix := bucket + "/" + prefix
	var raw string
	err := b.pool.QueryRow(ctx, "SELECT BLOB_LIST($1)", fullPrefix).Scan(&raw)
	if err != nil {
		return nil, wrapErr("blob list", err)
	}
	var metas []BlobMeta
	if err := json.Unmarshal([]byte(raw), &metas); err != nil {
		return nil, fmt.Errorf("nucleus: blob list unmarshal: %w", err)
	}
	return metas, nil
}

// Exists checks if a blob exists.
func (b *BlobModel) Exists(ctx context.Context, bucket, key string) (bool, error) {
	if err := b.client.requireNucleus("Blob.Exists"); err != nil {
		return false, err
	}
	meta, err := b.Meta(ctx, bucket, key)
	if err != nil {
		return false, err
	}
	return meta != nil, nil
}

// BlobCount returns the total number of stored blobs.
func (b *BlobModel) BlobCount(ctx context.Context) (int64, error) {
	if err := b.client.requireNucleus("Blob.BlobCount"); err != nil {
		return 0, err
	}
	var n int64
	err := b.pool.QueryRow(ctx, "SELECT BLOB_COUNT()").Scan(&n)
	return n, wrapErr("blob count", err)
}

// DedupRatio returns the deduplication ratio.
func (b *BlobModel) DedupRatio(ctx context.Context) (float64, error) {
	if err := b.client.requireNucleus("Blob.DedupRatio"); err != nil {
		return 0, err
	}
	var ratio float64
	err := b.pool.QueryRow(ctx, "SELECT BLOB_DEDUP_RATIO()").Scan(&ratio)
	return ratio, wrapErr("blob dedup_ratio", err)
}
