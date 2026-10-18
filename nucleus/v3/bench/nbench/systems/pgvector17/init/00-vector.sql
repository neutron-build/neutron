-- pgvector extension bootstrap for the nbench database.
-- Mounted at /docker-entrypoint-initdb.d/00-vector.sql through the
-- read-only volume nbench-pgvec-init (see runner start_system).
CREATE EXTENSION IF NOT EXISTS vector;
