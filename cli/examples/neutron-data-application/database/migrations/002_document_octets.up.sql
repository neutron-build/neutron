-- Additive expansion: existing readers/writers need no new field.
ALTER TABLE public.documents ADD COLUMN content_octets pg_catalog.int4
    GENERATED ALWAYS AS (pg_catalog.octet_length(content)) STORED;
UPDATE public.app_schema_revision SET revision = 2 WHERE singleton;
