-- Derived data only. Deploy consumers compatible with revision 1 before rollback.
ALTER TABLE public.documents DROP COLUMN content_octets;
UPDATE public.app_schema_revision SET revision = 1 WHERE singleton;
