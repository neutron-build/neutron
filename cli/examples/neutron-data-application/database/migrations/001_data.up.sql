CREATE TABLE public.app_schema_revision (
    singleton boolean PRIMARY KEY DEFAULT true CHECK (singleton),
    revision integer NOT NULL CHECK (revision > 0)
);
INSERT INTO public.app_schema_revision(singleton, revision) VALUES (true, 1);

CREATE TABLE public.projects (
    tenant_id text NOT NULL,
    id uuid NOT NULL,
    title text NOT NULL,
    PRIMARY KEY (tenant_id, id)
);
CREATE TABLE public.documents (
    tenant_id text NOT NULL,
    id uuid NOT NULL,
    project_id uuid NOT NULL,
    content text NOT NULL CHECK (octet_length(content) <= 65536),
    amount numeric(40,18) NOT NULL CHECK (amount NOT IN ('NaN'::numeric, 'Infinity'::numeric, '-Infinity'::numeric)),
    note text,
    payload bytea NOT NULL,
    version bigint NOT NULL DEFAULT 1 CHECK (version > 0),
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (tenant_id, id),
    FOREIGN KEY (tenant_id, project_id) REFERENCES public.projects(tenant_id, id)
);
CREATE INDEX documents_project_page ON public.documents(tenant_id, project_id, id);
CREATE TABLE public.processing_requests (
    tenant_id text NOT NULL,
    idempotency_key text NOT NULL CHECK (length(idempotency_key) BETWEEN 1 AND 128),
    document_id uuid NOT NULL,
    payload_digest text NOT NULL CHECK (payload_digest ~ '^[0-9a-f]{64}$'),
    PRIMARY KEY (tenant_id, idempotency_key),
    UNIQUE (tenant_id, document_id),
    FOREIGN KEY (tenant_id, document_id) REFERENCES public.documents(tenant_id, id)
);
CREATE TABLE public.jobs (
    tenant_id text NOT NULL,
    document_id uuid NOT NULL,
    status text NOT NULL DEFAULT 'pending' CHECK (status IN ('pending','processing','done','failed')),
    attempts integer NOT NULL DEFAULT 0 CHECK (attempts BETWEEN 0 AND 3),
    claim_token uuid,
    lease_until timestamptz,
    failure_code text,
    PRIMARY KEY (tenant_id, document_id),
    FOREIGN KEY (tenant_id, document_id) REFERENCES public.processing_requests(tenant_id, document_id),
    CHECK ((status = 'processing' AND claim_token IS NOT NULL AND lease_until IS NOT NULL)
        OR (status <> 'processing' AND claim_token IS NULL AND lease_until IS NULL))
);
CREATE INDEX jobs_claim ON public.jobs(tenant_id, status, lease_until, document_id);
CREATE TABLE public.results (
    tenant_id text NOT NULL,
    document_id uuid NOT NULL,
    content_digest text NOT NULL CHECK (content_digest ~ '^[0-9a-f]{64}$'),
    word_count integer NOT NULL CHECK (word_count >= 0),
    processed_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (tenant_id, document_id),
    FOREIGN KEY (tenant_id, document_id) REFERENCES public.documents(tenant_id, id)
);
ALTER TABLE public.projects ENABLE ROW LEVEL SECURITY;
ALTER TABLE public.projects FORCE ROW LEVEL SECURITY;
ALTER TABLE public.documents ENABLE ROW LEVEL SECURITY;
ALTER TABLE public.documents FORCE ROW LEVEL SECURITY;
ALTER TABLE public.processing_requests ENABLE ROW LEVEL SECURITY;
ALTER TABLE public.processing_requests FORCE ROW LEVEL SECURITY;
ALTER TABLE public.jobs ENABLE ROW LEVEL SECURITY;
ALTER TABLE public.jobs FORCE ROW LEVEL SECURITY;
ALTER TABLE public.results ENABLE ROW LEVEL SECURITY;
ALTER TABLE public.results FORCE ROW LEVEL SECURITY;
