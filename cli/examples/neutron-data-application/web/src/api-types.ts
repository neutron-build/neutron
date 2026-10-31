// Generated from ../api/openapi.json; run npm run generate:api. Do not edit.
export type CreateDocument = {
    amount: string;
    content: string;
    id: string;
    idempotency_key: string;
    note: string | null;
    payload: string;
    project_id: string;
};

export type CreateProject = {
    id: string;
    title: string;
};

export type Detail = {
    amount: string;
    content: string;
    created_at: string;
    id: string;
    job: Job | null;
    note: string | null;
    payload: string;
    project_id: string;
    result: Result | null;
    tenant_id: string;
    version: string;
};

export type Document = {
    amount: string;
    content: string;
    created_at: string;
    id: string;
    note: string | null;
    payload: string;
    project_id: string;
    tenant_id: string;
    version: string;
};

export type Job = {
    attempts: number;
    failure_code: string | null;
    lease_until: string | null;
    status: string;
};

export type Problem = {
    detail: string;
    instance?: string;
    status: number;
    title: string;
    type: string;
};

export type Project = {
    id: string;
    tenant_id: string;
    title: string;
};

export type Result = {
    content_digest: string;
    processed_at: string;
    word_count: number;
};

export type UpdateNote = {
    expected_version: string;
    note: string | null;
};
