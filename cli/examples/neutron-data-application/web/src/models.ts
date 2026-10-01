// Reviewed reference API wire shapes, not generated OpenAPI types.
export interface Project {
    tenant_id: string;
    id: string;
    title: string;
}
export interface Document {
    tenant_id: string;
    id: string;
    project_id: string;
    content: string;
    amount: string;
    note: string | null;
    payload: string;
    version: string;
    created_at: string;
}
export interface Job {
    status: string;
    attempts: number;
    failure_code: string | null;
    lease_until: string | null;
}
export interface Result {
    content_digest: string;
    word_count: number;
    processed_at: string;
}
export interface Detail extends Document {
    job: Job | null;
    result: Result | null;
}
export interface Draft {
    id: string;
    project_id: string;
    content: string;
    amount: string;
    note_kind: string;
    note: string;
    payload: string;
    idempotency_key: string;
}
export interface Outcome {
    status: number;
    message: string;
    kind: "success" | "conflict" | "unknown" | "error";
    draft?: Draft;
    projectDraft?: {
        id: string;
        title: string;
    };
    noteDraft?: {
        id: string;
        expected_version: string;
        note_kind: string;
        note: string;
    };
}
export interface Page {
    projects: Project[];
    documents: Document[];
    project: string;
    detail: Detail | null;
    draft: Draft;
    projectID: string;
}
