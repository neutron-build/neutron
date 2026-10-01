// API wire types come from the reviewed static spec; UI state remains local.
import type { Project, Document, Detail } from "./api-types";
export type { Project, Document, Job, Result, Detail } from "./api-types";
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
