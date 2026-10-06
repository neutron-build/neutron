import type { CreateProject, CreateDocument, UpdateNote } from "./api-types";
import { randomUUID } from "node:crypto";
import type { Project, Document, Detail, Draft, Outcome, Page } from "./models";
const loopback = new Set(["127.0.0.1", "localhost", "[::1]"]);
const uuidPattern = /^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$/;
export function requestOrigin(request: Request, mutation = false): URL {
    const url = new URL(request.url);
    if (url.protocol !== "http:" || !loopback.has(url.hostname))
        throw new Response("Loopback operator fixture only", { status: 403 });
    if (mutation && request.headers.get("origin") !== url.origin)
        throw new Response("Same-origin form required", { status: 403 });
    return url;
}
function id(value: string): string { if (!uuidPattern.test(value))
    throw new Error("A canonical UUID is required"); return value.toLowerCase(); }
function config() {
    const raw = process.env.NEUTRON_SERVICE_API_URL;
    const token = process.env.DATA_WEB_API_TOKEN;
    if (!raw || !token)
        throw new Error("Private operator API configuration is required");
    const api = new URL(raw);
    if (api.protocol !== "http:" || !loopback.has(api.hostname) || api.username || api.password || api.pathname !== "/" || api.search || api.hash)
        throw new Error("A loopback API origin is required");
    return { api: api.origin, token };
}
class APIError extends Error {
    constructor(readonly status: number, readonly unknown: boolean) { super("API request failed"); }
}
async function call<T>(path: string, body?: unknown): Promise<T> {
    const { api, token } = config();
    let response: Response;
    try {
        response = await fetch(api + path, { method: body === undefined ? "GET" : "POST", headers: { Authorization: `Bearer ${token}`, "Content-Type": "application/json" }, body: body === undefined ? undefined : JSON.stringify(body), signal: AbortSignal.timeout(7000), redirect: "manual", cache: "no-store" });
    }
    catch {
        throw new APIError(503, body !== undefined);
    }
    if (!response.ok) {
        let unknown = false;
        try {
            const problem = await response.json() as {
                type?: unknown;
            };
            unknown = typeof problem.type === "string" && problem.type.endsWith("/unknown-outcome");
        }
        catch { /* safe generic category */ }
        throw new APIError(response.status, unknown);
    }
    try {
        return await response.json() as T;
    }
    catch {
        throw new APIError(503, body !== undefined);
    }
}
export async function load(request: Request): Promise<Page> {
    const url = requestOrigin(request);
    const query = url.searchParams;
    const project = query.get("project_id") ? id(query.get("project_id")!) : "";
    const projectCursor = query.get("projects_after") ? id(query.get("projects_after")!) : "";
    const docCursor = query.get("documents_after") ? id(query.get("documents_after")!) : "";
    const fallback: Page = { projects: [], documents: [], project, detail: null, projectID: randomUUID(), draft: { id: randomUUID(), project_id: project, content: "", amount: "0.000000000000000000", note_kind: "null", note: "", payload: "", idempotency_key: randomUUID() } };
    try {
        const projects = await call<Project[]>("/api/projects" + (projectCursor ? "?after=" + projectCursor : ""));
        const documents = project ? await call<Document[]>("/api/documents?project_id=" + project + (docCursor ? "&after=" + docCursor : "")) : [];
        const detail = query.get("document_id") ? await call<Detail>("/api/documents/" + id(query.get("document_id")!)) : null;
        return { ...fallback, projects, documents, detail };
    } catch {
        // Rendering actionData must not depend on a successful follow-up read.
        // Empty fallback collections are unavailable data, never observed empty state.
        return { ...fallback, readError: "Current state is unavailable. Your submitted values below are preserved; a failed refresh does not establish whether a write committed. No write was retried. Keep these values and inspect durable state before another action." };
    }
}
function field(form: FormData, key: string): string { const values = form.getAll(key); if (values.length !== 1 || typeof values[0] !== "string")
    throw new Error("Required form value missing or repeated"); return values[0]; }
function nullable(form: FormData): string | null { const kind = field(form, "note_kind"); const value = field(form, "note"); if (kind !== "null" && kind !== "text")
    throw new Error("Choose NULL or text"); return kind === "null" ? null : value; }
export async function submit(request: Request): Promise<Outcome> {
    requestOrigin(request, true);
    let draft: Draft | undefined;
    let projectDraft: Outcome["projectDraft"];
    let noteDraft: Outcome["noteDraft"];
    let intent = "";
    try {
        if (request.headers.get("content-type")?.split(";")[0].trim() !== "application/x-www-form-urlencoded")
            throw new Error("URL-encoded form required");
        const encoded = await request.text();
        if (Buffer.byteLength(encoded) > 1 << 20)
            throw new Error("Form too large");
        const form = new FormData();
        for (const [key, value] of new URLSearchParams(encoded))
            form.append(key, value);
        intent = field(form, "intent");
        const allowed: Record<string, string[]> = { project: ["intent", "id", "title"], document: ["intent", "id", "project_id", "content", "amount", "note_kind", "note", "payload", "idempotency_key"], note: ["intent", "id", "expected_version", "note_kind", "note"] };
        if (!allowed[intent] || [...form.keys()].some(key => !allowed[intent].includes(key)))
            throw new Error("Unknown form field");
        if (intent === "project") {
            projectDraft = { id: id(field(form, "id")), title: field(form, "title") };
            const project = await call<Project>("/api/projects", projectDraft satisfies CreateProject);
            return { status: 200, kind: "success", projectDraft, message: `Project ${project.id} created` };
        }
        if (intent === "document") {
            draft = { id: id(field(form, "id")), project_id: id(field(form, "project_id")), content: field(form, "content"), amount: field(form, "amount"), note_kind: field(form, "note_kind"), note: field(form, "note"), payload: field(form, "payload"), idempotency_key: field(form, "idempotency_key") };
            const document = await call<Document>("/api/documents", { id: draft.id, project_id: draft.project_id, content: draft.content, amount: draft.amount, note: nullable(form), payload: draft.payload, idempotency_key: draft.idempotency_key } satisfies CreateDocument);
            return { status: 200, kind: "success", draft, message: `Created or replayed document ${document.id}; version ${document.version}` };
        }
        if (intent === "note") {
            noteDraft = { id: id(field(form, "id")), expected_version: field(form, "expected_version"), note_kind: field(form, "note_kind"), note: field(form, "note") };
            const document = await call<Document>("/api/documents/" + noteDraft.id + "/note", { expected_version: noteDraft.expected_version, note: nullable(form) } satisfies UpdateNote);
            return { status: 200, kind: "success", noteDraft, message: `Note updated; version ${document.version}` };
        }
        throw new Error("Unknown form intent");
    }
    catch (error) {
        if (error instanceof APIError) {
            const kind = error.unknown ? "unknown" : error.status === 409 ? "conflict" : "error";
            return { status: error.status, kind, draft, projectDraft, noteDraft, message: kind === "unknown" ? (intent === "document" ? "Outcome unknown. Keep this document ID, key and payload; inspect durable state before explicitly replaying the same request. No automatic retry occurred." : intent === "note" ? "Outcome unknown. Keep the submitted expected version and inspect the document before another action. No automatic retry occurred." : "Outcome unknown. Keep the submitted project UUID and inspect projects before another action. No automatic retry occurred.") : kind === "conflict" ? "Conflict. Inspect current state; do not silently replace the key or expected version." : `API refused the request (${error.status}). No automatic retry occurred.` };
        }
        return { status: 400, kind: "error", draft, projectDraft, noteDraft, message: "Invalid or missing form values. Nothing was automatically retried." };
    }
}
