import type { ActionArgs, LoaderArgs } from "@neutron-build/core";
import { load, submit } from "../api.server";
import type { Document, Outcome, Page } from "../models";
export const config = { mode: "app" };
export const headers = () => ({ "Cache-Control": "no-store", "Pragma": "no-cache" });
export async function loader({ request }: LoaderArgs) { return load(request); }
export async function action({ request }: ActionArgs) { return submit(request); }
function exactNote(note: string | null) { return note === null ? "NULL" : JSON.stringify(note); }
function Values({ doc }: {
    doc: Document;
}) { return <dl class="values"><dt>Amount</dt><dd data-amount={doc.amount}>{doc.amount}</dd><dt>Version</dt><dd data-version={doc.version}>{doc.version}</dd><dt>Note</dt><dd data-note={exactNote(doc.note)}>{exactNote(doc.note)}</dd><dt>Payload (base64)</dt><dd data-payload={doc.payload}><code>{JSON.stringify(doc.payload)}</code></dd><dt>Created (UTC micros)</dt><dd>{doc.created_at}</dd></dl>; }
export default function Home({ data, actionData }: {
    data?: Page;
    actionData?: Outcome;
}) {
    if (!data)
        return <p>Loading reference data.</p>;
    const draft = actionData?.draft ?? data.draft;
    return <main><h1>Neutron data reference</h1><p class="muted">Local operator fixture. One private server principal; no public multiuser sign-in.</p>
 {actionData && <p role="status" class={actionData.kind} data-outcome={actionData.kind}>{actionData.message}</p>}
 <section><h2>Projects</h2><ul>{data.projects.map(p => <li key={p.id}><a href={"/?project_id=" + p.id}>{p.title}</a> <code>{p.id}</code></li>)}</ul>
 {data.projects.length === 50 && <a href={"/?projects_after=" + data.projects.at(-1)!.id}>Next project page</a>}
 <form method="post"><input type="hidden" name="intent" value="project"/><label>Project UUID<input name="id" value={actionData?.projectDraft?.id ?? data.projectID} required/></label><label>Title<input name="title" value={actionData?.projectDraft?.title ?? ""} required/></label><button>Create project</button></form></section>
 {data.project && <section><h2>Documents</h2><p>Project <code>{data.project}</code></p>
 {data.documents.map(doc => <article key={doc.id}><h3><a href={"/?project_id=" + data.project + "&document_id=" + doc.id}>{doc.id}</a></h3><Values doc={doc}/><details><summary>Content</summary><pre>{doc.content}</pre></details>
 <form method="post"><input type="hidden" name="intent" value="note"/><input type="hidden" name="id" value={doc.id}/><input type="hidden" name="expected_version" value={actionData?.noteDraft?.id === doc.id ? actionData.noteDraft.expected_version : doc.version}/><p>Compare and swap expected version <code>{actionData?.noteDraft?.id === doc.id ? actionData.noteDraft.expected_version : doc.version}</code></p><label>Note kind<select name="note_kind" value={actionData?.noteDraft?.id === doc.id ? actionData.noteDraft.note_kind : doc.note === null ? "null" : "text"}><option value="null">NULL</option><option value="text">Text (empty allowed)</option></select></label><label>Note<input name="note" value={actionData?.noteDraft?.id === doc.id ? actionData.noteDraft.note : doc.note ?? ""}/></label><button>Update note once</button><a href={"/?project_id=" + data.project + "&document_id=" + doc.id}>Inspect fresh state / expected version</a></form></article>)}
 {data.documents.length === 50 && <a href={"/?project_id=" + data.project + "&documents_after=" + data.documents.at(-1)!.id}>Next document page</a>}
 <h3>Create or explicitly replay</h3><p>Keep the UUID, key and payload unchanged to replay. A new key is a new request. Amount and version never pass through floating point.</p>
 <form method="post"><input type="hidden" name="intent" value="document"/>{(["id", "project_id", "amount", "payload", "idempotency_key"] as const).map(key => <label key={key}>{key}<input name={key} value={draft[key]} required={key !== "payload"}/></label>)}<label>Content (up to 65536 UTF-8 bytes)<textarea name="content" value={draft.content}/></label><label>Note kind<select name="note_kind" value={draft.note_kind}><option value="null">NULL</option><option value="text">Text (empty allowed)</option></select></label><label>Note<input name="note" value={draft.note}/></label><button>Submit / replay once</button></form></section>}
 {data.detail && <section><h2>Processing detail</h2><code>{data.detail.id}</code><Values doc={data.detail}/><h3>Job</h3><pre data-job>{JSON.stringify(data.detail.job, null, 2)}</pre><h3>Result</h3><pre data-result>{JSON.stringify(data.detail.result, null, 2)}</pre><p><a href={"/?project_id=" + data.detail.project_id + "&document_id=" + data.detail.id}>Refresh durable state</a></p></section>}
 </main>;
}
