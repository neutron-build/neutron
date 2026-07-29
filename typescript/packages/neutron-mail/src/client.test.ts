import assert from "node:assert/strict";
import { test } from "node:test";

import { createMailClient, MailError } from "./client.js";
import { createMailTools } from "./tools.js";

/** A fetch stand-in that records calls and replays queued responses. */
function stubFetch(responses: Array<{ status?: number; body: unknown; json?: boolean }>) {
  const calls: Array<{ url: string; init?: RequestInit }> = [];
  let i = 0;

  const fn = async (url: string | URL | Request, init?: RequestInit) => {
    calls.push({ url: String(url), init });
    const r = responses[Math.min(i++, responses.length - 1)];
    const status = r.status ?? 200;
    return {
      ok: status >= 200 && status < 300,
      status,
      statusText: "",
      json: async () => {
        if (r.json === false) throw new Error("not json");
        return r.body;
      },
    } as unknown as Response;
  };

  return { fn: fn as unknown as typeof globalThis.fetch, calls };
}

test("search sends the query and unwraps the message list", async () => {
  const { fn, calls } = stubFetch([{ body: { messages: [{ ID: "m1", Subject: "hi" }] } }]);
  const client = createMailClient({ baseUrl: "http://svc", fetch: fn });

  const results = await client.search("acct-1", "quarterly numbers", 10);

  assert.equal(results.length, 1);
  assert.equal(results[0].ID, "m1");
  assert.match(calls[0].url, /\/v1\/accounts\/acct-1\/search\?/);
  assert.match(calls[0].url, /q=quarterly\+numbers/);
  assert.match(calls[0].url, /limit=10/);
});

test("a null list decodes as an empty array", async () => {
  // Go marshals an empty slice as null, so every list endpoint can return it.
  // Leaking that through would make callers crash on .map.
  const { fn } = stubFetch([{ body: { messages: null } }]);
  const client = createMailClient({ baseUrl: "http://svc", fetch: fn });

  const results = await client.search("acct-1", "nothing matches");
  assert.deepEqual(results, []);
});

test("account and message ids are URL encoded", async () => {
  const { fn, calls } = stubFetch([{ body: { ID: "x" } }]);
  const client = createMailClient({ baseUrl: "http://svc", fetch: fn });

  // Header-derived identities contain base64url characters, and native ones
  // contain colons — both must survive the path.
  await client.message("acct/one", "h:abc+def/ghi");

  assert.match(calls[0].url, /acct%2Fone/);
  assert.match(calls[0].url, /h%3Aabc%2Bdef%2Fghi/);
});

test("a problem document becomes a MailError", async () => {
  const { fn } = stubFetch([
    { status: 401, body: { title: "Reauthentication Required", detail: "grant revoked" } },
  ]);
  const client = createMailClient({ baseUrl: "http://svc", fetch: fn });

  await assert.rejects(
    () => client.search("acct-1", "anything"),
    (err: unknown) => {
      assert.ok(err instanceof MailError);
      assert.equal(err.status, 401);
      assert.equal(err.needsReauth, true);
      assert.equal(err.rateLimited, false);
      assert.match(err.message, /grant revoked/);
      return true;
    },
  );
});

test("a non-JSON error body still yields the status", async () => {
  // A proxy returning HTML on a 502 must not surface as a parse error that
  // hides what actually happened.
  const { fn } = stubFetch([{ status: 502, body: null, json: false }]);
  const client = createMailClient({ baseUrl: "http://svc", fetch: fn });

  await assert.rejects(
    () => client.accounts(),
    (err: unknown) => {
      assert.ok(err instanceof MailError);
      assert.equal(err.status, 502);
      return true;
    },
  );
});

test("rate limiting is distinguishable from reauth", async () => {
  const { fn } = stubFetch([{ status: 429, body: { title: "Rate Limited" } }]);
  const client = createMailClient({ baseUrl: "http://svc", fetch: fn });

  await assert.rejects(
    () => client.sync("acct-1"),
    (err: unknown) => {
      assert.ok(err instanceof MailError);
      assert.equal(err.rateLimited, true);
      assert.equal(err.needsReauth, false);
      return true;
    },
  );
});

test("a trailing slash on the base URL does not double up", async () => {
  const { fn, calls } = stubFetch([{ body: { accounts: [] } }]);
  const client = createMailClient({ baseUrl: "http://svc///", fetch: fn });

  await client.accounts();
  assert.equal(calls[0].url, "http://svc/v1/accounts");
});

test("a bearer token is attached when configured", async () => {
  const { fn, calls } = stubFetch([{ body: { accounts: [] } }]);
  const client = createMailClient({ baseUrl: "http://svc", token: "secret", fetch: fn });

  await client.accounts();
  const headers = calls[0].init?.headers as Record<string, string>;
  assert.equal(headers.authorization, "Bearer secret");
});

test("read-only tools omit every mutation", async () => {
  const { fn } = stubFetch([{ body: { messages: [] } }]);
  const client = createMailClient({ baseUrl: "http://svc", fetch: fn });

  const tools = createMailTools({ client, account: "acct-1" });
  const names = tools.map((t) => t.name);

  assert.ok(names.includes("mail_search"));
  assert.ok(names.includes("mail_read"));
  assert.ok(!names.includes("mail_mark"), "mutations must be opt-in");
  assert.ok(!names.includes("mail_move"), "mutations must be opt-in");
});

test("mutating tools require approval when enabled", async () => {
  const { fn } = stubFetch([{ body: { applied: 1 } }]);
  const client = createMailClient({ baseUrl: "http://svc", fetch: fn });

  const tools = createMailTools({ client, account: "acct-1", allowMutations: true });
  const mark = tools.find((t) => t.name === "mail_mark");

  assert.ok(mark, "mail_mark should exist when mutations are allowed");
  assert.equal(mark.needsApproval, true, "a real mailbox change must be a human decision");
});

test("search results are trimmed to what a model can use", async () => {
  const { fn } = stubFetch([
    {
      body: {
        messages: [
          {
            ID: "m1",
            ThreadID: "t1",
            Subject: "Quarterly numbers",
            From: [{ name: "Alice", email: "alice@example.com" }],
            To: [{ email: "bob@example.com" }],
            Preview: "Here are the figures",
            ReceivedAt: "2026-07-28T10:00:00Z",
            Keywords: { seen: false, flagged: true, draft: false, answered: false },
            HasAttachment: true,
            Fingerprint: "m:abc",
            MessageIDHeader: "<x@y>",
          },
        ],
      },
    },
  ]);
  const client = createMailClient({ baseUrl: "http://svc", fetch: fn });
  const tools = createMailTools({ client, account: "acct-1" });
  const search = tools.find((t) => t.name === "mail_search");
  assert.ok(search);

  const result = (await search.execute({ query: "numbers" })) as {
    count: number;
    messages: Array<Record<string, unknown>>;
  };

  assert.equal(result.count, 1);
  const m = result.messages[0];
  assert.equal(m.from, "Alice");
  assert.equal(m.unread, true, "seen:false should surface as unread");
  assert.equal(m.flagged, true);
  // Engine-internal fields are not worth a model's context.
  assert.equal(m.Fingerprint, undefined);
  assert.equal(m.MessageIDHeader, undefined);
});

test("send posts the message and returns its id", async () => {
  const { fn, calls } = stubFetch([{ body: { message_id: "<new@example.com>" } }]);
  const client = createMailClient({ baseUrl: "http://svc", fetch: fn });

  const id = await client.send("acct-1", {
    to: [{ email: "bob@example.com" }],
    subject: "Hello",
    text: "body",
  });

  assert.equal(id, "<new@example.com>");
  assert.match(calls[0].url, /\/v1\/accounts\/acct-1\/send$/);
  assert.equal(calls[0].init?.method, "POST");
  const sent = JSON.parse(String(calls[0].init?.body));
  assert.equal(sent.to[0].email, "bob@example.com");
});

test("mail_send requires a recipient for a new message", async () => {
  // A reply derives its recipient from the parent, but a fresh message with
  // no `to` would otherwise be rejected only after a round trip.
  const { fn } = stubFetch([{ body: { message_id: "<x>" } }]);
  const client = createMailClient({ baseUrl: "http://svc", fetch: fn });
  const tools = createMailTools({ client, account: "acct-1", allowMutations: true });
  const send = tools.find((t) => t.name === "mail_send");
  assert.ok(send);

  await assert.rejects(() => send.execute({ body: "no recipient" }), /recipient/);
});

test("mail_send allows a reply without an explicit recipient", async () => {
  const { fn, calls } = stubFetch([{ body: { message_id: "<reply@x>" } }]);
  const client = createMailClient({ baseUrl: "http://svc", fetch: fn });
  const tools = createMailTools({ client, account: "acct-1", allowMutations: true });
  const send = tools.find((t) => t.name === "mail_send");
  assert.ok(send);

  const result = (await send.execute({
    body: "sounds good",
    reply_to_message_id: "h:abc",
  })) as { sent: boolean };

  assert.equal(result.sent, true);
  const sent = JSON.parse(String(calls[0].init?.body));
  assert.equal(sent.reply_to_message_id, "h:abc");
});

test("mail_send requires approval", async () => {
  const { fn } = stubFetch([{ body: {} }]);
  const client = createMailClient({ baseUrl: "http://svc", fetch: fn });
  const tools = createMailTools({ client, account: "acct-1", allowMutations: true });
  const send = tools.find((t) => t.name === "mail_send");

  assert.ok(send);
  assert.equal(send.needsApproval, true, "sending mail must be a human decision");
});

test("the search limit is capped by maxResults", async () => {
  const { fn, calls } = stubFetch([{ body: { messages: [] } }]);
  const client = createMailClient({ baseUrl: "http://svc", fetch: fn });
  const tools = createMailTools({ client, account: "acct-1", maxResults: 5 });
  const search = tools.find((t) => t.name === "mail_search");
  assert.ok(search);

  await search.execute({ query: "anything", limit: 500 });
  assert.match(calls[0].url, /limit=5/);
});
