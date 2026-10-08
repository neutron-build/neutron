// Complete-output parser and request-URL oracle. Uses only an existing compiler.
const fs = require("node:fs");
const assert = require("node:assert/strict");
const vm = require("node:vm");
const ts = require(process.argv[2]);
const source = fs.readFileSync(process.argv[3], "utf8");
const parsed = ts.createSourceFile("client.ts", source, ts.ScriptTarget.Latest, true);
assert.deepEqual(parsed.parseDiagnostics, []);
const result = ts.transpileModule(source, {
  compilerOptions: { target: ts.ScriptTarget.ES2022, module: ts.ModuleKind.CommonJS },
  reportDiagnostics: true,
});
assert.deepEqual(result.diagnostics, []);
const calls = [];
const context = {
  exports: {}, URLSearchParams,
  fetch: async (url, options) => { calls.push([url, options]); return { json: async () => [] }; },
};
vm.runInNewContext(result.outputText, context);
(async () => {
  await context.exports.getUserProfile();
  assert.equal(calls[0][0], "https://api.example.com/user-profile");
  await context.exports.literalRequest("a/b ?`\\", { "x-filter": "é /" });
  assert.equal(calls[1][0], "https://api.example.com" + "/literal/${1+1}/`/\\/" +
    encodeURIComponent("a/b ?`\\") + "?" + new URLSearchParams({ "x-filter": "é /" }));
  assert.equal(calls[1][1].method, "GET");
  await context.exports.getUsers({ required: 3, optional: undefined });
  assert.equal(calls[2][0], "https://api.example.com/users?required=3");
  await context.exports.postUsers(undefined, "body");
  assert.equal(calls[3][0], "https://api.example.com/users");
  assert.equal(calls[3][1].body, JSON.stringify("body"));
})().catch(error => { console.error(error); process.exitCode = 1; });
