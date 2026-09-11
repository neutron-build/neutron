# Open audit items

Unresolved findings for this repository from the ChatGPT-led audit series (2026-09-09 through 2026-09-11, passes 1-5; register: teploy-neutron-lullmail expanded audit). Every P0/P1 finding has been fixed and verified; the items below are the remaining P2/P3 tail plus one item needing validation. Fields are quoted from the audit register; line references point at the review commits listed per item where recorded.

Open items: 18 P2 (18 total)

## neutron-build__neutron-02 - P2 - Open

**Return a failure status when an explicitly requested upgrade cannot check for updates**

- Kind: Confirmed from source
- Evidence: runUpgrade handles CheckForUpdate errors by displaying a warning and returning nil.
- Impact: Scripts cannot distinguish a successful latest-version check from a network, authorization or release-metadata failure.
- Proposed fix: Return a wrapped error for an explicit upgrade command; reserve best-effort warnings for optional startup update notifications.
- Acceptance test: Inject a failed update check and assert nonzero process status and no already-current/success indication.
- Review commit: `72492da8458a01efb4c4abd3864b0e19acf05d19` (last reviewed 2026-09-10)

## neutron-build__neutron-03 - P2 - Open improvement

**Make migration cancellation, time budgets and rollback arguments explicit**

- Kind: Improvement
- Evidence: Migration handlers derive timeouts from context.Background rather than cmd.Context, impose one fixed 60-second budget across the entire batch, and parse rollback count with fmt.Sscanf rather than strict full-string numeric validation.
- Impact: Caller cancellation is not propagated through the command context; large legitimate batches can hit an inflexible timeout, and an integer-prefix argument can be accepted unexpectedly.
- Proposed fix: Use cmd.Context, expose a documented timeout policy, parse counts with strconv.Atoi and validate bounds, and distinguish an interrupted partial batch from an untouched one.
- Acceptance test: Cancel the command context during a query, test a long valid migration with an explicit timeout override, and reject rollback counts such as 1junk.
- Review commit: `72492da8458a01efb4c4abd3864b0e19acf05d19` (last reviewed 2026-09-10)

## neutron-build__neutron-05 - P2 - Open

**Migration names can escape the selected migrations directory**

- Kind: Source-confirmed
- Evidence: safeName only lowercases and replaces spaces. Path separators and parent-directory components remain and are passed through filepath.Join into os.WriteFile. The name part/../../escape writes into the parent of the chosen migrations directory.
- Impact: An accidental or externally supplied path-like migration name can create or truncate files outside --dir. This is a local command-input integrity issue; no remote attack path is claimed. The diagnostic writes only within its own temporary root.
- Proposed fix: Require a nonempty slug from a conservative character set, reject separators and dot components, check path containment, and use exclusive creation. Treat filename and descriptive migration title as separate fields.
- Acceptance test: Test slash/backslash names, parent components, empty names and normal descriptive names. No rejected input may create files outside the selected directory.
- Review commit: `72492da8458a01efb4c4abd3864b0e19acf05d19` (last reviewed 2026-09-10)

## neutron-build__neutron-06 - P2 - Open

**Migration ordering breaks once versions exceed the padding width**

- Kind: Source-confirmed
- Evidence: Creation uses a minimum three-character decimal format, but readMigrationFilesWithSuffix sorts Version strings lexicographically. Consequently 1000 sorts before 999, and reverse rollback order is wrong as well.
- Impact: Once a repository crosses that boundary, migration application and rollback can violate numeric dependency order. Mixed-width imported versions have the same problem earlier.
- Proposed fix: Parse and validate versions once, sort by their numeric value, and preserve the original textual value only as the stored identifier. Alternatively define and enforce a fixed-width/timestamp scheme consistently, with a safe migration path for existing files.
- Acceptance test: Assert forward and reverse ordering for 998, 999, 1000 and 1001, plus mixed-width and duplicate numeric representations.
- Review commit: `72492da8458a01efb4c4abd3864b0e19acf05d19` (last reviewed 2026-09-10)

## neutron-build__neutron-07 - P2 - Open

**Migration status hides applied migrations whose source file is missing**

- Kind: Source-confirmed
- Evidence: MigrationStatuses builds appliedMap from the database but emits rows only by iterating local up files. Any applied version absent from the current checkout disappears from the status result.
- Impact: A checkout/schema mismatch can look healthy or empty despite the database containing migrations that cannot be inspected or rolled back from the available files. This is distinct from the earlier rollback-selection finding.
- Proposed fix: Build status from the union of database records and local versions. Mark applied-but-missing and conflicting-name/checksum states explicitly, and reject unsafe migration operations when the histories do not match.
- Acceptance test: Provide database versions 001/002 with only 001 locally and assert that 002 is displayed as applied-but-missing rather than omitted.
- Review commit: `72492da8458a01efb4c4abd3864b0e19acf05d19` (last reviewed 2026-09-10)

## neutron-build__neutron-08 - P2 - Open

**Windows self-update selects ZIP files but only implements gzip/tar extraction**

- Kind: Source-confirmed
- Evidence: DownloadAndReplace selects an archive extension of zip on Windows. It then unconditionally calls extractBinary, which begins with gzip.NewReader and has no ZIP branch.
- Impact: A valid Windows release ZIP is rejected before installation. The same extractor rejected a synthetic valid ZIP in the Linux diagnostic; the Windows executable replacement itself was not tested.
- Proposed fix: Dispatch extraction by the supported archive format and validate the chosen binary entry. Add native Windows upgrade tests rather than relying only on archive-name selection.
- Acceptance test: Extract real-shaped Windows ZIP and Unix tar.gz fixtures; verify the correct executable and errors for empty/wrong-format archives. Run the installation flow on Windows CI.
- Review commit: `72492da8458a01efb4c4abd3864b0e19acf05d19` (last reviewed 2026-09-10)

## neutron-build__neutron-10 - P2 - Open

**Self-update version comparison ignores prerelease precedence and corrupts build metadata**

- Kind: Source-confirmed
- Evidence: parseSemver removes the prerelease suffix, then accumulates every digit in each dot segment rather than parsing validated numeric identifiers. Thus 1.2.3-rc.1 compares equal to 1.2.3, and digits in 1.2.3+build.7 alter the patch number.
- Impact: Updates can be incorrectly offered or skipped. A release candidate may not upgrade to its final release, and build metadata changes ordering even though the SemVer contract says it must not.
- Proposed fix: Use a tested SemVer parser/comparator and define whether prereleases are eligible. Validate release tags and current-version strings instead of converting malformed components into plausible numbers.
- Acceptance test: Cover prerelease-to-final, numeric/alphanumeric prerelease identifiers, build metadata, leading v, malformed tags and large version components.
- Review commit: `72492da8458a01efb4c4abd3864b0e19acf05d19` (last reviewed 2026-09-10)

## neutron-build__neutron-11 - P2 - Open

**Self-update artifact and checksum downloads have no explicit bounds**

- Kind: Source-confirmed
- Evidence: CheckForUpdate uses a ten-second client, but downloadToTemp and verifyChecksum use http.Get on the default client. The archive copy and checksum io.ReadAll have no explicit byte limits or operation context; the checksum response status is not checked.
- Impact: An update can stall indefinitely or consume unbounded disk/memory on an abnormal upstream response, even though the initial version check was bounded. This is not evidence that a trusted release service is compromised.
- Proposed fix: Use one context-aware HTTP client with stage deadlines and explicit archive/checksum size caps. Require successful status codes, close/sync local files with checked errors, and remove temporary files after all failure paths.
- Acceptance test: Use local test servers that stall after headers, return oversized bodies, terminate mid-transfer or return non-200 checksum pages. Verify bounded failure and cleanup.
- Review commit: `72492da8458a01efb4c4abd3864b0e19acf05d19` (last reviewed 2026-09-10)

## neutron-build__neutron-12 - P2 - Open

**SMTP submission ignores the caller context**

- Kind: Source-confirmed
- Evidence: Sender.Send accepts context.Context but never checks or passes it to any operation. It invokes smtp.SendMail, which does not expose a context parameter. Lullmail vendors the same send.go Git blob and calls this sender from its nominally timed send queue.
- Impact: Canceling a send or reaching its deadline does not interrupt SMTP connection/read/write work. A stalled SMTP server can outlive the application send timeout. This shared defect is counted once here, with Lullmail listed as an affected consumer.
- Proposed fix: Implement submission over a context-aware dial and a connection with read/write deadlines and cancellation-driven close. Define the ambiguous-after-DATA outcome before adding automatic retries.
- Acceptance test: With a loopback fake SMTP server, stall at greeting, TLS, authentication, recipient acceptance and final DATA acknowledgment. Each canceled operation must terminate within a bounded grace period.
- Review commit: `72492da8458a01efb4c4abd3864b0e19acf05d19` (last reviewed 2026-09-10)

## neutron-build__neutron-13 - P2 - Open

**ASCII display names with commas produce malformed address headers**

- Kind: Source-confirmed
- Evidence: formatAddress applies mime.QEncoding.Encode to the display name and concatenates it with the address. QEncoding leaves ordinary ASCII text such as Doe, Jane unchanged; the resulting header is not quoted as an RFC address phrase.
- Impact: A normal display name containing a comma generates an invalid or differently parsed From/To/Cc header. The emitted value failed Go net/mail.ParseAddressList in the isolated check. The identical renderer is vendored by Lullmail.
- Proposed fix: Use a proper address formatter such as net/mail.Address.String, with validation and control-character rejection at the composition boundary. Preserve Unicode support without hand-building address syntax.
- Acceptance test: Round-trip names containing commas, quotes, backslashes, parentheses and Unicode through a standards-aware parser. Assert one intended mailbox per Address value.
- Review commit: `72492da8458a01efb4c4abd3864b0e19acf05d19` (last reviewed 2026-09-10)

## neutron-build__neutron-14 - P2 - Open

**Redis path-index TTL can expire before its cached entries**

- Kind: Source-confirmed
- Evidence: The app and loader set methods write the entry, add its key to the shared pathname index, then EXPIRE that index to max(the new entry TTL, 60). A short-lived variant written after a long-lived variant shortens the shared index lifetime; the live long-lived variant then has no index membership available to deleteByPath.
- Impact: A successful path invalidation can leave stale HTML or loader data serving until its individual expiry. This requires mixed lifetimes on one pathname, not merely any cache write.
- Proposed fix: Maintain the pathname index until at least the latest member expiry, atomically with entry/index updates, or use an invalidation generation scheme. Do not replace a longer remaining index lifetime with a shorter one. Keep app and loader semantics aligned.
- Acceptance test: For both cache types: write a 600-second variant and a 10-second variant for one pathname, advance 61 seconds, invalidate the pathname, and assert the long-lived variant is absent while other paths are unchanged.
- Review commit: `72492da8458a01efb4c4abd3864b0e19acf05d19` (last reviewed 2026-09-10)

## neutron-build__neutron-15 - P2 - Open

**Concurrent cache writes and path invalidation can orphan index membership**

- Kind: Source-confirmed
- Evidence: deleteIndexedPathKeys performs SMEMBERS, DEL members, then DEL index as separate awaited commands. A writer can add a new variant between the first and last commands. The new entry survives while its index is removed, so another later invalidation still cannot discover it.
- Impact: Cache invalidation may permanently lose track of a live entry until its own TTL. The finding does not assume that a concurrent later write must be removed by the first invalidation; failure of the subsequent invalidation is the unambiguous defect.
- Proposed fix: Use atomic entry/index mutation and atomic invalidation, or generation-based keys with a documented concurrency contract. An in-process mutex alone is insufficient for multiple framework instances sharing Redis.
- Acceptance test: Use barriers around SMEMBERS and the final index deletion. Insert a second variant in between, then issue another complete deleteByPath. Assert no variant remains, for app and loader caches; preserve unrelated paths.
- Review commit: `72492da8458a01efb4c4abd3864b0e19acf05d19` (last reviewed 2026-09-10)

## neutron-build__neutron-16 - P2 - Open

**Bearer transport adds authorization again after cross-origin redirects**

- Kind: Source-confirmed conditional defect
- Evidence: bearerClient uses an http.Client without a redirect-origin restriction. bearerTransport.RoundTrip clones every request and unconditionally sets its stored bearer token, including a redirected request to a different origin. This occurs below the Client redirect-header filtering layer. Lullmail vendors equivalent transport logic.
- Impact: If a provider request is redirected outside the intended provider origin, the credential is attached to that destination. This is a conditional transport defect: no live provider redirect, token compromise, or exploitability in a deployed service was demonstrated.
- Proposed fix: Bind credentials to explicitly permitted HTTPS origins and reject redirects outside that boundary, including HTTPS-to-HTTP downgrades. Keep any legitimate provider-origin exceptions narrowly enumerated. Apply the fix to Neutron and Lullmail’s vendored copy, counted as one shared finding.
- Acceptance test: With a synthetic RoundTripper and dummy token, test same-origin redirects, cross-origin redirects, and downgrades. Only explicitly allowed HTTPS origins may receive the bearer; enforce a redirect-count bound.
- Review commit: `72492da8458a01efb4c4abd3864b0e19acf05d19` (last reviewed 2026-09-10)

## neutron-build__neutron-18 - P2 - Open

**Reject or honor response Vary fields not represented in the Go cache key**

- Kind: Source-confirmed
- Evidence: responseIsCacheable rejects Vary values mentioning Cookie or Authorization only. It accepts Vary: * and Accept-Language/Accept-Encoding when those headers were not declared in WithVaryHeaders. The cache key contains only explicitly configured vary headers, so an emitted response Vary does not otherwise select a representation.
- Impact: A response can be replayed to a request with a different language or encoding preference. Vary: * can also be replayed without revalidation. Explicitly configuring every vary header avoids some cases, but does not handle the wildcard. This is separate from the missing authority dimension in N-17.
- Proposed fix: Parse all Vary field values. Reject caching for Vary: * and for any nominated field not safely represented in the key, or implement response-driven variant metadata. Keep Cookie/Authorization safeguards. Document WithVaryHeaders as an additional key declaration rather than permission to ignore conflicting response headers.
- Acceptance test: Without explicit vary options, serve English then French requests to a Vary: Accept-Language handler and verify correct bodies or misses. Vary: * must never be replayed as a reusable shared entry. Repeat with Accept-Encoding and multiple Vary field lines.
- Review commit: `1549c4a7f239e8f3a53d869acdfa5d0c617ab8b9` (last reviewed 2026-09-10)

## neutron-build__neutron-19 - P2 - Open improvement

**Make Go HTTP-cache freshness overrides explicit and safe by default**

- Kind: Improvement
- Evidence: The request predicate does not inspect request Cache-Control. The response predicate rejects no-store/private but accepts no-cache and max-age=0; the cache stores accepted entries using the middleware TTL. A configured one-minute TTL can therefore replay those responses without invoking the origin or processing a client refresh request.
- Impact: Routes or clients using normal HTTP freshness controls can observe stale bytes until the middleware TTL. This is recorded as a contract-hardening improvement, not a claim that every application cache must implement the entire HTTP cache standard: an intentional application-level override needs an explicit, documented contract.
- Proposed fix: Default to honoring request revalidation intent and response no-cache/freshness bounds, or bypass when revalidation is unavailable. Make an application-authoritative TTL override a clearly named opt-in. Parse directives rather than treating the provided TTL as silently stronger than all freshness metadata.
- Acceptance test: With default policy, a response marked no-cache is revalidated or bypassed; max-age=0,must-revalidate does not become a fresh one-minute entry; a client no-cache refresh reaches the origin. Test any explicit override separately so the chosen application contract is visible.
- Review commit: `1549c4a7f239e8f3a53d869acdfa5d0c617ab8b9` (last reviewed 2026-09-10)

## neutron-build__neutron-20 - P2 - Open

**Preserve streaming capabilities through the Go HTTP-cache response wrapper**

- Kind: Source-confirmed
- Evidence: The cache wraps every eligible GET before the downstream response headers are known. responseRecorder embeds only http.ResponseWriter and implements Write/WriteHeader; it has no Flusher, Hijacker, or Unwrap method. A downstream streaming handler loses access to capabilities implemented by the underlying writer, even after setting Cache-Control: no-store.
- Impact: A downstream http.Flusher assertion fails, and ResponseController.Flush returns ErrNotSupported through this wrapper. Public SSE/streaming routes can fail or stop flushing unless callers manually exclude them before middleware entry. Unbounded stream capture is another reason not to cache a response after it begins streaming.
- Proposed fix: Preserve optional writer capabilities without falsely advertising unsupported ones, and provide an Unwrap path for ResponseController. Mark flushed/hijacked responses non-cacheable and stop accumulating their bodies; also support explicit pre-handler streaming-route exclusion. Unwrap alone does not fix handlers using direct http.Flusher assertions.
- Acceptance test: Use an underlying writer with Flush. A no-store streaming GET must flush before handler completion through both ResponseController and supported direct interfaces, and must create no cache entry. Cover ordinary finite responses and unsupported optional interfaces too.
- Review commit: `1549c4a7f239e8f3a53d869acdfa5d0c617ab8b9` (last reviewed 2026-09-10)

## neutron-build__neutron-22 - P2 - Open

**Normalize the exact mount root consistently with the mounted subtree**

- Kind: Source-supported defect/contract mismatch
- Evidence: Mount wraps fullPrefix+"/" in http.StripPrefix(fullPrefix, handler), but registers the exact fullPrefix directly to handler. The subhandler therefore sees different path namespaces for /service and /service/.
- Impact: A subrouter with a root-only route works at the slash-suffixed URL but returns 404 for the explicitly registered exact mount root; root-relative routing and canonical redirects can be inconsistent. This is separate from omitted middleware in N-21.
- Proposed fix: Define one canonical exact-root behavior: a method-preserving redirect to the slash-suffixed mount, or a normalized subrequest path of /. Reuse the same mounted handler and middleware chain and preserve query strings.
- Acceptance test: Mount a subrouter whose only root route is GET /{$}. The exact root must either serve that root or redirect to the correct external mount URL; following the redirect must succeed. Subpaths and query strings must remain correct.
- Review commit: `a7e7bb09d521497695df897a34794036b3861381` (last reviewed 2026-09-10)

## neutron-build__neutron-23 - P2 - Open

**Do not rewrite matched application 404/405 responses as router errors**

- Kind: Source-supported defect/contract mismatch
- Evidence: errInterceptor.WriteHeader rewrites any 404/405 whose Content-Type is not application/problem+json, without checking whether a registered application handler produced it. Write then reports success while discarding the handler body.
- Impact: Application JSON error codes, custom HTML not-found pages, and domain-specific error details disappear behind a generic no-route or method-not-supported response. Existing Content-Length and response headers also need reconciliation when any legitimate rewrite occurs; that secondary header path was not reproduced in this pass.
- Proposed fix: Limit fallback rendering to actual unmatched/method-mismatch routing outcomes. Preserve errors produced by matched handlers, including non-problem JSON and HTML; keep the default router problem format for genuine routing misses.
- Acceptance test: A matched handler returning custom JSON 404 and 405 must retain its status, content type and exact body. Genuine unmatched and method-mismatch requests must still have the intended problem shape and Allow header, without stale length headers.
- Review commit: `a7e7bb09d521497695df897a34794036b3861381` (last reviewed 2026-09-10)

## Residual from the nucleus A1-A25 fix series (2026-09-11)

- **nucleus-residual-01 (P2)** - Cluster routing covers the wire entry only. `execute_statements_with_session` (extended protocol, real drivers) now routes through the dispatch policy, but the embedded-API entries `execute_parsed` / `execute_prepared` still bypass cluster routing, replication, and follower-read checks. No wire client can reach them; embedded callers in configured cluster mode can. Fix: route both entries through the same dispatch gate as the wire path.
