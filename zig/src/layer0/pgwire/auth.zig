// Layer 0: SCRAM-SHA-256 authentication — zero heap allocation
//
// Implements the SASL SCRAM-SHA-256 mechanism for PostgreSQL authentication.
// RFC 5802 (SCRAM) + RFC 7677 (SCRAM-SHA-256).
// All operations use caller-provided stack buffers.

const std = @import("std");
const HmacSha256 = std.crypto.auth.hmac.sha2.HmacSha256;
const Sha256 = std.crypto.hash.sha2.Sha256;

pub const Error = error{
    InvalidServerResponse,
    AuthenticationFailed,
    BufferTooShort,
    InvalidBase64,
    ServerVerificationFailed,
};

/// Maximum sizes for SCRAM buffers.
pub const MAX_NONCE_LEN = 48;
pub const MAX_CLIENT_FIRST_LEN = 256;
pub const MAX_CLIENT_FINAL_LEN = 512;
pub const MAX_SERVER_FIRST_LEN = 512;
pub const MAX_SERVER_FINAL_LEN = 256;

/// SCRAM-SHA-256 client state machine.
pub const ScramClient = struct {
    // Client nonce (18 random bytes, base64-encoded = 24 chars)
    client_nonce: [24]u8 = undefined,
    client_nonce_len: usize = 0,

    // Values from server-first-message
    server_nonce: [MAX_NONCE_LEN]u8 = undefined,
    server_nonce_len: usize = 0,
    salt: [64]u8 = undefined,
    salt_len: usize = 0,
    iterations: u32 = 0,

    // Computed keys
    salted_password: [32]u8 = undefined,
    auth_message_buf: [1024]u8 = undefined,
    auth_message_len: usize = 0,

    // Client-first-message-bare (needed for auth message)
    client_first_bare_buf: [MAX_CLIENT_FIRST_LEN]u8 = undefined,
    client_first_bare_len: usize = 0,

    /// Generate the client-first-message.
    /// Returns the message as a slice of `out_buf`.
    pub fn clientFirstMessage(self: *ScramClient, username: []const u8, out_buf: []u8) Error![]const u8 {
        // Generate random nonce
        var nonce_bytes: [18]u8 = undefined;
        std.crypto.random.bytes(&nonce_bytes);
        const nonce_enc = std.base64.standard.Encoder;
        self.client_nonce_len = nonce_enc.calcSize(18);
        if (self.client_nonce_len > self.client_nonce.len) return Error.BufferTooShort;
        _ = nonce_enc.encode(&self.client_nonce, &nonce_bytes);

        // client-first-message = gs2-header, client-first-message-bare
        // gs2-header = "n,,"
        // client-first-message-bare = "n=<user>,r=<nonce>"
        var pos: usize = 0;

        // Store client-first-message-bare for auth message computation
        const bare_prefix = "n=";
        @memcpy(self.client_first_bare_buf[0..bare_prefix.len], bare_prefix);
        var bare_pos: usize = bare_prefix.len;
        @memcpy(self.client_first_bare_buf[bare_pos .. bare_pos + username.len], username);
        bare_pos += username.len;
        const bare_r = ",r=";
        @memcpy(self.client_first_bare_buf[bare_pos .. bare_pos + bare_r.len], bare_r);
        bare_pos += bare_r.len;
        @memcpy(self.client_first_bare_buf[bare_pos .. bare_pos + self.client_nonce_len], self.client_nonce[0..self.client_nonce_len]);
        bare_pos += self.client_nonce_len;
        self.client_first_bare_len = bare_pos;

        // Full message: "n,," + bare
        const header = "n,,";
        if (out_buf.len < header.len + bare_pos) return Error.BufferTooShort;
        @memcpy(out_buf[0..header.len], header);
        pos = header.len;
        @memcpy(out_buf[pos .. pos + bare_pos], self.client_first_bare_buf[0..bare_pos]);
        pos += bare_pos;

        return out_buf[0..pos];
    }

    /// Process the server-first-message and generate the client-final-message.
    pub fn processServerFirst(self: *ScramClient, server_first: []const u8, password: []const u8, out_buf: []u8) Error![]const u8 {
        // Parse server-first-message: r=<nonce>,s=<salt>,i=<iterations>
        var remaining = server_first;

        // r= (combined nonce)
        if (remaining.len < 2 or remaining[0] != 'r' or remaining[1] != '=')
            return Error.InvalidServerResponse;
        remaining = remaining[2..];
        const nonce_end = std.mem.indexOfScalar(u8, remaining, ',') orelse return Error.InvalidServerResponse;
        const combined_nonce = remaining[0..nonce_end];
        if (combined_nonce.len >= self.server_nonce.len) return Error.BufferTooShort;
        @memcpy(self.server_nonce[0..combined_nonce.len], combined_nonce);
        self.server_nonce_len = combined_nonce.len;
        remaining = remaining[nonce_end + 1 ..];

        // Verify server nonce starts with client nonce
        if (combined_nonce.len < self.client_nonce_len) return Error.InvalidServerResponse;
        if (!std.mem.eql(u8, combined_nonce[0..self.client_nonce_len], self.client_nonce[0..self.client_nonce_len]))
            return Error.InvalidServerResponse;

        // s= (base64-encoded salt)
        if (remaining.len < 2 or remaining[0] != 's' or remaining[1] != '=')
            return Error.InvalidServerResponse;
        remaining = remaining[2..];
        const salt_end = std.mem.indexOfScalar(u8, remaining, ',') orelse return Error.InvalidServerResponse;
        const salt_b64 = remaining[0..salt_end];
        self.salt_len = std.base64.standard.Decoder.calcSizeForSlice(salt_b64) catch return Error.InvalidBase64;
        if (self.salt_len > self.salt.len) return Error.BufferTooShort;
        std.base64.standard.Decoder.decode(self.salt[0..self.salt_len], salt_b64) catch return Error.InvalidBase64;
        remaining = remaining[salt_end + 1 ..];

        // i= (iterations)
        if (remaining.len < 2 or remaining[0] != 'i' or remaining[1] != '=')
            return Error.InvalidServerResponse;
        remaining = remaining[2..];
        self.iterations = std.fmt.parseInt(u32, remaining, 10) catch return Error.InvalidServerResponse;

        // Compute SaltedPassword = Hi(password, salt, iterations)
        self.salted_password = hi(password, self.salt[0..self.salt_len], self.iterations);

        // ClientKey = HMAC(SaltedPassword, "Client Key")
        var client_key: [32]u8 = undefined;
        HmacSha256.create(&client_key, "Client Key", &self.salted_password);

        // StoredKey = SHA256(ClientKey)
        var stored_key: [32]u8 = undefined;
        Sha256.hash(&client_key, &stored_key, .{});

        // Build AuthMessage = client-first-message-bare + "," + server-first-message + "," + client-final-message-without-proof
        const cfm_no_proof_prefix = "c=biws,r="; // biws = base64("n,,")
        var auth_pos: usize = 0;
        @memcpy(self.auth_message_buf[auth_pos .. auth_pos + self.client_first_bare_len], self.client_first_bare_buf[0..self.client_first_bare_len]);
        auth_pos += self.client_first_bare_len;
        self.auth_message_buf[auth_pos] = ',';
        auth_pos += 1;
        @memcpy(self.auth_message_buf[auth_pos .. auth_pos + server_first.len], server_first);
        auth_pos += server_first.len;
        self.auth_message_buf[auth_pos] = ',';
        auth_pos += 1;
        @memcpy(self.auth_message_buf[auth_pos .. auth_pos + cfm_no_proof_prefix.len], cfm_no_proof_prefix);
        auth_pos += cfm_no_proof_prefix.len;
        @memcpy(self.auth_message_buf[auth_pos .. auth_pos + self.server_nonce_len], self.server_nonce[0..self.server_nonce_len]);
        auth_pos += self.server_nonce_len;
        self.auth_message_len = auth_pos;

        // ClientSignature = HMAC(StoredKey, AuthMessage)
        var client_sig: [32]u8 = undefined;
        HmacSha256.create(&client_sig, self.auth_message_buf[0..self.auth_message_len], &stored_key);

        // ClientProof = ClientKey XOR ClientSignature
        var client_proof: [32]u8 = undefined;
        for (&client_proof, client_key, client_sig) |*p, k, s| {
            p.* = k ^ s;
        }

        // Base64-encode the proof
        var proof_b64: [44]u8 = undefined;
        _ = std.base64.standard.Encoder.encode(&proof_b64, &client_proof);

        // Build client-final-message: "c=biws,r=<nonce>,p=<proof>"
        var pos: usize = 0;
        if (out_buf.len < cfm_no_proof_prefix.len + self.server_nonce_len + 3 + proof_b64.len)
            return Error.BufferTooShort;

        @memcpy(out_buf[pos .. pos + cfm_no_proof_prefix.len], cfm_no_proof_prefix);
        pos += cfm_no_proof_prefix.len;
        @memcpy(out_buf[pos .. pos + self.server_nonce_len], self.server_nonce[0..self.server_nonce_len]);
        pos += self.server_nonce_len;
        @memcpy(out_buf[pos .. pos + 3], ",p=");
        pos += 3;
        @memcpy(out_buf[pos .. pos + proof_b64.len], &proof_b64);
        pos += proof_b64.len;

        return out_buf[0..pos];
    }

    /// Verify the server-final-message.
    pub fn verifyServerFinal(self: *ScramClient, server_final: []const u8) Error!void {
        // Parse: v=<server-signature-base64>
        if (server_final.len < 2 or server_final[0] != 'v' or server_final[1] != '=')
            return Error.InvalidServerResponse;
        const sig_b64 = server_final[2..];

        // Compute expected ServerSignature
        // ServerKey = HMAC(SaltedPassword, "Server Key")
        var server_key: [32]u8 = undefined;
        HmacSha256.create(&server_key, "Server Key", &self.salted_password);

        // ServerSignature = HMAC(ServerKey, AuthMessage)
        var expected_sig: [32]u8 = undefined;
        HmacSha256.create(&expected_sig, self.auth_message_buf[0..self.auth_message_len], &server_key);

        // Base64-encode expected
        var expected_b64: [44]u8 = undefined;
        _ = std.base64.standard.Encoder.encode(&expected_b64, &expected_sig);

        if (!std.mem.eql(u8, sig_b64, &expected_b64))
            return Error.ServerVerificationFailed;
    }
};

/// Hi(password, salt, iterations) — PBKDF2-HMAC-SHA256
fn hi(password: []const u8, salt: []const u8, iterations: u32) [32]u8 {
    // U1 = HMAC(password, salt || INT(1))
    var salt_plus: [68]u8 = undefined; // max salt + 4
    @memcpy(salt_plus[0..salt.len], salt);
    salt_plus[salt.len] = 0;
    salt_plus[salt.len + 1] = 0;
    salt_plus[salt.len + 2] = 0;
    salt_plus[salt.len + 3] = 1;

    var u_prev: [32]u8 = undefined;
    HmacSha256.create(&u_prev, salt_plus[0 .. salt.len + 4], password);
    var result = u_prev;

    var i: u32 = 1;
    while (i < iterations) : (i += 1) {
        var u_next: [32]u8 = undefined;
        HmacSha256.create(&u_next, &u_prev, password);
        for (&result, u_next) |*r, n| {
            r.* ^= n;
        }
        u_prev = u_next;
    }

    return result;
}

/// Compute MD5 password hash for PostgreSQL MD5 authentication.
/// Format: "md5" + md5(md5(password + username) + salt)
pub fn md5Password(username: []const u8, password: []const u8, salt: [4]u8, out: *[35]u8) void {
    const Md5 = std.crypto.hash.Md5;

    // Step 1: md5(password + username)
    var h1 = Md5.init(.{});
    h1.update(password);
    h1.update(username);
    var digest1: [Md5.digest_length]u8 = undefined;
    h1.final(&digest1);

    // Hex-encode digest1
    var hex1: [32]u8 = undefined;
    hex_encode(&digest1, &hex1);

    // Step 2: md5(hex1 + salt)
    var h2 = Md5.init(.{});
    h2.update(&hex1);
    h2.update(&salt);
    var digest2: [Md5.digest_length]u8 = undefined;
    h2.final(&digest2);

    out[0] = 'm';
    out[1] = 'd';
    out[2] = '5';
    hex_encode(&digest2, out[3..35]);
}

fn hex_encode(input: []const u8, output: []u8) void {
    const hex = "0123456789abcdef";
    for (input, 0..) |b, i| {
        output[i * 2] = hex[b >> 4];
        output[i * 2 + 1] = hex[b & 0x0f];
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

test "Hi (PBKDF2) produces deterministic output" {
    const result1 = hi("password", "salt", 4096);
    const result2 = hi("password", "salt", 4096);
    try std.testing.expectEqualSlices(u8, &result1, &result2);
}

test "Hi with 1 iteration" {
    const result = hi("password", "salt", 1);
    // Just verify it produces 32 bytes and doesn't crash
    try std.testing.expectEqual(@as(usize, 32), result.len);
}

test "MD5 password hash format" {
    var out: [35]u8 = undefined;
    md5Password("postgres", "secret", .{ 0x01, 0x02, 0x03, 0x04 }, &out);
    // Must start with "md5"
    try std.testing.expectEqualStrings("md5", out[0..3]);
    // Total length is 35 (3 + 32 hex chars)
    try std.testing.expectEqual(@as(usize, 35), out.len);
}

test "MD5 password hash is deterministic" {
    var out1: [35]u8 = undefined;
    var out2: [35]u8 = undefined;
    md5Password("user", "pass", .{ 0xAA, 0xBB, 0xCC, 0xDD }, &out1);
    md5Password("user", "pass", .{ 0xAA, 0xBB, 0xCC, 0xDD }, &out2);
    try std.testing.expectEqualSlices(u8, &out1, &out2);
}

test "SCRAM client-first-message format" {
    var client = ScramClient{};
    var buf: [MAX_CLIENT_FIRST_LEN]u8 = undefined;
    const msg = try client.clientFirstMessage("postgres", &buf);
    // Must start with "n,,n="
    try std.testing.expect(std.mem.startsWith(u8, msg, "n,,n=postgres,r="));
    // Nonce must be present and non-empty
    try std.testing.expect(msg.len > "n,,n=postgres,r=".len);
}

test "SCRAM full handshake simulation" {
    // This tests the SCRAM state machine with a synthetic server response.
    var client = ScramClient{};

    // Step 1: Client first message
    var first_buf: [MAX_CLIENT_FIRST_LEN]u8 = undefined;
    const first_msg = try client.clientFirstMessage("postgres", &first_buf);
    try std.testing.expect(first_msg.len > 0);

    // Step 2: Simulate server-first-message
    // The server echoes the client nonce + adds its own, provides salt and iterations
    var server_first_buf: [MAX_SERVER_FIRST_LEN]u8 = undefined;
    var sf_pos: usize = 0;
    const sf_prefix = "r=";
    @memcpy(server_first_buf[sf_pos .. sf_pos + sf_prefix.len], sf_prefix);
    sf_pos += sf_prefix.len;
    // Combined nonce = client nonce + "servernonce"
    @memcpy(server_first_buf[sf_pos .. sf_pos + client.client_nonce_len], client.client_nonce[0..client.client_nonce_len]);
    sf_pos += client.client_nonce_len;
    const extra_nonce = "servernonce123";
    @memcpy(server_first_buf[sf_pos .. sf_pos + extra_nonce.len], extra_nonce);
    sf_pos += extra_nonce.len;
    // Salt (base64 of "testsalt")
    const salt_part = ",s=dGVzdHNhbHQ=,i=4096";
    @memcpy(server_first_buf[sf_pos .. sf_pos + salt_part.len], salt_part);
    sf_pos += salt_part.len;

    // Step 3: Process server-first and generate client-final
    var final_buf: [MAX_CLIENT_FINAL_LEN]u8 = undefined;
    const final_msg = try client.processServerFirst(
        server_first_buf[0..sf_pos],
        "mysecretpassword",
        &final_buf,
    );
    try std.testing.expect(std.mem.startsWith(u8, final_msg, "c=biws,r="));
    try std.testing.expect(std.mem.indexOf(u8, final_msg, ",p=") != null);
}

test "hex_encode" {
    var out: [8]u8 = undefined;
    hex_encode(&[_]u8{ 0xDE, 0xAD, 0xBE, 0xEF }, &out);
    try std.testing.expectEqualStrings("deadbeef", &out);
}
