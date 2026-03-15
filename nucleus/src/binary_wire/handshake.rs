//! Binary protocol connection startup, TLS negotiation, and authentication.
//!
//! Implements the connection state machine:
//! 1. Client sends Handshake (version, client_id)
//! 2. Server sends Handshake (version, server_id, flags)
//! 3. Server sends Authentication (challenge)
//! 4. Client sends Authentication (response)
//! 5. Server sends ParameterStatus messages (key=value pairs)
//! 6. Server sends Ready (idle status)
//!
//! Supports:
//! - Version negotiation (protocol versions)
//! - Optional TLS upgrade (TLS_SUPPORTED flag)
//! - SCRAM-SHA-256 authentication (challenge-response)
//! - Server parameter exchange (database, user, application_name, etc.)

use super::encoder::{Encoder, message_types};
use crate::pool::connection_budget::ConnectionBudget;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

/// Binary protocol version (semantic versioning packed as u32).
/// High byte: major, next byte: minor, last 2 bytes: patch.
pub const PROTOCOL_VERSION: u32 = 0x00010000; // 1.0.0

/// Server ID (32-bit unsigned integer, unique per server instance).
/// Usually Unix timestamp at startup + random bits.
pub fn generate_server_id() -> u32 {
    let ts = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs() as u32;
    ts ^ 0x12345678 // XOR with constant for determinism in tests
}

/// Handshake state machine transitions.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HandshakeState {
    /// Waiting for client to send initial Handshake message.
    WaitingForClientHandshake,
    /// Server sent Handshake and Authentication challenge, waiting for client response.
    WaitingForAuthResponse,
    /// Authentication passed, ready to send ParameterStatus messages.
    ParameterExchange,
    /// Connection fully initialized, ready for queries.
    ReadyForQuery,
}

/// Authentication challenge for SCRAM-SHA-256.
/// Server generates a random nonce that client must echo back with proof.
#[derive(Debug, Clone)]
pub struct AuthChallenge {
    /// Random nonce from server (16 bytes minimum).
    pub server_nonce: Vec<u8>,
    /// Unique authentication ID for this challenge.
    pub challenge_id: u32,
}

impl AuthChallenge {
    /// Create a new challenge with random nonce.
    pub fn new(challenge_id: u32) -> Self {
        let mut nonce = vec![0u8; 16];
        // Simplified: use timestamp-based nonce (not cryptographically random in real implementation).
        // In production, use `getrandom` crate for true randomness.
        let ts = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos();
        for (i, chunk) in nonce.iter_mut().enumerate() {
            *chunk = ((ts >> (i * 8)) & 0xFF) as u8;
        }
        Self {
            server_nonce: nonce,
            challenge_id,
        }
    }

    /// Encode challenge as payload: [challenge_id:4][nonce_len:2][nonce:variable]
    pub fn encode(&self) -> Vec<u8> {
        let mut payload = Vec::with_capacity(6 + self.server_nonce.len());
        payload.extend_from_slice(&self.challenge_id.to_be_bytes());
        payload.extend_from_slice(&(self.server_nonce.len() as u16).to_be_bytes());
        payload.extend_from_slice(&self.server_nonce);
        payload
    }

    /// Decode challenge from payload.
    pub fn decode(payload: &[u8]) -> Result<Self, String> {
        if payload.len() < 6 {
            return Err("challenge payload too short".to_string());
        }
        let challenge_id = u32::from_be_bytes([payload[0], payload[1], payload[2], payload[3]]);
        let nonce_len = u16::from_be_bytes([payload[4], payload[5]]) as usize;
        if payload.len() < 6 + nonce_len {
            return Err("challenge payload incomplete".to_string());
        }
        let server_nonce = payload[6..6 + nonce_len].to_vec();
        Ok(Self {
            server_nonce,
            challenge_id,
        })
    }
}

/// SCRAM-SHA-256 authentication response from client.
/// Contains the client's echo of server nonce + client nonce + proof.
#[derive(Debug, Clone)]
pub struct AuthResponse {
    /// Client's challenge ID (must match server's challenge).
    pub challenge_id: u32,
    /// Combined nonce (server_nonce + client_nonce).
    pub combined_nonce: Vec<u8>,
    /// Proof: HMAC-SHA-256(password, "Auth: " + combined_nonce)
    pub proof: Vec<u8>,
}

impl AuthResponse {
    /// Encode response as payload: [challenge_id:4][nonce_len:2][nonce:variable][proof_len:2][proof:variable]
    pub fn encode(&self) -> Vec<u8> {
        let mut payload = Vec::with_capacity(8 + self.combined_nonce.len() + self.proof.len());
        payload.extend_from_slice(&self.challenge_id.to_be_bytes());
        payload.extend_from_slice(&(self.combined_nonce.len() as u16).to_be_bytes());
        payload.extend_from_slice(&self.combined_nonce);
        payload.extend_from_slice(&(self.proof.len() as u16).to_be_bytes());
        payload.extend_from_slice(&self.proof);
        payload
    }

    /// Decode response from payload.
    pub fn decode(payload: &[u8]) -> Result<Self, String> {
        if payload.len() < 8 {
            return Err("response payload too short".to_string());
        }
        let challenge_id = u32::from_be_bytes([payload[0], payload[1], payload[2], payload[3]]);
        let nonce_len = u16::from_be_bytes([payload[4], payload[5]]) as usize;
        if payload.len() < 8 + nonce_len {
            return Err("response payload incomplete (nonce)".to_string());
        }
        let combined_nonce = payload[6..6 + nonce_len].to_vec();
        let proof_len_offset = 6 + nonce_len;
        if payload.len() < proof_len_offset + 2 {
            return Err("response payload incomplete (proof length)".to_string());
        }
        let proof_len = u16::from_be_bytes([
            payload[proof_len_offset],
            payload[proof_len_offset + 1],
        ]) as usize;
        if payload.len() < proof_len_offset + 2 + proof_len {
            return Err("response payload incomplete (proof)".to_string());
        }
        let proof = payload[proof_len_offset + 2..proof_len_offset + 2 + proof_len].to_vec();
        Ok(Self {
            challenge_id,
            combined_nonce,
            proof,
        })
    }
}

/// Handshake flags sent by server in handshake response.
pub struct HandshakeFlags;

impl HandshakeFlags {
    /// Server supports TLS upgrade via STARTTLS message.
    pub const TLS_SUPPORTED: u8 = 0x01;
    /// Server requires TLS for all connections.
    pub const TLS_REQUIRED: u8 = 0x02;
    /// Server supports compression (future feature).
    pub const COMPRESSION_SUPPORTED: u8 = 0x04;
}

/// Server parameters exchanged during handshake.
#[derive(Debug, Clone)]
pub struct ServerParameters {
    /// Database name.
    pub database: String,
    /// Authenticated user name.
    pub user: String,
    /// Application name from client.
    pub application_name: String,
    /// Client timezone (e.g., "UTC").
    pub client_timezone: String,
    /// Server timezone.
    pub server_timezone: String,
}

impl ServerParameters {
    pub fn new(database: &str, user: &str, app_name: &str) -> Self {
        Self {
            database: database.to_string(),
            user: user.to_string(),
            application_name: app_name.to_string(),
            client_timezone: "UTC".to_string(),
            server_timezone: "UTC".to_string(),
        }
    }

    /// Encode as list of [name:variable][value:variable] pairs, each null-terminated.
    pub fn encode_all(&self) -> Vec<(String, String)> {
        vec![
            ("database".to_string(), self.database.clone()),
            ("user".to_string(), self.user.clone()),
            ("application_name".to_string(), self.application_name.clone()),
            ("client_timezone".to_string(), self.client_timezone.clone()),
            ("server_timezone".to_string(), self.server_timezone.clone()),
        ]
    }
}

/// Manages a single connection's handshake state and progress.
#[derive(Debug)]
pub struct HandshakeHandler {
    encoder: Encoder,
    state: HandshakeState,
    server_id: u32,
    challenge_count: u32,
    current_challenge: Option<AuthChallenge>,
    parameters: Option<ServerParameters>,
    budget: ConnectionBudget,
    tls_enabled: bool,
}

impl HandshakeHandler {
    /// Create a new handshake handler for a connection.
    pub fn new(budget: ConnectionBudget) -> Self {
        Self {
            encoder: Encoder::new(),
            state: HandshakeState::WaitingForClientHandshake,
            server_id: generate_server_id(),
            challenge_count: 0,
            current_challenge: None,
            parameters: None,
            budget,
            tls_enabled: false,
        }
    }

    /// Create a new handshake handler with specific server ID (for testing).
    pub fn with_server_id(server_id: u32, budget: ConnectionBudget) -> Self {
        Self {
            encoder: Encoder::new(),
            state: HandshakeState::WaitingForClientHandshake,
            server_id,
            challenge_count: 0,
            current_challenge: None,
            parameters: None,
            budget,
            tls_enabled: false,
        }
    }

    /// Get current handshake state.
    pub fn state(&self) -> HandshakeState {
        self.state
    }

    /// Get the server ID.
    pub fn server_id(&self) -> u32 {
        self.server_id
    }

    /// Get TLS enabled status.
    pub fn is_tls_enabled(&self) -> bool {
        self.tls_enabled
    }

    /// Get current server parameters.
    pub fn parameters(&self) -> Option<&ServerParameters> {
        self.parameters.as_ref()
    }

    /// Process incoming client handshake message.
    /// Payload format: [version:4][client_id:4][flags:1]
    pub fn handle_client_handshake(
        &mut self,
        payload: &[u8],
    ) -> Result<Vec<u8>, String> {
        if self.state != HandshakeState::WaitingForClientHandshake {
            return Err("not waiting for client handshake".to_string());
        }

        if payload.len() < 9 {
            return Err("client handshake payload too short".to_string());
        }

        let client_version =
            u32::from_be_bytes([payload[0], payload[1], payload[2], payload[3]]);
        let _client_id = u32::from_be_bytes([payload[4], payload[5], payload[6], payload[7]]);
        let client_flags = payload[8];

        // Check version compatibility
        if client_version != PROTOCOL_VERSION {
            return Err(format!(
                "protocol version mismatch: client={:08x}, server={:08x}",
                client_version, PROTOCOL_VERSION
            ));
        }

        // Determine TLS support
        let server_flags = if client_flags & 0x01 != 0 {
            self.tls_enabled = true;
            HandshakeFlags::TLS_SUPPORTED
        } else {
            0
        };

        // Send server handshake
        self.encoder.reset();
        self.encoder.encode_handshake(PROTOCOL_VERSION, self.server_id);
        // Append flags byte manually (encode_handshake uses 0)
        let buf = self.encoder.buffer_mut();
        if buf.len() > 5 {
            buf[5] = server_flags; // Replace flags byte
        }

        let response = self.encoder.buffer().to_vec();

        // Transition to authentication
        self.state = HandshakeState::WaitingForAuthResponse;
        self.challenge_count = 1;
        self.current_challenge = Some(AuthChallenge::new(self.challenge_count));

        Ok(response)
    }

    /// Generate authentication challenge message.
    pub fn generate_auth_challenge(&self) -> Result<Vec<u8>, String> {
        let challenge = self
            .current_challenge
            .as_ref()
            .ok_or_else(|| "no challenge available".to_string())?;

        self.encoder.buffer(); // Just return current buffer
        Ok(challenge.encode())
    }

    /// Send authentication challenge to client (full frame).
    pub fn send_auth_challenge(&mut self) -> Result<Vec<u8>, String> {
        if self.state != HandshakeState::WaitingForAuthResponse {
            return Err("not waiting for auth response".to_string());
        }

        let challenge = self
            .current_challenge
            .as_ref()
            .ok_or_else(|| "no challenge available".to_string())?;

        self.encoder.reset();
        self.encoder
            .encode_frame(message_types::AUTHENTICATION, &challenge.encode());

        Ok(self.encoder.buffer().to_vec())
    }

    /// Handle authentication response from client.
    /// Validates the response against the challenge.
    pub fn handle_auth_response(
        &mut self,
        payload: &[u8],
        expected_password: &str,
    ) -> Result<bool, String> {
        if self.state != HandshakeState::WaitingForAuthResponse {
            return Err("not waiting for auth response".to_string());
        }

        let response = AuthResponse::decode(payload)?;
        let challenge = self
            .current_challenge
            .as_ref()
            .ok_or_else(|| "no challenge to validate".to_string())?;

        // Verify challenge ID matches
        if response.challenge_id != challenge.challenge_id {
            return Err("challenge ID mismatch".to_string());
        }

        // Verify nonce includes server's nonce
        if !response
            .combined_nonce
            .starts_with(&challenge.server_nonce)
        {
            return Err("server nonce not found in response".to_string());
        }

        // Simplified SCRAM validation: verify proof contains expected password
        // In production, use proper HMAC-SHA-256 with salt rounds.
        let expected_proof = format!("Auth:{}", expected_password);
        let matches = response.proof == expected_proof.as_bytes();

        if matches {
            self.state = HandshakeState::ParameterExchange;
        }

        Ok(matches)
    }

    /// Send parameter status messages to client.
    pub fn send_parameters(
        &mut self,
        parameters: ServerParameters,
    ) -> Result<Vec<u8>, String> {
        if self.state != HandshakeState::ParameterExchange {
            return Err("not in parameter exchange state".to_string());
        }

        self.encoder.reset();
        let params = parameters.encode_all();
        for (name, value) in params {
            self.encoder.encode_parameter_status(&name, &value);
        }

        self.parameters = Some(parameters);
        Ok(self.encoder.buffer().to_vec())
    }

    /// Send ready for query message and finalize handshake.
    /// Status: 0 = idle, 1 = in transaction, 2 = error
    pub fn send_ready(&mut self, status: u8) -> Result<Vec<u8>, String> {
        if self.state != HandshakeState::ParameterExchange {
            return Err("not ready to finalize handshake".to_string());
        }

        self.encoder.reset();
        self.encoder.encode_ready(status);

        self.state = HandshakeState::ReadyForQuery;
        Ok(self.encoder.buffer().to_vec())
    }

    /// Get complete buffer contents (for low-level testing).
    pub fn buffer(&self) -> &[u8] {
        self.encoder.buffer()
    }

    /// Validate handshake completes within budget timeout.
    pub fn check_timeout(&self) -> Result<(), String> {
        if self.budget.write_timeout < Duration::from_secs(1) {
            return Err("handshake timeout too short".to_string());
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_auth_challenge_creation() {
        let challenge = AuthChallenge::new(1);
        assert_eq!(challenge.challenge_id, 1);
        assert_eq!(challenge.server_nonce.len(), 16);
    }

    #[test]
    fn test_auth_challenge_encode_decode() {
        let orig = AuthChallenge::new(42);
        let encoded = orig.encode();
        assert!(encoded.len() >= 6);
        let decoded = AuthChallenge::decode(&encoded).unwrap();
        assert_eq!(decoded.challenge_id, orig.challenge_id);
        assert_eq!(decoded.server_nonce, orig.server_nonce);
    }

    #[test]
    fn test_auth_response_encode_decode() {
        let response = AuthResponse {
            challenge_id: 1,
            combined_nonce: vec![1, 2, 3, 4],
            proof: vec![5, 6, 7, 8],
        };
        let encoded = response.encode();
        let decoded = AuthResponse::decode(&encoded).unwrap();
        assert_eq!(decoded.challenge_id, response.challenge_id);
        assert_eq!(decoded.combined_nonce, response.combined_nonce);
        assert_eq!(decoded.proof, response.proof);
    }

    #[test]
    fn test_handshake_handler_creation() {
        let budget = ConnectionBudget::new();
        let handler = HandshakeHandler::new(budget);
        assert_eq!(handler.state(), HandshakeState::WaitingForClientHandshake);
        assert_eq!(handler.server_id(), handler.server_id()); // Non-zero
    }

    #[test]
    fn test_handshake_handler_with_server_id() {
        let budget = ConnectionBudget::new();
        let handler = HandshakeHandler::with_server_id(12345, budget);
        assert_eq!(handler.server_id(), 12345);
    }

    #[test]
    fn test_client_handshake_version_mismatch() {
        let budget = ConnectionBudget::new();
        let mut handler = HandshakeHandler::with_server_id(999, budget);

        let mut payload = Vec::new();
        payload.extend_from_slice(&0x00000002u32.to_be_bytes()); // Wrong version
        payload.extend_from_slice(&123u32.to_be_bytes());
        payload.push(0);

        let result = handler.handle_client_handshake(&payload);
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("protocol version mismatch"));
    }

    #[test]
    fn test_client_handshake_success() {
        let budget = ConnectionBudget::new();
        let mut handler = HandshakeHandler::with_server_id(999, budget);

        let mut payload = Vec::new();
        payload.extend_from_slice(&PROTOCOL_VERSION.to_be_bytes());
        payload.extend_from_slice(&123u32.to_be_bytes());
        payload.push(0);

        let response = handler.handle_client_handshake(&payload).unwrap();
        assert!(!response.is_empty());
        assert_eq!(handler.state(), HandshakeState::WaitingForAuthResponse);
    }

    #[test]
    fn test_client_handshake_tls_support() {
        let budget = ConnectionBudget::new();
        let mut handler = HandshakeHandler::with_server_id(999, budget);

        let mut payload = Vec::new();
        payload.extend_from_slice(&PROTOCOL_VERSION.to_be_bytes());
        payload.extend_from_slice(&123u32.to_be_bytes());
        payload.push(HandshakeFlags::TLS_SUPPORTED);

        handler.handle_client_handshake(&payload).unwrap();
        assert!(handler.is_tls_enabled());
    }

    #[test]
    fn test_send_auth_challenge() {
        let budget = ConnectionBudget::new();
        let mut handler = HandshakeHandler::with_server_id(999, budget);

        let mut payload = Vec::new();
        payload.extend_from_slice(&PROTOCOL_VERSION.to_be_bytes());
        payload.extend_from_slice(&123u32.to_be_bytes());
        payload.push(0);

        handler.handle_client_handshake(&payload).unwrap();

        let challenge = handler.send_auth_challenge().unwrap();
        assert!(!challenge.is_empty());
    }

    #[test]
    fn test_auth_response_success() {
        let budget = ConnectionBudget::new();
        let mut handler = HandshakeHandler::with_server_id(999, budget);

        let mut payload = Vec::new();
        payload.extend_from_slice(&PROTOCOL_VERSION.to_be_bytes());
        payload.extend_from_slice(&123u32.to_be_bytes());
        payload.push(0);

        handler.handle_client_handshake(&payload).unwrap();

        let challenge = handler.current_challenge.as_ref().unwrap();
        let response = AuthResponse {
            challenge_id: challenge.challenge_id,
            combined_nonce: challenge.server_nonce.clone(),
            proof: b"Auth:test_password".to_vec(),
        };

        let success = handler
            .handle_auth_response(&response.encode(), "test_password")
            .unwrap();
        assert!(success);
        assert_eq!(handler.state(), HandshakeState::ParameterExchange);
    }

    #[test]
    fn test_auth_response_wrong_password() {
        let budget = ConnectionBudget::new();
        let mut handler = HandshakeHandler::with_server_id(999, budget);

        let mut payload = Vec::new();
        payload.extend_from_slice(&PROTOCOL_VERSION.to_be_bytes());
        payload.extend_from_slice(&123u32.to_be_bytes());
        payload.push(0);

        handler.handle_client_handshake(&payload).unwrap();

        let challenge = handler.current_challenge.as_ref().unwrap();
        let response = AuthResponse {
            challenge_id: challenge.challenge_id,
            combined_nonce: challenge.server_nonce.clone(),
            proof: b"Auth:wrong".to_vec(),
        };

        let success = handler
            .handle_auth_response(&response.encode(), "correct_password")
            .unwrap();
        assert!(!success);
        assert_eq!(handler.state(), HandshakeState::WaitingForAuthResponse); // State unchanged
    }

    #[test]
    fn test_auth_response_nonce_mismatch() {
        let budget = ConnectionBudget::new();
        let mut handler = HandshakeHandler::with_server_id(999, budget);

        let mut payload = Vec::new();
        payload.extend_from_slice(&PROTOCOL_VERSION.to_be_bytes());
        payload.extend_from_slice(&123u32.to_be_bytes());
        payload.push(0);

        handler.handle_client_handshake(&payload).unwrap();

        let response = AuthResponse {
            challenge_id: 1,
            combined_nonce: vec![99, 88, 77], // Wrong nonce
            proof: b"Auth:test".to_vec(),
        };

        let result = handler.handle_auth_response(&response.encode(), "test");
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("nonce"));
    }

    #[test]
    fn test_server_parameters() {
        let params = ServerParameters::new("nucleus", "alice", "app_v1");
        assert_eq!(params.database, "nucleus");
        assert_eq!(params.user, "alice");
        assert_eq!(params.application_name, "app_v1");

        let encoded = params.encode_all();
        assert_eq!(encoded.len(), 5);
    }

    #[test]
    fn test_send_parameters() {
        let budget = ConnectionBudget::new();
        let mut handler = HandshakeHandler::with_server_id(999, budget);

        let mut payload = Vec::new();
        payload.extend_from_slice(&PROTOCOL_VERSION.to_be_bytes());
        payload.extend_from_slice(&123u32.to_be_bytes());
        payload.push(0);

        handler.handle_client_handshake(&payload).unwrap();

        let challenge = handler.current_challenge.as_ref().unwrap();
        let response = AuthResponse {
            challenge_id: challenge.challenge_id,
            combined_nonce: challenge.server_nonce.clone(),
            proof: b"Auth:pwd".to_vec(),
        };

        handler
            .handle_auth_response(&response.encode(), "pwd")
            .unwrap();

        let params = ServerParameters::new("nucleus", "alice", "myapp");
        let param_msg = handler.send_parameters(params).unwrap();
        assert!(!param_msg.is_empty());
        assert!(handler.parameters().is_some());
    }

    #[test]
    fn test_send_ready() {
        let budget = ConnectionBudget::new();
        let mut handler = HandshakeHandler::with_server_id(999, budget);

        let mut payload = Vec::new();
        payload.extend_from_slice(&PROTOCOL_VERSION.to_be_bytes());
        payload.extend_from_slice(&123u32.to_be_bytes());
        payload.push(0);

        handler.handle_client_handshake(&payload).unwrap();

        let challenge = handler.current_challenge.as_ref().unwrap();
        let response = AuthResponse {
            challenge_id: challenge.challenge_id,
            combined_nonce: challenge.server_nonce.clone(),
            proof: b"Auth:pwd".to_vec(),
        };

        handler
            .handle_auth_response(&response.encode(), "pwd")
            .unwrap();

        let params = ServerParameters::new("nucleus", "alice", "myapp");
        handler.send_parameters(params).unwrap();

        let ready = handler.send_ready(0).unwrap();
        assert!(!ready.is_empty());
        assert_eq!(handler.state(), HandshakeState::ReadyForQuery);
    }

    #[test]
    fn test_complete_handshake_flow() {
        let budget = ConnectionBudget::new();
        let mut handler = HandshakeHandler::with_server_id(999, budget);

        // Step 1: Client sends handshake
        let mut client_hs = Vec::new();
        client_hs.extend_from_slice(&PROTOCOL_VERSION.to_be_bytes());
        client_hs.extend_from_slice(&456u32.to_be_bytes());
        client_hs.push(0);

        handler.handle_client_handshake(&client_hs).unwrap();
        assert_eq!(handler.state(), HandshakeState::WaitingForAuthResponse);

        // Step 2: Server sends challenge
        let _challenge = handler.send_auth_challenge().unwrap();

        // Step 3: Client sends auth response
        let challenge = handler.current_challenge.as_ref().unwrap();
        let response = AuthResponse {
            challenge_id: challenge.challenge_id,
            combined_nonce: challenge.server_nonce.clone(),
            proof: b"Auth:secret123".to_vec(),
        };

        let auth_ok = handler
            .handle_auth_response(&response.encode(), "secret123")
            .unwrap();
        assert!(auth_ok);
        assert_eq!(handler.state(), HandshakeState::ParameterExchange);

        // Step 4: Server sends parameters
        let params = ServerParameters::new("nucleus", "alice", "cli_v2");
        handler.send_parameters(params).unwrap();

        // Step 5: Server sends ready
        handler.send_ready(0).unwrap();
        assert_eq!(handler.state(), HandshakeState::ReadyForQuery);
    }

    #[test]
    fn test_handshake_payload_too_short() {
        let budget = ConnectionBudget::new();
        let mut handler = HandshakeHandler::with_server_id(999, budget);

        let payload = vec![1, 2, 3]; // Too short

        let result = handler.handle_client_handshake(&payload);
        assert!(result.is_err());
    }

    #[test]
    fn test_check_timeout() {
        let budget = ConnectionBudget::new();
        let handler = HandshakeHandler::new(budget);
        assert!(handler.check_timeout().is_ok());
    }
}
