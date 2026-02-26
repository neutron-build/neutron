//! RPC error type.

use std::fmt;

use neutron_grpc::GrpcStatus;

/// An error returned from an RPC handler.
///
/// Mapped to the appropriate `grpc-status` trailer automatically.
#[derive(Debug)]
pub struct RpcError {
    pub status:  GrpcStatus,
    pub message: String,
}

impl RpcError {
    pub fn invalid_argument(msg: impl Into<String>) -> Self {
        Self { status: GrpcStatus::InvalidArgument, message: msg.into() }
    }
    pub fn not_found(msg: impl Into<String>) -> Self {
        Self { status: GrpcStatus::NotFound, message: msg.into() }
    }
    pub fn already_exists(msg: impl Into<String>) -> Self {
        Self { status: GrpcStatus::AlreadyExists, message: msg.into() }
    }
    pub fn permission_denied(msg: impl Into<String>) -> Self {
        Self { status: GrpcStatus::PermissionDenied, message: msg.into() }
    }
    pub fn internal(msg: impl Into<String>) -> Self {
        Self { status: GrpcStatus::Internal, message: msg.into() }
    }
    pub fn unimplemented(msg: impl Into<String>) -> Self {
        Self { status: GrpcStatus::Unimplemented, message: msg.into() }
    }
    pub fn unavailable(msg: impl Into<String>) -> Self {
        Self { status: GrpcStatus::Unavailable, message: msg.into() }
    }
}

impl fmt::Display for RpcError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "[{:?}] {}", self.status, self.message)
    }
}

impl std::error::Error for RpcError {}
