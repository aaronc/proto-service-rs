use alloc::string::String;
use core::fmt;

use num_enum::{FromPrimitive, IntoPrimitive};

/// gRPC status codes.
///
/// `i32::from(code)` encodes to the wire value; `Code::from(i32)` decodes it,
/// mapping any unrecognized value to `Unknown` per gRPC's rule.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, FromPrimitive, IntoPrimitive)]
#[repr(i32)]
pub enum Code {
    Ok = 0,
    Cancelled = 1,
    #[num_enum(default)]
    Unknown = 2,
    InvalidArgument = 3,
    DeadlineExceeded = 4,
    NotFound = 5,
    AlreadyExists = 6,
    PermissionDenied = 7,
    ResourceExhausted = 8,
    FailedPrecondition = 9,
    Aborted = 10,
    OutOfRange = 11,
    Unimplemented = 12,
    Internal = 13,
    Unavailable = 14,
    DataLoss = 15,
    Unauthenticated = 16,
}

/// A gRPC status describing the result of an RPC call.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Status {
    code: Code,
    message: String,
}

impl Status {
    pub fn new(code: Code, message: impl Into<String>) -> Self {
        Self {
            code,
            message: message.into(),
        }
    }

    pub fn ok(message: impl Into<String>) -> Self {
        Self::new(Code::Ok, message)
    }

    pub fn cancelled(message: impl Into<String>) -> Self {
        Self::new(Code::Cancelled, message)
    }

    pub fn unknown(message: impl Into<String>) -> Self {
        Self::new(Code::Unknown, message)
    }

    pub fn invalid_argument(message: impl Into<String>) -> Self {
        Self::new(Code::InvalidArgument, message)
    }

    pub fn deadline_exceeded(message: impl Into<String>) -> Self {
        Self::new(Code::DeadlineExceeded, message)
    }

    pub fn not_found(message: impl Into<String>) -> Self {
        Self::new(Code::NotFound, message)
    }

    pub fn already_exists(message: impl Into<String>) -> Self {
        Self::new(Code::AlreadyExists, message)
    }

    pub fn permission_denied(message: impl Into<String>) -> Self {
        Self::new(Code::PermissionDenied, message)
    }

    pub fn resource_exhausted(message: impl Into<String>) -> Self {
        Self::new(Code::ResourceExhausted, message)
    }

    pub fn failed_precondition(message: impl Into<String>) -> Self {
        Self::new(Code::FailedPrecondition, message)
    }

    pub fn aborted(message: impl Into<String>) -> Self {
        Self::new(Code::Aborted, message)
    }

    pub fn out_of_range(message: impl Into<String>) -> Self {
        Self::new(Code::OutOfRange, message)
    }

    pub fn unimplemented(message: impl Into<String>) -> Self {
        Self::new(Code::Unimplemented, message)
    }

    pub fn internal(message: impl Into<String>) -> Self {
        Self::new(Code::Internal, message)
    }

    pub fn unavailable(message: impl Into<String>) -> Self {
        Self::new(Code::Unavailable, message)
    }

    pub fn data_loss(message: impl Into<String>) -> Self {
        Self::new(Code::DataLoss, message)
    }

    pub fn unauthenticated(message: impl Into<String>) -> Self {
        Self::new(Code::Unauthenticated, message)
    }

    pub fn code(&self) -> Code {
        self.code
    }

    pub fn message(&self) -> &str {
        &self.message
    }
}

impl fmt::Display for Status {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "status: {:?}, message: {:?}", self.code, self.message)
    }
}

impl core::error::Error for Status {}

impl From<Code> for Status {
    fn from(code: Code) -> Self {
        Self::new(code, "")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn code_i32_roundtrip() {
        for code in [
            Code::Ok,
            Code::NotFound,
            Code::Internal,
            Code::Unauthenticated,
        ] {
            assert_eq!(Code::from(i32::from(code)), code);
        }
    }

    #[test]
    fn unknown_code_maps_to_unknown() {
        assert_eq!(Code::from(42), Code::Unknown);
    }
}
