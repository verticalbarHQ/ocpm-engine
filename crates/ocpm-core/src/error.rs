use serde::{Deserialize, Serialize};
use std::fmt::{Display, Formatter};

pub type OcpmResult<T> = Result<T, OcpmError>;

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OcpmErrorCode {
    InvalidRequest,
    UnsupportedSemanticVersion,
    UnsupportedFormat,
    InvalidData,
    NotFound,
    UnauthorizedScope,
    ProviderUnavailable,
    ProviderContractViolation,
    ResourceLimit,
    InputLimit,
    Timeout,
    Cancelled,
    InsufficientData,
    SearchTruncated,
    ArtifactIncompatible,
    Internal,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct OcpmError {
    pub code: OcpmErrorCode,
    pub message: String,
    pub retryable: bool,
    pub field_path: Option<String>,
    pub limit: Option<u64>,
    pub actual: Option<u64>,
}

impl OcpmError {
    pub fn new(code: OcpmErrorCode, message: impl Into<String>) -> Self {
        Self {
            code,
            message: message.into(),
            retryable: false,
            field_path: None,
            limit: None,
            actual: None,
        }
    }

    pub fn invalid_data(message: impl Into<String>) -> Self {
        Self::new(OcpmErrorCode::InvalidData, message)
    }

    pub fn invalid_request(message: impl Into<String>) -> Self {
        Self::new(OcpmErrorCode::InvalidRequest, message)
    }

    pub fn resource_limit(message: impl Into<String>, limit: u64, actual: u64) -> Self {
        Self {
            code: OcpmErrorCode::ResourceLimit,
            message: message.into(),
            retryable: true,
            field_path: None,
            limit: Some(limit),
            actual: Some(actual),
        }
    }

    pub fn at(mut self, field_path: impl Into<String>) -> Self {
        self.field_path = Some(field_path.into());
        self
    }
}

impl Display for OcpmError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "{}", self.message)
    }
}

impl std::error::Error for OcpmError {}
