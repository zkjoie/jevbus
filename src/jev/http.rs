//! [`SystemOne`] over TypeSafe AI's HTTPS API. The only network code in the
//! crate.

use std::fmt;

use async_trait::async_trait;

use super::{wire, JevJudge, SystemOne};
use crate::judge::JudgeError;

/// Default System One endpoint.
pub const DEFAULT_ENDPOINT: &str = "https://api.typesafe.ai/v1/systemone";
/// Environment variable read by [`JevJudge::from_env`].
pub const KEY_ENV: &str = "JEV_KEY";

/// A Jev API key.
///
/// Not `Clone`: the key is a capability, and each holder is a deliberate
/// choice. `Debug` redacts the value.
pub struct ApiKey(String);

impl ApiKey {
    /// Validates that `key` is non-empty.
    pub fn new(key: impl Into<String>) -> Result<Self, JevConfigError> {
        let key = key.into();
        if key.is_empty() {
            Err(JevConfigError::EmptyKey)
        } else {
            Ok(ApiKey(key))
        }
    }
}

impl fmt::Debug for ApiKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("ApiKey(<redacted>)")
    }
}

/// Why an [`HttpSystemOne`] could not be built.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum JevConfigError {
    /// The key variable is not set.
    #[error("environment variable {var} is not set")]
    MissingKey {
        /// The variable name.
        var: &'static str,
    },
    /// The key is empty.
    #[error("api key is empty")]
    EmptyKey,
    /// The HTTP client could not be constructed.
    #[error("http client: {reason}")]
    Client {
        /// Detail from the client library.
        reason: String,
    },
}

/// The System One protocol over HTTPS with bearer authentication.
#[derive(Debug)]
pub struct HttpSystemOne {
    client: reqwest::Client,
    endpoint: String,
    key: ApiKey,
}

impl HttpSystemOne {
    /// Talks to [`DEFAULT_ENDPOINT`] with `key`.
    pub fn new(key: ApiKey) -> Result<Self, JevConfigError> {
        let client = reqwest::Client::builder()
            .build()
            .map_err(|e| JevConfigError::Client {
                reason: e.to_string(),
            })?;
        Ok(HttpSystemOne {
            client,
            endpoint: DEFAULT_ENDPOINT.to_owned(),
            key,
        })
    }

    /// Reads the key from [`KEY_ENV`].
    pub fn from_env() -> Result<Self, JevConfigError> {
        let key =
            std::env::var(KEY_ENV).map_err(|_| JevConfigError::MissingKey { var: KEY_ENV })?;
        Self::new(ApiKey::new(key)?)
    }

    /// Overrides the endpoint, for a proxy or gateway that speaks the same
    /// protocol.
    pub fn with_endpoint(mut self, endpoint: impl Into<String>) -> Self {
        self.endpoint = endpoint.into();
        self
    }

    /// The endpoint in use.
    pub fn endpoint(&self) -> &str {
        &self.endpoint
    }
}

#[async_trait]
impl SystemOne for HttpSystemOne {
    async fn ask(&self, request: &wire::Request<'_>) -> Result<wire::Response, JudgeError> {
        let response = self
            .client
            .post(&self.endpoint)
            .bearer_auth(&self.key.0)
            .json(request)
            .send()
            .await
            .map_err(|e| JudgeError::Unavailable {
                reason: e.to_string(),
            })?;
        let status = response.status();
        if !status.is_success() {
            let message = response.text().await.unwrap_or_default();
            return Err(JudgeError::Rejected {
                status: status.as_u16(),
                message,
            });
        }
        response
            .json()
            .await
            .map_err(|e| JudgeError::MalformedReply {
                detail: e.to_string(),
            })
    }
}

impl JevJudge<HttpSystemOne> {
    /// A judge against [`DEFAULT_ENDPOINT`] with `key`.
    pub fn new(key: ApiKey) -> Result<Self, JevConfigError> {
        HttpSystemOne::new(key).map(JevJudge::over)
    }

    /// A judge whose key comes from [`KEY_ENV`].
    pub fn from_env() -> Result<Self, JevConfigError> {
        HttpSystemOne::from_env().map(JevJudge::over)
    }

    /// Overrides the HTTP endpoint.
    pub fn with_endpoint(self, endpoint: impl Into<String>) -> Self {
        let JevJudge { protocol, model } = self;
        JevJudge {
            protocol: protocol.with_endpoint(endpoint),
            model,
        }
    }
}
