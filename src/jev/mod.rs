//! Adapter: [`Judge`] over TypeSafe AI's Jev HTTP API.
//!
//! This is the only module that performs network IO. The wire format lives in
//! [`wire`] and is converted to the crate's [`crate::question`] vocabulary at
//! this boundary.

pub mod wire;

use std::fmt;

use async_trait::async_trait;

use crate::event::Payload;
use crate::judge::{Judge, JudgeError};
use crate::question::{AnswerSet, QuestionSet};

/// Default System One endpoint.
pub const DEFAULT_ENDPOINT: &str = "https://api.typesafe.ai/v1/systemone";
/// Default model alias.
pub const DEFAULT_MODEL: &str = "jev-latest";
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

/// Why a [`JevJudge`] could not be built.
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

/// A [`Judge`] backed by the Jev API.
#[derive(Debug)]
pub struct JevJudge {
    client: reqwest::Client,
    endpoint: String,
    model: String,
    key: ApiKey,
}

impl JevJudge {
    /// Builds a judge against [`DEFAULT_ENDPOINT`] and [`DEFAULT_MODEL`].
    pub fn new(key: ApiKey) -> Result<Self, JevConfigError> {
        let client = reqwest::Client::builder()
            .build()
            .map_err(|e| JevConfigError::Client {
                reason: e.to_string(),
            })?;
        Ok(JevJudge {
            client,
            endpoint: DEFAULT_ENDPOINT.to_owned(),
            model: DEFAULT_MODEL.to_owned(),
            key,
        })
    }

    /// Reads the key from [`KEY_ENV`].
    pub fn from_env() -> Result<Self, JevConfigError> {
        let key =
            std::env::var(KEY_ENV).map_err(|_| JevConfigError::MissingKey { var: KEY_ENV })?;
        Self::new(ApiKey::new(key)?)
    }

    /// Overrides the endpoint, for proxies such as LiteLLM.
    pub fn with_endpoint(mut self, endpoint: impl Into<String>) -> Self {
        self.endpoint = endpoint.into();
        self
    }

    /// Overrides the model alias.
    pub fn with_model(mut self, model: impl Into<String>) -> Self {
        self.model = model.into();
        self
    }
}

#[async_trait]
impl Judge for JevJudge {
    async fn judge(
        &self,
        payload: &Payload,
        questions: &QuestionSet,
    ) -> Result<AnswerSet, JudgeError> {
        let request = wire::encode(&self.model, payload, questions);
        let response = self
            .client
            .post(&self.endpoint)
            .bearer_auth(&self.key.0)
            .json(&request)
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
        let body: wire::Response =
            response
                .json()
                .await
                .map_err(|e| JudgeError::MalformedReply {
                    detail: e.to_string(),
                })?;
        wire::decode(body)
    }
}
