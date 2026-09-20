//! Jev: TypeSafe AI's System One protocol, as a trait.
//!
//! ```text
//! class SystemOne p where
//!   ask :: p -> Request -> IO (Either JudgeError Response)
//!
//! newtype JevJudge p = JevJudge { protocol :: p, model :: Model }
//! instance SystemOne p => Judge (JevJudge p)
//! ```
//!
//! [`SystemOne`] is the protocol: a typed-question request in, a typed-answer
//! response out, in the shape defined by [`wire`]. [`JevJudge`] turns any
//! [`SystemOne`] into a [`Judge`] by encoding the crate's questions and
//! decoding the answers. The reference implementation, [`http::HttpSystemOne`],
//! speaks to TypeSafe's HTTPS API and is the only part that needs a network
//! stack; a gateway, a proxy, another vendor that speaks the same protocol,
//! or a test double implements [`SystemOne`] directly and needs none of it.

pub mod wire;

#[cfg(feature = "jev")]
pub mod http;

use async_trait::async_trait;

use crate::event::Payload;
use crate::judge::{Judge, JudgeError};
use crate::question::{AnswerSet, QuestionSet};

/// Default model alias.
pub const DEFAULT_MODEL: &str = "jev-latest";

/// Something that answers a System One request.
#[async_trait]
pub trait SystemOne: Send + Sync {
    /// Sends `request` and returns the decoded response body.
    ///
    /// Transport failures map to [`JudgeError::Unavailable`], refusals to
    /// [`JudgeError::Rejected`], and an undecodable body to
    /// [`JudgeError::MalformedReply`].
    async fn ask(&self, request: &wire::Request<'_>) -> Result<wire::Response, JudgeError>;
}

#[async_trait]
impl<T: SystemOne + ?Sized> SystemOne for &T {
    async fn ask(&self, request: &wire::Request<'_>) -> Result<wire::Response, JudgeError> {
        (**self).ask(request).await
    }
}

/// A [`Judge`] over any [`SystemOne`] protocol implementation.
#[derive(Debug)]
pub struct JevJudge<P> {
    protocol: P,
    model: String,
}

impl<P: SystemOne> JevJudge<P> {
    /// Judges through `protocol` with [`DEFAULT_MODEL`].
    pub fn over(protocol: P) -> Self {
        JevJudge {
            protocol,
            model: DEFAULT_MODEL.to_owned(),
        }
    }

    /// Overrides the model alias sent in every request.
    pub fn with_model(mut self, model: impl Into<String>) -> Self {
        self.model = model.into();
        self
    }

    /// The protocol implementation.
    pub fn protocol(&self) -> &P {
        &self.protocol
    }

    /// The model alias.
    pub fn model(&self) -> &str {
        &self.model
    }
}

#[async_trait]
impl<P: SystemOne> Judge for JevJudge<P> {
    async fn judge(
        &self,
        payload: &Payload,
        questions: &QuestionSet,
    ) -> Result<AnswerSet, JudgeError> {
        let request = wire::encode(&self.model, payload, questions);
        let response = self.protocol.ask(&request).await?;
        wire::decode(response)
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;
    use crate::question::{Answer, Question, QuestionName};
    use serde_json::json;
    use std::sync::Mutex;

    /// A protocol double that records the request and replays a fixed body.
    struct Scripted {
        seen: Mutex<Vec<serde_json::Value>>,
        body: serde_json::Value,
    }

    #[async_trait]
    impl SystemOne for Scripted {
        async fn ask(&self, request: &wire::Request<'_>) -> Result<wire::Response, JudgeError> {
            self.seen
                .lock()
                .unwrap()
                .push(serde_json::to_value(request).unwrap());
            serde_json::from_value(self.body.clone()).map_err(|e| JudgeError::MalformedReply {
                detail: e.to_string(),
            })
        }
    }

    #[tokio::test]
    async fn a_judge_over_a_protocol_double_needs_no_network() {
        let protocol = Scripted {
            seen: Mutex::new(Vec::new()),
            body: json!({ "answers": { "billing": { "type": "noul", "noul": 0.9 } } }),
        };
        let judge = JevJudge::over(&protocol).with_model("jev-test");
        let questions = QuestionSet::from_iter([(
            QuestionName::new("billing").unwrap(),
            Question::Noul {
                instructions: "billing?".into(),
            },
        )]);
        let answers = judge
            .judge(&Payload::new("charged twice"), &questions)
            .await
            .unwrap();
        assert!(matches!(
            answers.get(&QuestionName::new("billing").unwrap()),
            Some(Answer::Noul { .. })
        ));
        let seen = protocol.seen.lock().unwrap();
        let first = seen.first().cloned().unwrap_or_default();
        assert_eq!(first.pointer("/model"), Some(&json!("jev-test")));
        assert_eq!(first.pointer("/state"), Some(&json!("charged twice")));
    }
}
