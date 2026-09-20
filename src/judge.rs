//! The judge boundary: something that answers typed questions about a payload.

use async_trait::async_trait;

use crate::event::Payload;
use crate::question::{AnswerSet, QuestionName, QuestionSet};

/// A model or rule engine that answers a [`QuestionSet`] about a [`Payload`].
///
/// Implementations may perform IO. They must answer every question in the
/// set; the router reports a missing or mistyped answer as a
/// [`crate::RoutingError`].
#[async_trait]
pub trait Judge: Send + Sync {
    /// Answers `questions` about `payload`.
    async fn judge(
        &self,
        payload: &Payload,
        questions: &QuestionSet,
    ) -> Result<AnswerSet, JudgeError>;
}

/// Why a judge could not produce answers.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum JudgeError {
    /// The judge could not be reached.
    #[error("judge unavailable: {reason}")]
    Unavailable {
        /// Transport-level detail.
        reason: String,
    },
    /// The judge refused the request.
    #[error("judge rejected the request with status {status}: {message}")]
    Rejected {
        /// Protocol status code, when the transport has one.
        status: u16,
        /// The judge's message.
        message: String,
    },
    /// The judge replied, but one answer could not be decoded.
    #[error("malformed answer to {question}: {detail}")]
    MalformedAnswer {
        /// The question whose answer was malformed.
        question: QuestionName,
        /// What was wrong.
        detail: String,
    },
    /// The judge replied with something that was not an answer set at all.
    #[error("malformed reply: {detail}")]
    MalformedReply {
        /// What was wrong.
        detail: String,
    },
    /// The judge did not answer within the configured limit.
    #[error("judge timed out after {after:?}")]
    TimedOut {
        /// The limit that elapsed.
        after: std::time::Duration,
    },
}

impl JudgeError {
    /// Semantic predicate: a later attempt may succeed without any change on
    /// our side. Transport failures, timeouts, throttling and server errors
    /// are transient; client errors and malformed replies are not.
    pub fn is_transient(&self) -> bool {
        match self {
            JudgeError::Unavailable { .. } | JudgeError::TimedOut { .. } => true,
            JudgeError::Rejected { status, .. } => is_transient_status(*status),
            JudgeError::MalformedAnswer { .. } | JudgeError::MalformedReply { .. } => false,
        }
    }
}

/// HTTP statuses that signal a temporary condition.
fn is_transient_status(status: u16) -> bool {
    matches!(status, 408 | 429) || (500..=599).contains(&status)
}

// A shared reference to a judge is a judge: the bus may borrow one that the
// caller keeps for inspection.
#[async_trait]
impl<T: Judge + ?Sized> Judge for &T {
    async fn judge(
        &self,
        payload: &Payload,
        questions: &QuestionSet,
    ) -> Result<AnswerSet, JudgeError> {
        (**self).judge(payload, questions).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn transient_errors_are_transport_timeout_throttle_and_server_side() {
        let rejected = |status| JudgeError::Rejected {
            status,
            message: String::new(),
        };
        assert!(JudgeError::Unavailable {
            reason: String::new()
        }
        .is_transient());
        assert!(JudgeError::TimedOut {
            after: std::time::Duration::ZERO
        }
        .is_transient());
        assert!(rejected(429).is_transient());
        assert!(rejected(503).is_transient());
        assert!(!rejected(400).is_transient());
        assert!(!rejected(401).is_transient());
        assert!(!JudgeError::MalformedReply {
            detail: String::new()
        }
        .is_transient());
    }
}
