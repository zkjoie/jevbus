//! Events published on the bus.

use std::fmt;

use serde::{Deserialize, Deserializer, Serialize};

/// Identity of an event.
///
/// Invariant: non-empty. Deserialization goes through [`EventId::new`].
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize)]
#[serde(transparent)]
pub struct EventId(String);

impl<'de> Deserialize<'de> for EventId {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let raw = String::deserialize(deserializer)?;
        EventId::new(raw).map_err(serde::de::Error::custom)
    }
}

/// An identifier was empty.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[error("identifier must not be empty")]
pub struct EmptyId;

impl EventId {
    /// Validates that `id` is non-empty.
    pub fn new(id: impl Into<String>) -> Result<Self, EmptyId> {
        let id = id.into();
        if id.is_empty() {
            Err(EmptyId)
        } else {
            Ok(EventId(id))
        }
    }

    /// The identifier text.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for EventId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

/// The content a judge evaluates. Jev calls this the `state`.
///
/// The bus treats the payload as opaque text; producers serialise structured
/// data before publishing.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(transparent)]
pub struct Payload(String);

impl Payload {
    /// Wraps arbitrary text. Empty payloads are allowed; the judge decides
    /// what to make of them.
    pub fn new(text: impl Into<String>) -> Self {
        Payload(text.into())
    }

    /// The payload text.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// An event: an identity, the payload to judge, and lineage.
///
/// `parts` is empty for an event that arrived from outside and lists the
/// direct constituents of an event produced by merging. Nested lineage is
/// recovered from the ledger's `Composed` rows of those parts.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Event {
    id: EventId,
    payload: Payload,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    parts: Vec<EventId>,
}

impl Event {
    /// Builds an event with no lineage.
    pub fn new(id: EventId, payload: Payload) -> Self {
        Event {
            id,
            payload,
            parts: Vec::new(),
        }
    }

    /// Builds an event that stands for `parts`.
    ///
    /// Post: `event.parts()` lists the ids of `parts` in the given order.
    pub fn merged<'a>(
        id: EventId,
        payload: Payload,
        parts: impl IntoIterator<Item = &'a Event>,
    ) -> Self {
        Event {
            id,
            payload,
            parts: parts.into_iter().map(|part| part.id.clone()).collect(),
        }
    }

    /// Ids of the events this one was merged from. Empty for an external event.
    pub fn parts(&self) -> &[EventId] {
        &self.parts
    }

    /// Semantic predicate: this event was produced by merging.
    pub fn is_composite(&self) -> bool {
        !self.parts.is_empty()
    }

    /// The event identity.
    pub fn id(&self) -> &EventId {
        &self.id
    }

    /// The payload the judge evaluates.
    pub fn payload(&self) -> &Payload {
        &self.payload
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn event_id_rejects_empty_text() {
        assert_eq!(EventId::new(""), Err(EmptyId));
        assert!(EventId::new("e-1").is_ok());
    }

    #[test]
    fn merged_event_lists_its_parts_in_order() {
        let (Ok(a), Ok(b), Ok(m)) = (EventId::new("a"), EventId::new("b"), EventId::new("m"))
        else {
            return;
        };
        let parts = [
            Event::new(a.clone(), Payload::new("x")),
            Event::new(b.clone(), Payload::new("y")),
        ];
        let merged = Event::merged(m, Payload::new("x y"), &parts);
        assert_eq!(merged.parts(), [a, b]);
        assert!(merged.is_composite());
        assert!(!parts.iter().any(Event::is_composite));
    }
}
