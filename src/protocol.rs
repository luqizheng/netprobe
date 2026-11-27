use std::fmt;
use std::time::{Duration, Instant};

use thiserror::Error;

pub const MAX_MESSAGE_LEN: usize = 1024;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MessageKind {
    Helo,
    Ok,
    Ack,
    End,
}

impl MessageKind {
    pub fn as_str(self) -> &'static str {
        match self {
            MessageKind::Helo => "helo",
            MessageKind::Ok => "ok",
            MessageKind::Ack => "ack",
            MessageKind::End => "end",
        }
    }
}

impl fmt::Display for MessageKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Message {
    Helo(u64),
    Ok(u64),
    Ack(u64),
    End(u64),
}

impl Message {
    pub fn kind(self) -> MessageKind {
        match self {
            Message::Helo(_) => MessageKind::Helo,
            Message::Ok(_) => MessageKind::Ok,
            Message::Ack(_) => MessageKind::Ack,
            Message::End(_) => MessageKind::End,
        }
    }

    pub fn seq(self) -> u64 {
        match self {
            Message::Helo(n) | Message::Ok(n) | Message::Ack(n) | Message::End(n) => n,
        }
    }

    pub fn helo(seq: u64) -> Self {
        Message::Helo(seq)
    }

    pub fn ok(seq: u64) -> Self {
        Message::Ok(seq)
    }

    pub fn ack(seq: u64) -> Self {
        Message::Ack(seq)
    }

    pub fn end(seq: u64) -> Self {
        Message::End(seq)
    }

    pub fn parse(s: &str) -> Result<Self, ProtocolError> {
        let trimmed = s.trim();
        if trimmed.is_empty() {
            return Err(ProtocolError::Empty);
        }
        let Some(open) = trimmed.find('(') else {
            return Err(ProtocolError::Invalid(trimmed.to_owned()));
        };
        if !trimmed.ends_with(')') {
            return Err(ProtocolError::Invalid(trimmed.to_owned()));
        }
        let kind_part = &trimmed[..open];
        let seq_part = &trimmed[open + 1..trimmed.len() - 1];
        if seq_part.is_empty() || !seq_part.bytes().all(|b| b.is_ascii_digit()) {
            return Err(ProtocolError::Invalid(trimmed.to_owned()));
        }
        let seq = seq_part
            .parse::<u64>()
            .map_err(|_| ProtocolError::Invalid(trimmed.to_owned()))?;
        let msg = match kind_part {
            "helo" => Message::Helo(seq),
            "ok" => Message::Ok(seq),
            "ack" => Message::Ack(seq),
            "end" => Message::End(seq),
            _ => return Err(ProtocolError::Invalid(trimmed.to_owned())),
        };
        Ok(msg)
    }
}

impl fmt::Display for Message {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match *self {
            Message::Helo(n) => write!(f, "helo({n})"),
            Message::Ok(n) => write!(f, "ok({n})"),
            Message::Ack(n) => write!(f, "ack({n})"),
            Message::End(n) => write!(f, "end({n})"),
        }
    }
}

#[derive(Debug, Error, PartialEq)]
pub enum ProtocolError {
    #[error("empty message")]
    Empty,
    #[error("invalid message: {0}")]
    Invalid(String),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HeloDecision {
    NewRound,
    Duplicate,
    Expired { current: u64 },
    Superseded { abandoned: u64 },
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum AckDecision {
    Complete { round: u64, elapsed_ms: f64 },
    Duplicate { round: u64, elapsed_ms: f64 },
    Mismatch { received: u64, current: Option<u64> },
}

/// Server-side per-client round state machine, shared by TCP and UDP transports.
///
/// Timing rules:
/// - `started` is set exactly once, when a new (strictly increasing) round number
///   is first observed via `helo(n)`. Duplicate `helo(n)` never resets it.
/// - `last_elapsed_ms` is recorded exactly once, when `end(n)` is first sent in
///   response to `ack(n)`. Duplicate `ack(n)` resends `end(n)` without recomputing.
#[derive(Debug)]
pub struct RoundTracker {
    current: Option<u64>,
    started: Option<Instant>,
    completed: bool,
    last_elapsed_ms: Option<f64>,
    last_active: Instant,
}

impl Default for RoundTracker {
    fn default() -> Self {
        Self {
            current: None,
            started: None,
            completed: false,
            last_elapsed_ms: None,
            last_active: Instant::now(),
        }
    }
}

impl RoundTracker {
    pub fn new() -> Self {
        Self::default()
    }

    fn start_round(&mut self, seq: u64) {
        self.current = Some(seq);
        self.started = Some(Instant::now());
        self.completed = false;
        self.last_elapsed_ms = None;
    }

    pub fn on_helo(&mut self, seq: u64) -> HeloDecision {
        self.last_active = Instant::now();
        match self.current {
            Some(c) if c == seq => HeloDecision::Duplicate,
            Some(c) if c > seq => HeloDecision::Expired { current: c },
            Some(c) => {
                let abandoned = if self.completed { None } else { Some(c) };
                self.start_round(seq);
                match abandoned {
                    Some(a) => HeloDecision::Superseded { abandoned: a },
                    None => HeloDecision::NewRound,
                }
            }
            None => {
                self.start_round(seq);
                HeloDecision::NewRound
            }
        }
    }

    pub fn on_ack(&mut self, seq: u64) -> AckDecision {
        self.last_active = Instant::now();
        match self.current {
            Some(c) if c == seq => {
                if self.completed {
                    AckDecision::Duplicate {
                        round: seq,
                        elapsed_ms: self.last_elapsed_ms.unwrap_or(0.0),
                    }
                } else {
                    let elapsed_ms = self
                        .started
                        .map(|s| s.elapsed().as_secs_f64() * 1000.0)
                        .unwrap_or(0.0);
                    self.completed = true;
                    self.last_elapsed_ms = Some(elapsed_ms);
                    AckDecision::Complete {
                        round: seq,
                        elapsed_ms,
                    }
                }
            }
            _ => AckDecision::Mismatch {
                received: seq,
                current: self.current,
            },
        }
    }

    pub fn unfinished_round(&self) -> Option<u64> {
        if self.completed {
            None
        } else {
            self.current
        }
    }

    pub fn is_idle(&self, limit: Duration) -> bool {
        self.last_active.elapsed() > limit
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::thread::sleep;

    #[test]
    fn parse_helo_ok_ack_end() {
        assert_eq!(Message::parse("helo(1)\n"), Ok(Message::helo(1)));
        assert_eq!(Message::parse(" ok(42) "), Ok(Message::ok(42)));
        assert_eq!(
            Message::parse("ack(18446744073709551615)"),
            Ok(Message::ack(u64::MAX))
        );
        assert_eq!(Message::parse("end(0)"), Ok(Message::end(0)));
    }

    #[test]
    fn parse_rejects_invalid_messages() {
        for bad in [
            "",
            "   ",
            "\n",
            "hello(1)",
            "helo",
            "helo(",
            "helo)",
            "helo()",
            "helo(1a)",
            "helo(1",
            "helo(1))",
            "helo(+1)",
            "helo(-1)",
            "HELO(1)",
            "helo(1) tail",
            "helo 1",
            "(1)",
            "ok() ",
        ] {
            assert!(
                Message::parse(bad).is_err(),
                "expected rejection of {bad:?}"
            );
        }
    }

    #[test]
    fn parse_rejects_seq_overflow() {
        assert!(Message::parse("helo(99999999999999999999999)").is_err());
    }

    #[test]
    fn display_roundtrip() {
        assert_eq!(Message::helo(7).to_string(), "helo(7)");
        assert_eq!(Message::ok(7).to_string(), "ok(7)");
        assert_eq!(Message::ack(7).to_string(), "ack(7)");
        assert_eq!(Message::end(7).to_string(), "end(7)");
        assert_eq!(
            Message::parse(&Message::end(9).to_string()),
            Ok(Message::end(9))
        );
    }

    #[test]
    fn tracker_new_round_then_ack_completes() {
        let mut t = RoundTracker::new();
        assert_eq!(t.on_helo(1), HeloDecision::NewRound);
        sleep(std::time::Duration::from_millis(5));
        match t.on_ack(1) {
            AckDecision::Complete { round, elapsed_ms } => {
                assert_eq!(round, 1);
                assert!(elapsed_ms >= 5.0);
            }
            other => panic!("expected Complete, got {other:?}"),
        }
        assert_eq!(t.unfinished_round(), None);
    }

    #[test]
    fn tracker_duplicate_helo_does_not_reset_timing() {
        let mut t = RoundTracker::new();
        assert_eq!(t.on_helo(1), HeloDecision::NewRound);
        sleep(std::time::Duration::from_millis(5));
        assert_eq!(t.on_helo(1), HeloDecision::Duplicate);
        match t.on_ack(1) {
            AckDecision::Complete { elapsed_ms, .. } => assert!(elapsed_ms >= 5.0),
            other => panic!("expected Complete, got {other:?}"),
        }
    }

    #[test]
    fn tracker_duplicate_ack_resends_without_recomputing() {
        let mut t = RoundTracker::new();
        assert_eq!(t.on_helo(1), HeloDecision::NewRound);
        let first = match t.on_ack(1) {
            AckDecision::Complete { round, elapsed_ms } => {
                assert_eq!(round, 1);
                elapsed_ms
            }
            other => panic!("expected Complete, got {other:?}"),
        };
        sleep(std::time::Duration::from_millis(5));
        match t.on_ack(1) {
            AckDecision::Duplicate { round, elapsed_ms } => {
                assert_eq!(round, 1);
                assert!((elapsed_ms - first).abs() < f64::EPSILON);
            }
            other => panic!("expected Duplicate, got {other:?}"),
        }
    }

    #[test]
    fn tracker_mismatched_ack_is_rejected() {
        let mut t = RoundTracker::new();
        assert_eq!(t.on_helo(2), HeloDecision::NewRound);
        assert_eq!(
            t.on_ack(1),
            AckDecision::Mismatch {
                received: 1,
                current: Some(2)
            }
        );
        assert_eq!(
            t.on_ack(3),
            AckDecision::Mismatch {
                received: 3,
                current: Some(2)
            }
        );
    }

    #[test]
    fn tracker_expired_helo_is_rejected() {
        let mut t = RoundTracker::new();
        assert_eq!(t.on_helo(3), HeloDecision::NewRound);
        assert_eq!(t.on_helo(2), HeloDecision::Expired { current: 3 });
        assert_eq!(t.unfinished_round(), Some(3));
    }

    #[test]
    fn tracker_superseded_abandons_unfinished_round() {
        let mut t = RoundTracker::new();
        assert_eq!(t.on_helo(1), HeloDecision::NewRound);
        assert_eq!(t.on_helo(2), HeloDecision::Superseded { abandoned: 1 });
        assert_eq!(
            t.on_ack(1),
            AckDecision::Mismatch {
                received: 1,
                current: Some(2)
            }
        );
        match t.on_ack(2) {
            AckDecision::Complete { round, .. } => assert_eq!(round, 2),
            other => panic!("expected Complete, got {other:?}"),
        }
    }

    #[test]
    fn tracker_next_round_after_completed_is_new_round() {
        let mut t = RoundTracker::new();
        assert_eq!(t.on_helo(1), HeloDecision::NewRound);
        assert!(matches!(t.on_ack(1), AckDecision::Complete { .. }));
        assert_eq!(t.on_helo(2), HeloDecision::NewRound);
        assert_eq!(t.unfinished_round(), Some(2));
    }

    #[test]
    fn tracker_idle_detection() {
        let t = RoundTracker::new();
        assert!(t.is_idle(Duration::ZERO));
        assert!(!t.is_idle(Duration::from_secs(3600)));
    }
}
