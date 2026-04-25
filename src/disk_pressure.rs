//! Shared disk-pressure machinery: state machine + command spawner.
//!
//! Both `seibi monitor` (full health daemon with webhook surface) and
//! `seibi watch` (lightweight disk-only daemon) consume this module so the
//! threshold-with-hysteresis semantics + command-spawn contract stay
//! identical regardless of which surface fires them.

use std::process::{Command, Stdio};
use tracing::{info, warn};

/// 1-bit state machine. Fires `Pressured` exactly once per below→above
/// crossing, fires `Cleared` once when usage drops below the hysteresis
/// floor, and stays silent otherwise. This prevents flapping when usage
/// hovers near the threshold.
#[derive(Debug, Default)]
pub struct DiskPressureState {
    above_threshold: bool,
}

#[derive(Debug, PartialEq, Eq)]
pub enum DiskTransition {
    Pressured,
    Cleared,
}

impl DiskPressureState {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Feed an observation in. Returns `Some(transition)` exactly when state
    /// flips, else `None`. `threshold` is the firing line; `clear` is the
    /// hysteresis floor (must be `<= threshold`).
    pub fn observe(&mut self, current: f64, threshold: f64, clear: f64) -> Option<DiskTransition> {
        if current >= threshold && !self.above_threshold {
            self.above_threshold = true;
            return Some(DiskTransition::Pressured);
        }
        if current < clear && self.above_threshold {
            self.above_threshold = false;
            return Some(DiskTransition::Cleared);
        }
        None
    }
}

/// Spawn a whitespace-tokenised command line detached from the caller.
/// We deliberately don't pull in a full shlex parser — these triggers are
/// configured in declarative Nix and the command lines are short, so
/// "split on whitespace" is the contract.
pub fn spawn_pressure_command(cmdline: &str) {
    let mut parts = cmdline.split_whitespace();
    let Some(program) = parts.next() else {
        warn!("on-disk-pressure entry is empty — skipping");
        return;
    };
    let argv: Vec<&str> = parts.collect();
    match Command::new(program)
        .args(&argv)
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
    {
        Ok(child) => info!(program, args = ?argv, pid = child.id(), "spawned pressure command"),
        Err(e) => warn!(program, error = %e, "failed to spawn pressure command"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pressured_fires_once_on_crossing() {
        let mut s = DiskPressureState::new();
        assert_eq!(s.observe(70.0, 85.0, 80.0), None);
        assert_eq!(s.observe(86.0, 85.0, 80.0), Some(DiskTransition::Pressured));
        // Stays above — must not refire
        assert_eq!(s.observe(90.0, 85.0, 80.0), None);
        assert_eq!(s.observe(86.0, 85.0, 80.0), None);
    }

    #[test]
    fn cleared_only_below_hysteresis_floor() {
        let mut s = DiskPressureState::new();
        s.observe(86.0, 85.0, 80.0);
        // Drop just below threshold but above clear — still pressured
        assert_eq!(s.observe(82.0, 85.0, 80.0), None);
        // Drop below clear — fires Cleared
        assert_eq!(s.observe(79.0, 85.0, 80.0), Some(DiskTransition::Cleared));
        // Re-cross — fires again
        assert_eq!(s.observe(86.0, 85.0, 80.0), Some(DiskTransition::Pressured));
    }

    #[test]
    fn at_exactly_threshold_is_pressured() {
        let mut s = DiskPressureState::new();
        assert_eq!(s.observe(85.0, 85.0, 80.0), Some(DiskTransition::Pressured));
    }

    #[test]
    fn never_above_means_no_clear_event() {
        let mut s = DiskPressureState::new();
        assert_eq!(s.observe(50.0, 85.0, 80.0), None);
        assert_eq!(s.observe(10.0, 85.0, 80.0), None);
    }
}
