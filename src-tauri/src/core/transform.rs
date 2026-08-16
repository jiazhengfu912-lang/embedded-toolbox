use crate::core::model::{ResetReason, TransformSpec};
use std::collections::{HashMap, VecDeque};
use uuid::Uuid;

enum TransformState {
    Ema(Option<f64>),
    MovingAverage { values: VecDeque<f64>, sum: f64 },
    Derivative(Option<(f64, i64)>),
}

#[derive(Default)]
pub struct TransformEngine {
    states: HashMap<(Uuid, Uuid, Uuid), TransformState>,
    pub last_reset: Option<ResetReason>,
    pub warming: bool,
}

impl TransformEngine {
    pub fn apply(
        &mut self,
        source_id: Uuid,
        channel_id: Uuid,
        specs: &[TransformSpec],
        mut value: f64,
        timestamp_ns: i64,
    ) -> f64 {
        for spec in specs {
            match spec {
                TransformSpec::ScaleBias { scale, bias, .. } => value = value * scale + bias,
                TransformSpec::Ema { id, alpha } => {
                    let state = self
                        .states
                        .entry((source_id, channel_id, *id))
                        .or_insert(TransformState::Ema(None));
                    if let TransformState::Ema(previous) = state {
                        value = previous
                            .map(|old| {
                                alpha.clamp(0.0, 1.0) * value + (1.0 - alpha.clamp(0.0, 1.0)) * old
                            })
                            .unwrap_or(value);
                        *previous = Some(value);
                    }
                }
                TransformSpec::MovingAverage { id, window } => {
                    let state = self
                        .states
                        .entry((source_id, channel_id, *id))
                        .or_insert_with(|| TransformState::MovingAverage {
                            values: VecDeque::new(),
                            sum: 0.0,
                        });
                    if let TransformState::MovingAverage { values, sum } = state {
                        values.push_back(value);
                        *sum += value;
                        while values.len() > (*window).max(1) {
                            if let Some(removed) = values.pop_front() {
                                *sum -= removed;
                            }
                        }
                        value = *sum / values.len() as f64;
                    }
                }
                TransformSpec::Derivative { id } => {
                    let state = self
                        .states
                        .entry((source_id, channel_id, *id))
                        .or_insert(TransformState::Derivative(None));
                    if let TransformState::Derivative(previous) = state {
                        let next = previous
                            .and_then(|(old_value, old_time)| {
                                let dt = (timestamp_ns - old_time) as f64 / 1_000_000_000.0;
                                (dt > 0.0).then_some((value - old_value) / dt)
                            })
                            .unwrap_or(0.0);
                        *previous = Some((value, timestamp_ns));
                        value = next;
                    }
                }
            }
        }
        self.warming = false;
        value
    }

    pub fn reset(&mut self, reason: ResetReason) {
        self.states.clear();
        self.last_reset = Some(reason);
        self.warming = matches!(
            reason,
            ResetReason::ReplaySeek | ResetReason::StreamGap | ResetReason::EpochChanged
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn reset_clears_stateful_transform_history() {
        let channel = Uuid::now_v7();
        let source = Uuid::now_v7();
        let spec = TransformSpec::Ema {
            id: Uuid::now_v7(),
            alpha: 0.5,
        };
        let mut engine = TransformEngine::default();
        assert_eq!(
            engine.apply(source, channel, std::slice::from_ref(&spec), 10.0, 0),
            10.0
        );
        assert_eq!(
            engine.apply(source, channel, std::slice::from_ref(&spec), 20.0, 1),
            15.0
        );
        engine.reset(ResetReason::EpochChanged);
        assert_eq!(engine.apply(source, channel, &[spec], 20.0, 2), 20.0);
    }
}
