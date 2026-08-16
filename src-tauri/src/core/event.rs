use crate::core::error::ToolboxError;
use crate::core::model::{
    APP_VERSION, AppEvent, AppSnapshot, EventResume, EventSubscription, SessionSnapshot,
    SourceSnapshot, ToolboxProject,
};
use parking_lot::Mutex;
use serde::Serialize;
use serde_json::Value;
use std::collections::{BTreeMap, VecDeque};
use tauri::ipc::{Channel, InvokeResponseBody};
use uuid::Uuid;

const EVENT_RETENTION: usize = 4096;
const ERROR_RETENTION: usize = 200;

struct StateInner {
    cursor: u64,
    recent_events: VecDeque<AppEvent>,
    subscribers: Vec<Channel<AppEvent>>,
    project: ToolboxProject,
    sources: BTreeMap<Uuid, SourceSnapshot>,
    session: SessionSnapshot,
    latest_channel_values: BTreeMap<Uuid, f64>,
    recent_errors: VecDeque<ToolboxError>,
}

pub struct StateStore {
    runtime_instance_id: Uuid,
    inner: Mutex<StateInner>,
}

impl StateStore {
    pub fn new(project: ToolboxProject) -> Self {
        Self {
            runtime_instance_id: Uuid::now_v7(),
            inner: Mutex::new(StateInner {
                cursor: 0,
                recent_events: VecDeque::with_capacity(EVENT_RETENTION),
                subscribers: Vec::new(),
                project,
                sources: BTreeMap::new(),
                session: SessionSnapshot::default(),
                latest_channel_values: BTreeMap::new(),
                recent_errors: VecDeque::with_capacity(ERROR_RETENTION),
            }),
        }
    }

    pub fn runtime_instance_id(&self) -> Uuid {
        self.runtime_instance_id
    }

    pub fn snapshot(&self) -> AppSnapshot {
        let inner = self.inner.lock();
        AppSnapshot {
            runtime_instance_id: self.runtime_instance_id,
            app_version: APP_VERSION.into(),
            event_cursor: inner.cursor,
            project: inner.project.clone(),
            sources: inner.sources.values().cloned().collect(),
            session: inner.session.clone(),
            latest_channel_values: inner.latest_channel_values.clone(),
            recent_errors: inner.recent_errors.iter().cloned().collect(),
        }
    }

    pub fn project(&self) -> ToolboxProject {
        self.inner.lock().project.clone()
    }

    pub fn set_project(&self, project: ToolboxProject) {
        let payload = serde_json::json!({ "projectId": project.id, "name": project.name });
        self.mutate_and_emit("project.changed", payload, |inner| inner.project = project);
    }

    pub fn set_source(&self, source: SourceSnapshot, event_type: &str) {
        let payload = serde_json::to_value(&source).unwrap_or(Value::Null);
        self.mutate_and_emit(event_type, payload, |inner| {
            inner.sources.insert(source.source_id, source);
        });
    }

    pub fn update_source_silent(&self, source: SourceSnapshot) {
        self.inner.lock().sources.insert(source.source_id, source);
    }

    pub fn set_session(&self, session: SessionSnapshot, event_type: &str) {
        let payload = serde_json::to_value(&session).unwrap_or(Value::Null);
        self.mutate_and_emit(event_type, payload, |inner| inner.session = session);
    }

    pub fn update_latest_values(&self, values: impl IntoIterator<Item = (Uuid, f64)>) {
        let mut inner = self.inner.lock();
        for (id, value) in values {
            inner.latest_channel_values.insert(id, value);
        }
    }

    pub fn push_error(&self, error: ToolboxError) {
        let payload = serde_json::to_value(&error).unwrap_or(Value::Null);
        self.mutate_and_emit("error.raised", payload, |inner| {
            if inner.recent_errors.len() == ERROR_RETENTION {
                inner.recent_errors.pop_front();
            }
            inner.recent_errors.push_back(error);
        });
    }

    pub fn emit<T: Serialize>(&self, event_type: &str, payload: T) -> AppEvent {
        let payload = serde_json::to_value(payload).unwrap_or(Value::Null);
        self.mutate_and_emit(event_type, payload, |_| {})
    }

    fn mutate_and_emit<F>(&self, event_type: &str, payload: Value, mutation: F) -> AppEvent
    where
        F: FnOnce(&mut StateInner),
    {
        let (event, subscribers) = {
            let mut inner = self.inner.lock();
            mutation(&mut inner);
            inner.cursor = inner.cursor.saturating_add(1);
            let event = AppEvent {
                runtime_instance_id: self.runtime_instance_id,
                cursor: inner.cursor,
                event_type: event_type.into(),
                payload,
            };
            if inner.recent_events.len() == EVENT_RETENTION {
                inner.recent_events.pop_front();
            }
            inner.recent_events.push_back(event.clone());
            (event, inner.subscribers.clone())
        };
        let mut failed = Vec::new();
        for subscriber in subscribers {
            if subscriber.send(event.clone()).is_err() {
                failed.push(subscriber.id());
            }
        }
        if !failed.is_empty() {
            self.inner
                .lock()
                .subscribers
                .retain(|channel| !failed.contains(&channel.id()));
        }
        event
    }

    pub fn subscribe(
        &self,
        resume: Option<EventResume>,
        channel: Channel<AppEvent>,
    ) -> EventSubscription {
        let mut inner = self.inner.lock();
        let mut resync_required = false;
        if let Some(resume) = resume {
            let earliest = inner
                .recent_events
                .front()
                .map(|event| event.cursor)
                .unwrap_or(inner.cursor.saturating_add(1));
            if resume_requires_snapshot(self.runtime_instance_id, earliest, &resume) {
                resync_required = true;
            } else {
                for event in inner
                    .recent_events
                    .iter()
                    .filter(|event| event.cursor > resume.last_cursor)
                {
                    if channel.send(event.clone()).is_err() {
                        break;
                    }
                }
            }
        }
        inner.subscribers.push(channel);
        EventSubscription {
            runtime_instance_id: self.runtime_instance_id,
            current_cursor: inner.cursor,
            resync_required,
        }
    }
}

fn resume_requires_snapshot(
    runtime_instance_id: Uuid,
    earliest_cursor: u64,
    resume: &EventResume,
) -> bool {
    resume.runtime_instance_id != runtime_instance_id
        || resume.last_cursor.saturating_add(1) < earliest_cursor
}

#[derive(Default)]
pub struct DataHub {
    subscribers: Mutex<Vec<Channel<InvokeResponseBody>>>,
}

impl DataHub {
    pub fn subscribe(&self, channel: Channel<InvokeResponseBody>) -> usize {
        let mut subscribers = self.subscribers.lock();
        subscribers.push(channel);
        subscribers.len()
    }

    pub fn send(&self, body: InvokeResponseBody) {
        let subscribers = self.subscribers.lock().clone();
        let mut failed = Vec::new();
        for subscriber in subscribers {
            if subscriber.send(body.clone()).is_err() {
                failed.push(subscriber.id());
            }
        }
        if !failed.is_empty() {
            self.subscribers
                .lock()
                .retain(|channel| !failed.contains(&channel.id()));
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn snapshot_cursor_matches_mutated_state() {
        let store = StateStore::new(ToolboxProject::demo());
        store.emit("test.one", serde_json::json!({"ok": true}));
        store.update_latest_values([(Uuid::nil(), 42.0)]);
        let snapshot = store.snapshot();
        assert_eq!(snapshot.event_cursor, 1);
        assert_eq!(
            snapshot.latest_channel_values.get(&Uuid::nil()),
            Some(&42.0)
        );
    }

    #[test]
    fn resume_rejects_process_restart_and_expired_cursor() {
        let runtime = Uuid::now_v7();
        assert!(resume_requires_snapshot(
            runtime,
            10,
            &EventResume {
                runtime_instance_id: Uuid::now_v7(),
                last_cursor: 9,
            },
        ));
        assert!(resume_requires_snapshot(
            runtime,
            10,
            &EventResume {
                runtime_instance_id: runtime,
                last_cursor: 1,
            },
        ));
        assert!(!resume_requires_snapshot(
            runtime,
            10,
            &EventResume {
                runtime_instance_id: runtime,
                last_cursor: 9,
            },
        ));
    }
}
