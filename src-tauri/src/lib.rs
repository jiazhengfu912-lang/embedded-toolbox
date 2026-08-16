#![allow(clippy::result_large_err)] // ToolboxError intentionally carries structured IPC context.
#![allow(clippy::too_many_arguments)] // Worker construction keeps ownership dependencies explicit.
#![allow(clippy::enum_variant_names)] // Payload type names are part of the versioned wire contract.
#![allow(clippy::collapsible_if)] // Nested branches preserve queue/error policy boundaries.

mod core;

use crate::core::{
    AppCore, AppEvent, AppSnapshot, EventResume, EventSubscription, SerialConfig,
    SerialPortDescriptor, SessionSnapshot, SourceSnapshot, SyntheticConfig, ToolboxError,
    ToolboxProject, ToolboxResult, TxAccepted, TxRequest,
};
use std::path::PathBuf;
use std::sync::Arc;
use tauri::ipc::{Channel, InvokeResponseBody};
use tauri::{AppHandle, Manager, State};
use uuid::Uuid;

#[tauri::command]
fn app_get_snapshot(core: State<'_, Arc<AppCore>>) -> AppSnapshot {
    core.get_snapshot()
}

#[tauri::command]
fn app_subscribe_events(
    core: State<'_, Arc<AppCore>>,
    resume: Option<EventResume>,
    events: Channel<AppEvent>,
) -> EventSubscription {
    core.subscribe_events(resume, events)
}

#[tauri::command]
fn stream_subscribe(core: State<'_, Arc<AppCore>>, stream: Channel<InvokeResponseBody>) -> usize {
    core.subscribe_stream(stream)
}

#[tauri::command]
fn device_list_ports(core: State<'_, Arc<AppCore>>) -> ToolboxResult<Vec<SerialPortDescriptor>> {
    core.list_ports()
}

#[tauri::command]
fn device_connect_serial(
    core: State<'_, Arc<AppCore>>,
    config: SerialConfig,
) -> ToolboxResult<SourceSnapshot> {
    core.connect_serial(config)
}

#[tauri::command]
fn device_connect_synthetic(
    core: State<'_, Arc<AppCore>>,
    config: SyntheticConfig,
) -> ToolboxResult<SourceSnapshot> {
    core.connect_synthetic(config)
}

#[tauri::command]
fn device_disconnect(
    core: State<'_, Arc<AppCore>>,
    source_id: Uuid,
) -> ToolboxResult<SourceSnapshot> {
    core.disconnect(source_id)
}

#[tauri::command]
fn device_send(core: State<'_, Arc<AppCore>>, request: TxRequest) -> ToolboxResult<TxAccepted> {
    core.send(request)
}

#[tauri::command]
fn tx_cancel(core: State<'_, Arc<AppCore>>, source_id: Uuid, job_id: Uuid) -> ToolboxResult<()> {
    core.cancel_tx(source_id, job_id)
}

#[tauri::command]
fn project_set(core: State<'_, Arc<AppCore>>, project: ToolboxProject) -> ToolboxResult<()> {
    core.set_project(project)
}

#[tauri::command]
fn project_load(core: State<'_, Arc<AppCore>>, path: String) -> ToolboxResult<ToolboxProject> {
    core.load_project(&PathBuf::from(path))
}

#[tauri::command]
fn project_save(core: State<'_, Arc<AppCore>>, path: String) -> ToolboxResult<()> {
    core.save_project(&PathBuf::from(path))
}

#[tauri::command]
fn session_start(core: State<'_, Arc<AppCore>>, path: String) -> ToolboxResult<SessionSnapshot> {
    core.start_session(PathBuf::from(path))
}

#[tauri::command]
fn session_start_default(
    app: AppHandle,
    core: State<'_, Arc<AppCore>>,
) -> ToolboxResult<SessionSnapshot> {
    let documents = app.path().document_dir().map_err(|error| {
        ToolboxError::new(
            crate::core::ErrorCode::SessionOpen,
            "session.defaultPath",
            "documents_directory_missing",
        )
        .cause(error)
    })?;
    let stamp = crate::core::session::utc_now_ns() / 1_000_000;
    core.start_session(
        documents
            .join("Embedded Toolbox")
            .join("Sessions")
            .join(format!("session-{stamp}.etdb")),
    )
}

#[tauri::command]
fn session_stop(core: State<'_, Arc<AppCore>>) -> ToolboxResult<SessionSnapshot> {
    core.stop_session()
}

#[tauri::command]
fn session_export_csv(
    core: State<'_, Arc<AppCore>>,
    session_path: String,
    csv_path: String,
) -> ToolboxResult<u64> {
    core.export_csv(&PathBuf::from(session_path), &PathBuf::from(csv_path))
}

#[tauri::command]
fn replay_start(core: State<'_, Arc<AppCore>>, path: String, speed: f64) -> ToolboxResult<()> {
    core.start_replay(PathBuf::from(path), speed)
}

#[tauri::command]
fn replay_stop(core: State<'_, Arc<AppCore>>) {
    core.stop_replay();
}

#[tauri::command]
fn replay_seek(core: State<'_, Arc<AppCore>>, session_offset_ns: i64) -> ToolboxResult<()> {
    core.seek_replay(session_offset_ns)
}

#[cfg_attr(mobile, tauri::mobile_entry_point)]
pub fn run() {
    tauri::Builder::default()
        .manage(AppCore::new())
        .plugin(tauri_plugin_opener::init())
        .invoke_handler(tauri::generate_handler![
            app_get_snapshot,
            app_subscribe_events,
            stream_subscribe,
            device_list_ports,
            device_connect_serial,
            device_connect_synthetic,
            device_disconnect,
            device_send,
            tx_cancel,
            project_set,
            project_load,
            project_save,
            session_start,
            session_start_default,
            session_stop,
            session_export_csv,
            replay_start,
            replay_stop,
            replay_seek,
        ])
        .run(tauri::generate_context!())
        .expect("failed to run Embedded Toolbox");
}
