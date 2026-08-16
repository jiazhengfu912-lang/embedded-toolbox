use crate::core::error::{ErrorCode, ToolboxError, ToolboxResult};
use crate::core::event::StateStore;
use crate::core::model::{
    Direction, EpochIdentity, RawChunk, ResetReason, SessionSnapshot, SessionStatus,
    SourceSnapshot, ToolboxProject,
};
use crate::core::pipeline::Pipeline;
use parking_lot::Mutex;
use rusqlite::{Connection, OpenFlags, params};
use std::fs;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use uuid::Uuid;

const CHECKPOINT_INTERVAL: Duration = Duration::from_secs(30);
const CHECKPOINT_WAL_BYTES: u64 = 32 * 1024 * 1024;
const CHECKPOINT_STOP_BUDGET: Duration = Duration::from_secs(5);

struct Recorder {
    conn: Connection,
    path: PathBuf,
    session_id: Uuid,
    runtime_instance_id: Uuid,
    session_anchor_monotonic_ns: i64,
    current_epoch_id: Option<Uuid>,
    epoch_ordinal: u32,
    bytes_written: u64,
    last_checkpoint: Instant,
}

struct SessionInner {
    snapshot: SessionSnapshot,
    recorder: Option<Recorder>,
}

pub struct SessionManager {
    inner: Mutex<SessionInner>,
    state: Arc<StateStore>,
}

#[derive(Debug, Clone)]
pub struct ReplayChunk {
    pub source_id: Uuid,
    pub source_epoch: u64,
    pub sequence: u64,
    pub monotonic_offset_ns: i64,
    pub session_offset_ns: i64,
    pub direction: Direction,
    pub bytes: Vec<u8>,
    pub epoch_changed: bool,
}

pub struct ReplayData {
    pub project: ToolboxProject,
    pub chunks: Vec<ReplayChunk>,
}

impl SessionManager {
    pub fn new(state: Arc<StateStore>) -> Self {
        Self {
            inner: Mutex::new(SessionInner {
                snapshot: SessionSnapshot::default(),
                recorder: None,
            }),
            state,
        }
    }

    pub fn snapshot(&self) -> SessionSnapshot {
        self.inner.lock().snapshot.clone()
    }

    pub fn is_active(&self) -> bool {
        matches!(
            self.inner.lock().snapshot.status,
            SessionStatus::Recording | SessionStatus::Suspended | SessionStatus::Finalizing
        )
    }

    pub fn start(
        &self,
        path: PathBuf,
        project: &ToolboxProject,
        source: &SourceSnapshot,
        session_anchor_monotonic_ns: i64,
    ) -> ToolboxResult<SessionSnapshot> {
        let mut inner = self.inner.lock();
        if self.is_active_locked(&inner) {
            return Err(ToolboxError::new(
                ErrorCode::SessionState,
                "session.start",
                "session_already_active",
            ));
        }
        if source.status != crate::core::model::SourceStatus::Connected {
            return Err(ToolboxError::new(
                ErrorCode::SessionState,
                "session.start",
                "source_not_connected",
            ));
        }
        if path.exists() {
            return Err(ToolboxError::new(
                ErrorCode::SessionOpen,
                "session.start",
                "session_path_exists",
            )
            .context("path", path.display().to_string()));
        }
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent).map_err(|error| {
                ToolboxError::new(
                    ErrorCode::SessionOpen,
                    "session.start",
                    "session_directory_failed",
                )
                .cause(error)
            })?;
        }
        let conn = Connection::open_with_flags(
            &path,
            OpenFlags::SQLITE_OPEN_READ_WRITE | OpenFlags::SQLITE_OPEN_CREATE,
        )
        .map_err(|error| {
            ToolboxError::new(
                ErrorCode::SessionOpen,
                "session.start",
                "session_open_failed",
            )
            .cause(error)
        })?;
        configure_database(&conn)?;
        create_schema(&conn)?;
        let session_id = Uuid::now_v7();
        let started_utc = utc_now_ns();
        let project_json = serde_json::to_string(project).map_err(|error| {
            ToolboxError::new(
                ErrorCode::SessionWrite,
                "session.start",
                "project_snapshot_failed",
            )
            .cause(error)
        })?;
        let runtime_instance_id = self.state.runtime_instance_id();
        conn.execute(
            "INSERT INTO sessions(id, status, started_utc_ns, utc_anchor_unix_ns, monotonic_anchor_ns, runtime_instance_id, project_json, project_schema_version, pipeline_semantic_version, app_version) VALUES (?1,'Recording',?2,?2,?3,?4,?5,?6,?7,?8)",
            params![session_id.to_string(), started_utc, session_anchor_monotonic_ns, runtime_instance_id.to_string(), project_json, project.project_schema_version, project.pipeline_semantic_version, env!("CARGO_PKG_VERSION")],
        ).map_err(|error| ToolboxError::new(ErrorCode::SessionWrite, "session.start", "session_insert_failed").cause(error))?;
        let mut recorder = Recorder {
            conn,
            path: path.clone(),
            session_id,
            runtime_instance_id,
            session_anchor_monotonic_ns,
            current_epoch_id: None,
            epoch_ordinal: 0,
            bytes_written: 0,
            last_checkpoint: Instant::now(),
        };
        begin_epoch(&mut recorder, source)?;
        let snapshot = SessionSnapshot {
            status: SessionStatus::Recording,
            session_id: Some(session_id),
            path: Some(path.display().to_string()),
            epoch_ordinal: Some(recorder.epoch_ordinal),
            bytes_written: 0,
            checkpoint_pending: false,
            message: None,
        };
        inner.snapshot = snapshot.clone();
        inner.recorder = Some(recorder);
        drop(inner);
        self.state.set_session(snapshot.clone(), "session.started");
        Ok(snapshot)
    }

    pub fn append_batch(&self, chunks: &[RawChunk]) -> ToolboxResult<()> {
        if chunks.is_empty() {
            return Ok(());
        }
        let mut inner = self.inner.lock();
        if inner.snapshot.status != SessionStatus::Recording {
            return Ok(());
        }
        let bytes_written = {
            let recorder = inner.recorder.as_mut().ok_or_else(|| {
                ToolboxError::new(
                    ErrorCode::SessionState,
                    "session.append",
                    "recorder_missing",
                )
            })?;
            let epoch_id = recorder.current_epoch_id.ok_or_else(|| {
                ToolboxError::new(ErrorCode::SessionState, "session.append", "epoch_missing")
            })?;
            let tx = recorder.conn.transaction().map_err(|error| {
                ToolboxError::new(
                    ErrorCode::SessionWrite,
                    "session.append",
                    "transaction_begin_failed",
                )
                .cause(error)
            })?;
            let mut batch_bytes = 0u64;
            {
                let mut statement = tx.prepare_cached(
                    "INSERT INTO raw_chunks(session_id, epoch_id, source_id, source_epoch, sequence, direction, monotonic_offset_ns, bytes, tx_job_id) VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9)"
                ).map_err(|error| ToolboxError::new(ErrorCode::SessionWrite, "session.append", "statement_prepare_failed").cause(error))?;
                for chunk in chunks {
                    statement
                        .execute(params![
                            recorder.session_id.to_string(),
                            epoch_id.to_string(),
                            chunk.source_id.to_string(),
                            chunk.source_epoch as i64,
                            chunk.sequence as i64,
                            direction_text(chunk.direction),
                            chunk.monotonic_offset_ns,
                            chunk.bytes.as_ref(),
                            chunk.tx_job_id.map(|id| id.to_string()),
                        ])
                        .map_err(|error| {
                            ToolboxError::new(
                                ErrorCode::SessionWrite,
                                "session.append",
                                "chunk_insert_failed",
                            )
                            .cause(error)
                        })?;
                    batch_bytes += chunk.bytes.len() as u64;
                }
            }
            tx.commit().map_err(|error| {
                ToolboxError::new(
                    ErrorCode::SessionWrite,
                    "session.append",
                    "transaction_commit_failed",
                )
                .cause(error)
            })?;
            recorder.bytes_written += batch_bytes;
            let should_checkpoint = recorder.last_checkpoint.elapsed() >= CHECKPOINT_INTERVAL
                || wal_size(&recorder.path) >= CHECKPOINT_WAL_BYTES;
            if should_checkpoint {
                checkpoint(&recorder.conn, "PASSIVE")?;
                recorder.last_checkpoint = Instant::now();
            }
            recorder.bytes_written
        };
        inner.snapshot.bytes_written = bytes_written;
        Ok(())
    }

    pub fn suspend_epoch(
        &self,
        source_id: Uuid,
        source_epoch: u64,
        reason: &str,
    ) -> ToolboxResult<()> {
        let mut inner = self.inner.lock();
        if inner.snapshot.status != SessionStatus::Recording {
            return Ok(());
        }
        let recorder = inner.recorder.as_mut().ok_or_else(|| {
            ToolboxError::new(
                ErrorCode::SessionState,
                "session.suspend",
                "recorder_missing",
            )
        })?;
        if let Some(epoch_id) = recorder.current_epoch_id.take() {
            recorder
                .conn
                .execute(
                    "UPDATE epochs SET ended_utc_ns=?1, end_reason=?2 WHERE id=?3",
                    params![utc_now_ns(), reason, epoch_id.to_string()],
                )
                .map_err(|error| {
                    ToolboxError::new(
                        ErrorCode::SessionWrite,
                        "session.suspend",
                        "epoch_update_failed",
                    )
                    .cause(error)
                })?;
            recorder.conn.execute(
                "INSERT INTO session_events(session_id, epoch_id, event_type, utc_ns, payload_json) VALUES (?1,?2,'StreamGap',?3,?4)",
                params![recorder.session_id.to_string(), epoch_id.to_string(), utc_now_ns(), serde_json::json!({"sourceId": source_id, "sourceEpoch": source_epoch, "reason": reason}).to_string()],
            ).map_err(|error| ToolboxError::new(ErrorCode::SessionWrite, "session.suspend", "gap_event_failed").cause(error))?;
        }
        inner.snapshot.status = SessionStatus::Suspended;
        inner.snapshot.message = Some(reason.into());
        let snapshot = inner.snapshot.clone();
        drop(inner);
        self.state.set_session(snapshot, "session.suspended");
        Ok(())
    }

    pub fn resume_epoch(&self, source: &SourceSnapshot) -> ToolboxResult<()> {
        let mut inner = self.inner.lock();
        if inner.snapshot.status != SessionStatus::Suspended {
            return Ok(());
        }
        let epoch_ordinal = {
            let recorder = inner.recorder.as_mut().ok_or_else(|| {
                ToolboxError::new(
                    ErrorCode::SessionState,
                    "session.resume",
                    "recorder_missing",
                )
            })?;
            begin_epoch(recorder, source)?;
            recorder.epoch_ordinal
        };
        inner.snapshot.status = SessionStatus::Recording;
        inner.snapshot.epoch_ordinal = Some(epoch_ordinal);
        inner.snapshot.message = None;
        let snapshot = inner.snapshot.clone();
        drop(inner);
        self.state.set_session(snapshot, "session.resumed");
        Ok(())
    }

    pub fn fail_recording(&self, error: &ToolboxError) {
        let mut inner = self.inner.lock();
        if self.is_active_locked(&inner) {
            inner.snapshot.status = SessionStatus::Failed;
            inner.snapshot.message = Some(error.code.clone());
            let snapshot = inner.snapshot.clone();
            drop(inner);
            self.state.set_session(snapshot, "session.failed");
            self.state.push_error(error.clone());
        }
    }

    pub fn stop(&self) -> ToolboxResult<SessionSnapshot> {
        let (mut recorder, finalizing) = {
            let mut inner = self.inner.lock();
            if !self.is_active_locked(&inner) && inner.snapshot.status != SessionStatus::Failed {
                return Err(ToolboxError::new(
                    ErrorCode::SessionState,
                    "session.stop",
                    "session_not_active",
                ));
            }
            inner.snapshot.status = SessionStatus::Finalizing;
            inner.snapshot.checkpoint_pending = true;
            let finalizing = inner.snapshot.clone();
            let recorder = inner.recorder.take().ok_or_else(|| {
                ToolboxError::new(ErrorCode::SessionState, "session.stop", "recorder_missing")
            })?;
            (recorder, finalizing)
        };
        self.state.set_session(finalizing, "session.finalizing");
        {
            let tx = recorder.conn.transaction().map_err(|error| {
                ToolboxError::new(
                    ErrorCode::SessionWrite,
                    "session.stop",
                    "final_transaction_begin_failed",
                )
                .cause(error)
            })?;
            if let Some(epoch_id) = recorder.current_epoch_id.take() {
                tx.execute(
                    "UPDATE epochs SET ended_utc_ns=?1, end_reason='SessionStop' WHERE id=?2",
                    params![utc_now_ns(), epoch_id.to_string()],
                )
                .map_err(|error| {
                    ToolboxError::new(
                        ErrorCode::SessionWrite,
                        "session.stop",
                        "epoch_close_failed",
                    )
                    .cause(error)
                })?;
            }
            tx.execute(
                "UPDATE sessions SET status='Closed', ended_utc_ns=?1, bytes_written=?2 WHERE id=?3",
                params![utc_now_ns(), recorder.bytes_written as i64, recorder.session_id.to_string()],
            ).map_err(|error| ToolboxError::new(ErrorCode::SessionWrite, "session.stop", "session_close_failed").cause(error))?;
            tx.commit().map_err(|error| {
                ToolboxError::new(
                    ErrorCode::SessionWrite,
                    "session.stop",
                    "final_transaction_commit_failed",
                )
                .cause(error)
            })?;
        }
        recorder.conn.flush_prepared_statement_cache();
        let checkpoint_result = checkpoint_with_budget(&recorder.conn, CHECKPOINT_STOP_BUDGET);
        let path = recorder.path.display().to_string();
        let session_id = recorder.session_id;
        let bytes_written = recorder.bytes_written;
        drop(recorder.conn);
        let (snapshot, result) = match checkpoint_result {
            Ok(()) => (
                SessionSnapshot {
                    status: SessionStatus::Closed,
                    session_id: Some(session_id),
                    path: Some(path),
                    epoch_ordinal: None,
                    bytes_written,
                    checkpoint_pending: false,
                    message: Some("recording_committed".into()),
                },
                Ok(()),
            ),
            Err(error) => (
                SessionSnapshot {
                    status: SessionStatus::Closed,
                    session_id: Some(session_id),
                    path: Some(path),
                    epoch_ordinal: None,
                    bytes_written,
                    checkpoint_pending: true,
                    message: Some("data_committed_wal_not_merged".into()),
                },
                Err(error),
            ),
        };
        self.inner.lock().snapshot = snapshot.clone();
        self.state.set_session(snapshot.clone(), "session.closed");
        result.map(|_| snapshot)
    }

    pub fn export_csv(&self, session_path: &Path, csv_path: &Path) -> ToolboxResult<u64> {
        if self.is_active()
            && self.snapshot().path.as_deref() == Some(&session_path.display().to_string())
        {
            return Err(ToolboxError::new(
                ErrorCode::ReplayActiveSession,
                "session.export",
                "active_session_export_forbidden",
            ));
        }
        let replay = load_replay_data(session_path)?;
        let mut pipeline = Pipeline::new(replay.project)?;
        let mut file = fs::File::create(csv_path).map_err(|error| {
            ToolboxError::new(
                ErrorCode::SessionWrite,
                "session.export",
                "csv_create_failed",
            )
            .cause(error)
        })?;
        writeln!(
            file,
            "session_time_ns,source_id,source_epoch,frame_sequence,channel_id,value"
        )
        .map_err(|error| {
            ToolboxError::new(
                ErrorCode::SessionWrite,
                "session.export",
                "csv_write_failed",
            )
            .cause(error)
        })?;
        let mut rows = 0u64;
        for replay_chunk in replay.chunks {
            if replay_chunk.epoch_changed {
                pipeline.reset(ResetReason::EpochChanged);
            }
            let chunk = RawChunk {
                source_id: replay_chunk.source_id,
                source_epoch: replay_chunk.source_epoch,
                sequence: replay_chunk.sequence,
                monotonic_offset_ns: replay_chunk.monotonic_offset_ns,
                direction: replay_chunk.direction,
                bytes: Arc::from(replay_chunk.bytes),
                gap_before: replay_chunk.epoch_changed,
                tx_job_id: None,
            };
            for sample in pipeline.process(&chunk).samples {
                writeln!(
                    file,
                    "{},{},{},{},{},{}",
                    replay_chunk.session_offset_ns,
                    replay_chunk.source_id,
                    replay_chunk.source_epoch,
                    sample.frame_sequence,
                    sample.channel_id,
                    sample.value
                )
                .map_err(|error| {
                    ToolboxError::new(
                        ErrorCode::SessionWrite,
                        "session.export",
                        "csv_write_failed",
                    )
                    .cause(error)
                })?;
                rows += 1;
            }
        }
        Ok(rows)
    }

    fn is_active_locked(&self, inner: &SessionInner) -> bool {
        matches!(
            inner.snapshot.status,
            SessionStatus::Recording | SessionStatus::Suspended | SessionStatus::Finalizing
        )
    }
}

fn begin_epoch(recorder: &mut Recorder, source: &SourceSnapshot) -> ToolboxResult<()> {
    recorder.epoch_ordinal += 1;
    let epoch_id = source.epoch_id.ok_or_else(|| {
        ToolboxError::new(
            ErrorCode::EpochIdentity,
            "session.epoch",
            "epoch_identity_missing",
        )
    })?;
    let identity = EpochIdentity {
        epoch_id,
        runtime_instance_id: recorder.runtime_instance_id,
        source_id: source.source_id,
        source_epoch: source.source_epoch,
        session_epoch_ordinal: Some(recorder.epoch_ordinal),
    };
    let anchor = source.clock_anchor.as_ref().ok_or_else(|| {
        ToolboxError::new(
            ErrorCode::SessionState,
            "session.epoch",
            "clock_anchor_missing",
        )
    })?;
    let session_offset_ns = anchor.monotonic_anchor_ns - recorder.session_anchor_monotonic_ns;
    recorder.conn.execute(
        "INSERT INTO epochs(id, session_id, ordinal, runtime_instance_id, source_id, source_epoch, endpoint, utc_anchor_ns, monotonic_anchor_ns, session_offset_ns, started_utc_ns) VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11)",
        params![identity.epoch_id.to_string(), recorder.session_id.to_string(), recorder.epoch_ordinal, identity.runtime_instance_id.to_string(), identity.source_id.to_string(), identity.source_epoch as i64, source.endpoint, anchor.utc_anchor_unix_ns, anchor.monotonic_anchor_ns, session_offset_ns, utc_now_ns()],
    ).map_err(|error| ToolboxError::new(ErrorCode::SessionWrite, "session.epoch", "epoch_insert_failed").cause(error))?;
    recorder.current_epoch_id = Some(epoch_id);
    Ok(())
}

fn configure_database(conn: &Connection) -> ToolboxResult<()> {
    conn.busy_timeout(Duration::from_secs(1))
        .map_err(db_config_error)?;
    conn.pragma_update(None, "journal_mode", "WAL")
        .map_err(db_config_error)?;
    conn.pragma_update(None, "synchronous", "FULL")
        .map_err(db_config_error)?;
    conn.pragma_update(None, "wal_autocheckpoint", 0)
        .map_err(db_config_error)?;
    conn.pragma_update(None, "foreign_keys", "ON")
        .map_err(db_config_error)?;
    Ok(())
}

fn db_config_error(error: rusqlite::Error) -> ToolboxError {
    ToolboxError::new(
        ErrorCode::SessionOpen,
        "session.configure",
        "sqlite_pragma_failed",
    )
    .cause(error)
}

fn create_schema(conn: &Connection) -> ToolboxResult<()> {
    conn.execute_batch(
        "CREATE TABLE IF NOT EXISTS sessions(
            id TEXT PRIMARY KEY, status TEXT NOT NULL, started_utc_ns INTEGER NOT NULL, ended_utc_ns INTEGER,
            utc_anchor_unix_ns INTEGER NOT NULL, monotonic_anchor_ns INTEGER NOT NULL,
            runtime_instance_id TEXT NOT NULL, project_json TEXT NOT NULL, project_schema_version TEXT NOT NULL,
            pipeline_semantic_version TEXT NOT NULL, app_version TEXT NOT NULL, bytes_written INTEGER NOT NULL DEFAULT 0
        );
        CREATE TABLE IF NOT EXISTS epochs(
            id TEXT PRIMARY KEY, session_id TEXT NOT NULL REFERENCES sessions(id), ordinal INTEGER NOT NULL,
            runtime_instance_id TEXT NOT NULL, source_id TEXT NOT NULL, source_epoch INTEGER NOT NULL, endpoint TEXT NOT NULL,
            utc_anchor_ns INTEGER NOT NULL, monotonic_anchor_ns INTEGER NOT NULL, session_offset_ns INTEGER NOT NULL,
            started_utc_ns INTEGER NOT NULL, ended_utc_ns INTEGER, end_reason TEXT
        );
        CREATE TABLE IF NOT EXISTS raw_chunks(
            id INTEGER PRIMARY KEY AUTOINCREMENT, session_id TEXT NOT NULL REFERENCES sessions(id),
            epoch_id TEXT NOT NULL REFERENCES epochs(id), source_id TEXT NOT NULL, source_epoch INTEGER NOT NULL,
            sequence INTEGER NOT NULL, direction TEXT NOT NULL, monotonic_offset_ns INTEGER NOT NULL,
            bytes BLOB NOT NULL, tx_job_id TEXT
        );
        CREATE INDEX IF NOT EXISTS raw_chunks_timeline ON raw_chunks(epoch_id, sequence);
        CREATE TABLE IF NOT EXISTS session_events(
            id INTEGER PRIMARY KEY AUTOINCREMENT, session_id TEXT NOT NULL, epoch_id TEXT,
            event_type TEXT NOT NULL, utc_ns INTEGER NOT NULL, payload_json TEXT NOT NULL
        );"
    ).map_err(|error| ToolboxError::new(ErrorCode::SessionOpen, "session.schema", "schema_create_failed").cause(error))?;
    Ok(())
}

fn checkpoint(conn: &Connection, mode: &str) -> ToolboxResult<(i64, i64, i64)> {
    let sql = format!("PRAGMA wal_checkpoint({mode})");
    conn.query_row(&sql, [], |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)))
        .map_err(|error| {
            ToolboxError::new(
                ErrorCode::SessionCheckpointFailed,
                "session.checkpoint",
                "checkpoint_query_failed",
            )
            .cause(error)
        })
}

fn checkpoint_with_budget(conn: &Connection, budget: Duration) -> ToolboxResult<()> {
    let started = Instant::now();
    loop {
        let (busy, log_frames, checkpointed) = checkpoint(conn, "TRUNCATE")?;
        if busy == 0 && log_frames == checkpointed {
            return Ok(());
        }
        if started.elapsed() >= budget {
            return Err(ToolboxError::new(
                ErrorCode::SessionCheckpointBusy,
                "session.stop",
                "checkpoint_busy",
            )
            .context("busy", busy)
            .context("logFrames", log_frames)
            .context("checkpointedFrames", checkpointed));
        }
        thread::sleep(Duration::from_millis(100));
    }
}

fn wal_size(path: &Path) -> u64 {
    let mut wal = path.as_os_str().to_os_string();
    wal.push("-wal");
    fs::metadata(PathBuf::from(wal))
        .map(|meta| meta.len())
        .unwrap_or(0)
}

pub fn load_replay_data(path: &Path) -> ToolboxResult<ReplayData> {
    let mut conn = Connection::open(path).map_err(|error| {
        ToolboxError::new(
            ErrorCode::ReplayFailed,
            "replay.open",
            "session_open_failed",
        )
        .cause(error)
    })?;
    configure_database(&conn)?;
    recover_interrupted_sessions(&mut conn)?;
    checkpoint_with_budget(&conn, CHECKPOINT_STOP_BUDGET)?;
    let (project_json, pipeline_version): (String, String) = conn.query_row(
        "SELECT project_json, pipeline_semantic_version FROM sessions ORDER BY started_utc_ns LIMIT 1", [], |row| Ok((row.get(0)?, row.get(1)?)),
    ).map_err(|error| ToolboxError::new(ErrorCode::SessionCorrupt, "replay.load", "session_metadata_missing").cause(error))?;
    if pipeline_version != crate::core::model::PIPELINE_SEMANTIC_VERSION {
        return Err(ToolboxError::new(
            ErrorCode::PipelineVersionUnsupported,
            "replay.load",
            "pipeline_version_unsupported",
        )
        .context("version", pipeline_version));
    }
    let project: ToolboxProject = serde_json::from_str(&project_json).map_err(|error| {
        ToolboxError::new(
            ErrorCode::SessionCorrupt,
            "replay.load",
            "project_snapshot_invalid",
        )
        .cause(error)
    })?;
    let mut statement = conn.prepare(
        "SELECT r.source_id, r.source_epoch, r.sequence, r.monotonic_offset_ns, e.session_offset_ns, r.direction, r.bytes, e.ordinal
         FROM raw_chunks r JOIN epochs e ON e.id=r.epoch_id ORDER BY e.ordinal, r.sequence, r.id"
    ).map_err(|error| ToolboxError::new(ErrorCode::ReplayFailed, "replay.load", "replay_query_failed").cause(error))?;
    let mut last_ordinal = None;
    let chunks = statement
        .query_map([], |row| {
            let ordinal: u32 = row.get(7)?;
            let changed = last_ordinal
                .replace(ordinal)
                .is_some_and(|last| last != ordinal);
            let source: String = row.get(0)?;
            let direction: String = row.get(5)?;
            Ok(ReplayChunk {
                source_id: Uuid::parse_str(&source).unwrap_or(Uuid::nil()),
                source_epoch: row.get::<_, i64>(1)? as u64,
                sequence: row.get::<_, i64>(2)? as u64,
                monotonic_offset_ns: row.get(3)?,
                session_offset_ns: row.get::<_, i64>(4)? + row.get::<_, i64>(3)?,
                direction: if direction == "Tx" {
                    Direction::Tx
                } else {
                    Direction::Rx
                },
                bytes: row.get(6)?,
                epoch_changed: changed,
            })
        })
        .map_err(|error| {
            ToolboxError::new(ErrorCode::ReplayFailed, "replay.load", "replay_rows_failed")
                .cause(error)
        })?
        .collect::<Result<Vec<_>, _>>()
        .map_err(|error| {
            ToolboxError::new(ErrorCode::ReplayFailed, "replay.load", "replay_row_invalid")
                .cause(error)
        })?;
    Ok(ReplayData { project, chunks })
}

fn recover_interrupted_sessions(conn: &mut Connection) -> ToolboxResult<()> {
    let tx = conn.transaction().map_err(|error| {
        ToolboxError::new(
            ErrorCode::SessionWrite,
            "session.recover",
            "recovery_transaction_begin_failed",
        )
        .cause(error)
    })?;
    let interrupted = tx
        .execute(
            "UPDATE sessions SET status='Interrupted', ended_utc_ns=COALESCE(ended_utc_ns, ?1) WHERE status IN ('Recording','Suspended','Finalizing')",
            params![utc_now_ns()],
        )
        .map_err(|error| {
            ToolboxError::new(
                ErrorCode::SessionWrite,
                "session.recover",
                "session_interrupt_mark_failed",
            )
            .cause(error)
        })?;
    if interrupted > 0 {
        tx.execute(
            "UPDATE epochs SET ended_utc_ns=COALESCE(ended_utc_ns, ?1), end_reason=COALESCE(end_reason, 'Interrupted') WHERE ended_utc_ns IS NULL",
            params![utc_now_ns()],
        )
        .map_err(|error| {
            ToolboxError::new(
                ErrorCode::SessionWrite,
                "session.recover",
                "epoch_interrupt_mark_failed",
            )
            .cause(error)
        })?;
    }
    tx.commit().map_err(|error| {
        ToolboxError::new(
            ErrorCode::SessionWrite,
            "session.recover",
            "recovery_transaction_commit_failed",
        )
        .cause(error)
    })
}

fn direction_text(direction: Direction) -> &'static str {
    match direction {
        Direction::Rx => "Rx",
        Direction::Tx => "Tx",
    }
}

pub fn utc_now_ns() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos()
        .min(i64::MAX as u128) as i64
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::event::StateStore;
    use crate::core::model::{ClockAnchor, QueueDepths, SourceStats, SourceStatus};
    use tempfile::tempdir;

    fn source(project: &ToolboxProject) -> SourceSnapshot {
        SourceSnapshot {
            source_id: project.source.id,
            name: "test".into(),
            status: SourceStatus::Connected,
            transport: "synthetic".into(),
            endpoint: "seed:1".into(),
            source_epoch: 1,
            epoch_id: Some(Uuid::now_v7()),
            clock_anchor: Some(ClockAnchor {
                utc_anchor_unix_ns: utc_now_ns(),
                monotonic_anchor_ns: 100,
            }),
            queue_depths: QueueDepths {
                recorder_bytes: 0,
                parser_bytes: 0,
                terminal_bytes: 0,
                ipc_bytes: 0,
                tx_bytes: 0,
            },
            stats: SourceStats::default(),
        }
    }

    #[test]
    fn stop_checkpoints_and_closes_session() {
        let project = ToolboxProject::demo();
        let state = Arc::new(StateStore::new(project.clone()));
        let manager = SessionManager::new(state);
        let dir = tempdir().unwrap();
        let path = dir.path().join("test.etdb");
        manager
            .start(path.clone(), &project, &source(&project), 100)
            .unwrap();
        let chunk = RawChunk {
            source_id: project.source.id,
            source_epoch: 1,
            sequence: 1,
            monotonic_offset_ns: 10,
            direction: Direction::Rx,
            bytes: Arc::from(&b"1,2,3\n"[..]),
            gap_before: false,
            tx_job_id: None,
        };
        manager.append_batch(&[chunk]).unwrap();
        let stopped = manager.stop().unwrap();
        assert_eq!(stopped.status, SessionStatus::Closed);
        assert!(!stopped.checkpoint_pending);
        assert_eq!(wal_size(&path), 0);
    }

    #[test]
    fn interrupted_session_is_marked_before_replay() {
        let project = ToolboxProject::demo();
        let dir = tempdir().unwrap();
        let path = dir.path().join("interrupted.etdb");
        {
            let state = Arc::new(StateStore::new(project.clone()));
            let manager = SessionManager::new(state);
            manager
                .start(path.clone(), &project, &source(&project), 100)
                .unwrap();
        }
        load_replay_data(&path).unwrap();
        let conn = Connection::open(path).unwrap();
        let status: String = conn
            .query_row("SELECT status FROM sessions", [], |row| row.get(0))
            .unwrap();
        assert_eq!(status, "Interrupted");
    }
}
