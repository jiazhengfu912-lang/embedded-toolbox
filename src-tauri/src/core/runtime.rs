use crate::core::envelope::{
    EnvelopeMeta, PayloadType, encode, encode_json_payload, encode_raw_payload,
};
use crate::core::error::{ErrorCode, ToolboxError, ToolboxResult};
use crate::core::event::{DataHub, StateStore};
use crate::core::model::*;
use crate::core::pipeline::Pipeline;
use crate::core::queue::{ByteQueue, ByteSize, OverflowPolicy};
use crate::core::session::{SessionManager, load_replay_data, utc_now_ns};
use crate::core::transport::{SerialPair, list_ports, open_serial};
use crate::core::tx::{SerialTxTarget, SyntheticTxTarget, TxScheduler, TxTarget};
use crossbeam_channel::{Sender, bounded};
use parking_lot::{Mutex, RwLock};
use std::collections::{HashMap, HashSet};
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicI64, AtomicU64, Ordering};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};
use tauri::ipc::{Channel, InvokeResponseBody};
use uuid::Uuid;

const RECORDER_BYTES: usize = 16 * 1024 * 1024;
const PARSER_BYTES: usize = 4 * 1024 * 1024;
const TERMINAL_BYTES: usize = 2 * 1024 * 1024;
const IPC_BYTES: usize = 4 * 1024 * 1024;
const AUTO_RECONNECT_POLL_INTERVAL: Duration = Duration::from_millis(250);
const AUTO_RECONNECT_RETRY_INTERVAL: Duration = Duration::from_secs(1);

impl ByteSize for RawChunk {
    fn byte_size(&self) -> usize {
        self.bytes.len().max(1)
    }
}

enum RecorderItem {
    Chunk(RawChunk),
    Barrier(Sender<()>),
}
impl ByteSize for RecorderItem {
    fn byte_size(&self) -> usize {
        match self {
            Self::Chunk(chunk) => chunk.byte_size(),
            Self::Barrier(_) => 1,
        }
    }
}

struct StreamMessage {
    payload_type: PayloadType,
    payload_version: u16,
    monotonic_offset_ns: i64,
    payload: Vec<u8>,
}
impl ByteSize for StreamMessage {
    fn byte_size(&self) -> usize {
        self.payload.len().max(1)
    }
}

struct Fanout {
    source_id: Uuid,
    source_epoch: u64,
    epoch_start: Instant,
    raw_sequence: AtomicU64,
    recorder: Arc<ByteQueue<RecorderItem>>,
    parser: Arc<ByteQueue<RawChunk>>,
    terminal: Arc<ByteQueue<RawChunk>>,
    parser_gap_generation: AtomicU64,
    recording_accepting: AtomicBool,
    stats: Arc<Mutex<SourceStats>>,
    state: Arc<StateStore>,
    session: Arc<SessionManager>,
}

impl Fanout {
    fn push(&self, direction: Direction, bytes: Vec<u8>, tx_job_id: Option<Uuid>) {
        if bytes.is_empty() {
            return;
        }
        let sequence = self.raw_sequence.fetch_add(1, Ordering::Relaxed) + 1;
        let chunk = RawChunk {
            source_id: self.source_id,
            source_epoch: self.source_epoch,
            sequence,
            monotonic_offset_ns: self.epoch_start.elapsed().as_nanos().min(i64::MAX as u128) as i64,
            direction,
            bytes: Arc::from(bytes),
            gap_before: false,
            tx_job_id,
        };
        {
            let mut stats = self.stats.lock();
            match direction {
                Direction::Rx => stats.rx_bytes += chunk.bytes.len() as u64,
                Direction::Tx => stats.tx_bytes += chunk.bytes.len() as u64,
            }
        }
        if self.recording_accepting.load(Ordering::Relaxed) {
            if self
                .recorder
                .push(RecorderItem::Chunk(chunk.clone()), OverflowPolicy::Reject)
                .is_err()
            {
                self.recording_accepting.store(false, Ordering::Relaxed);
                let error = ToolboxError::new(
                    ErrorCode::RecordingBackpressure,
                    "recorder.queue",
                    "recording_queue_full",
                )
                .source(self.source_id);
                self.session.fail_recording(&error);
            }
        }
        if let Ok(outcome) = self
            .parser
            .push(chunk.clone(), OverflowPolicy::ClearToLowWater)
        {
            if outcome.dropped_items > 0 {
                self.parser_gap_generation.fetch_add(1, Ordering::Release);
                let mut stats = self.stats.lock();
                stats.parser_gaps += 1;
                drop(stats);
                self.state.emit(
                    "diagnostic.parserGap",
                    serde_json::json!({
                        "sourceId": self.source_id, "droppedBytes": outcome.dropped_bytes,
                    }),
                );
            }
        }
        if let Ok(outcome) = self.terminal.push(chunk, OverflowPolicy::DropOldest) {
            if outcome.dropped_bytes > 0 {
                self.stats.lock().ui_dropped_bytes += outcome.dropped_bytes as u64;
            }
        }
    }

    fn flush_recorder(&self, timeout: Duration) -> bool {
        let (tx, rx) = bounded(1);
        if self
            .recorder
            .push(RecorderItem::Barrier(tx), OverflowPolicy::Reject)
            .is_err()
        {
            return false;
        }
        rx.recv_timeout(timeout).is_ok()
    }
}

pub struct SourceRuntime {
    snapshot_base: SourceSnapshot,
    fanout: Arc<Fanout>,
    ipc: Arc<ByteQueue<StreamMessage>>,
    stop: Arc<AtomicBool>,
    tx: TxScheduler,
    workers: Vec<JoinHandle<()>>,
}

impl SourceRuntime {
    fn start(
        project: ToolboxProject,
        source_epoch: u64,
        transport: String,
        endpoint: String,
        clock_anchor: ClockAnchor,
        reader: ReaderKind,
        tx_target: Box<dyn TxTarget>,
        state: Arc<StateStore>,
        data: Arc<DataHub>,
        session: Arc<SessionManager>,
    ) -> ToolboxResult<Self> {
        let source_id = project.source.id;
        let stop = Arc::new(AtomicBool::new(false));
        let recorder = Arc::new(ByteQueue::new(RECORDER_BYTES, 4096));
        let parser = Arc::new(ByteQueue::new(PARSER_BYTES, 2048));
        let terminal = Arc::new(ByteQueue::new(TERMINAL_BYTES, 1024));
        let ipc = Arc::new(ByteQueue::new(IPC_BYTES, 256));
        let stats = Arc::new(Mutex::new(SourceStats::default()));
        let epoch_start = Instant::now();
        let fanout = Arc::new(Fanout {
            source_id,
            source_epoch,
            epoch_start,
            raw_sequence: AtomicU64::new(0),
            recorder: Arc::clone(&recorder),
            parser: Arc::clone(&parser),
            terminal: Arc::clone(&terminal),
            parser_gap_generation: AtomicU64::new(0),
            recording_accepting: AtomicBool::new(false),
            stats: Arc::clone(&stats),
            state: Arc::clone(&state),
            session: Arc::clone(&session),
        });
        let snapshot_base = SourceSnapshot {
            source_id,
            name: project.source.name.clone(),
            status: SourceStatus::Connected,
            transport: transport.clone(),
            endpoint: endpoint.clone(),
            source_epoch,
            epoch_id: Some(Uuid::now_v7()),
            clock_anchor: Some(clock_anchor),
            queue_depths: QueueDepths {
                recorder_bytes: 0,
                parser_bytes: 0,
                terminal_bytes: 0,
                ipc_bytes: 0,
                tx_bytes: 0,
            },
            stats: SourceStats::default(),
        };
        let mut workers = Vec::new();

        // Recorder worker batches by 100 ms or 64 KiB and acknowledges barriers after prior chunks commit.
        {
            let queue = Arc::clone(&recorder);
            let worker_session = Arc::clone(&session);
            let worker_state = Arc::clone(&state);
            let worker_stop = Arc::clone(&stop);
            workers.push(
                thread::Builder::new()
                    .name("etb-recorder".into())
                    .spawn(move || {
                        while !worker_stop.load(Ordering::Relaxed) || queue.depth_items() > 0 {
                            let Some(first) = queue.pop_timeout(Duration::from_millis(100)) else {
                                continue;
                            };
                            let mut batch = Vec::new();
                            let mut barriers = Vec::new();
                            match first {
                                RecorderItem::Chunk(chunk) => batch.push(chunk),
                                RecorderItem::Barrier(sender) => barriers.push(sender),
                            }
                            let started = Instant::now();
                            let mut bytes = batch.iter().map(RawChunk::byte_size).sum::<usize>();
                            while bytes < 64 * 1024
                                && started.elapsed() < Duration::from_millis(100)
                            {
                                let Some(item) = queue.try_pop() else { break };
                                match item {
                                    RecorderItem::Chunk(chunk) => {
                                        bytes += chunk.byte_size();
                                        batch.push(chunk);
                                    }
                                    RecorderItem::Barrier(sender) => barriers.push(sender),
                                }
                            }
                            if let Err(error) = worker_session.append_batch(&batch) {
                                worker_session.fail_recording(&error);
                                worker_state.push_error(error);
                            }
                            for barrier in barriers {
                                let _ = barrier.send(());
                            }
                        }
                    })
                    .map_err(|error| {
                        ToolboxError::new(
                            ErrorCode::Internal,
                            "runtime.start",
                            "recorder_thread_failed",
                        )
                        .cause(error)
                    })?,
            );
        }

        // Terminal worker preserves raw bytes and direction in a compact binary payload.
        {
            let queue = Arc::clone(&terminal);
            let worker_ipc = Arc::clone(&ipc);
            let worker_stop = Arc::clone(&stop);
            let worker_stats = Arc::clone(&stats);
            workers.push(
                thread::Builder::new()
                    .name("etb-terminal".into())
                    .spawn(move || {
                        while !worker_stop.load(Ordering::Relaxed) || queue.depth_items() > 0 {
                            let Some(chunk) = queue.pop_timeout(Duration::from_millis(20)) else {
                                continue;
                            };
                            let message = StreamMessage {
                                payload_type: PayloadType::RawBatch,
                                payload_version: 1,
                                monotonic_offset_ns: chunk.monotonic_offset_ns,
                                payload: encode_raw_payload(
                                    chunk.direction,
                                    chunk.sequence,
                                    &chunk.bytes,
                                ),
                            };
                            if let Ok(outcome) =
                                worker_ipc.push(message, OverflowPolicy::DropOldest)
                            {
                                if outcome.dropped_items > 0 {
                                    worker_stats.lock().ui_dropped_batches +=
                                        outcome.dropped_items as u64;
                                }
                            }
                        }
                    })
                    .map_err(|error| {
                        ToolboxError::new(
                            ErrorCode::Internal,
                            "runtime.start",
                            "terminal_thread_failed",
                        )
                        .cause(error)
                    })?,
            );
        }

        // Parser worker owns stateful framing/decoding/transforms for this Source epoch.
        {
            let queue = Arc::clone(&parser);
            let worker_ipc = Arc::clone(&ipc);
            let worker_stop = Arc::clone(&stop);
            let worker_stats = Arc::clone(&stats);
            let worker_state = Arc::clone(&state);
            let worker_fanout = Arc::clone(&fanout);
            workers.push(thread::Builder::new().name("etb-parser".into()).spawn(move || {
                let mut pipeline = match Pipeline::new(project) {
                    Ok(pipeline) => pipeline,
                    Err(error) => { worker_state.push_error(error); return; }
                };
                pipeline.reset(ResetReason::Connect);
                let mut seen_gap = worker_fanout.parser_gap_generation.load(Ordering::Acquire);
                while !worker_stop.load(Ordering::Relaxed) || queue.depth_items() > 0 {
                    let Some(mut chunk) = queue.pop_timeout(Duration::from_millis(20)) else { continue };
                    let current_gap = worker_fanout.parser_gap_generation.load(Ordering::Acquire);
                    if current_gap != seen_gap { chunk.gap_before = true; seen_gap = current_gap; }
                    let output = pipeline.process(&chunk);
                    {
                        let mut stats = worker_stats.lock();
                        stats.parsed_frames += output.frames.len() as u64;
                        stats.checksum_failures += output.checksum_failures;
                    }
                    if !output.samples.is_empty() {
                        worker_state.update_latest_values(output.samples.iter().map(|sample| (sample.channel_id, sample.value)));
                        let _ = worker_ipc.push(StreamMessage {
                            payload_type: PayloadType::SampleBatch, payload_version: 1, monotonic_offset_ns: chunk.monotonic_offset_ns,
                            payload: encode_json_payload(&output.samples),
                        }, OverflowPolicy::DropOldest);
                    }
                    if !output.frames.is_empty() {
                        let _ = worker_ipc.push(StreamMessage {
                            payload_type: PayloadType::PacketBatch, payload_version: 1, monotonic_offset_ns: chunk.monotonic_offset_ns,
                            payload: encode_json_payload(&output.frames),
                        }, OverflowPolicy::DropOldest);
                    }
                    if output.oversize_frames + output.resyncs + output.checksum_failures + output.parse_failures > 0 {
                        let diagnostic = serde_json::json!({
                            "oversizeFrames": output.oversize_frames, "resyncs": output.resyncs,
                            "checksumFailures": output.checksum_failures, "parseFailures": output.parse_failures,
                        });
                        let _ = worker_ipc.push(StreamMessage {
                            payload_type: PayloadType::DiagnosticBatch, payload_version: 1, monotonic_offset_ns: chunk.monotonic_offset_ns,
                            payload: encode_json_payload(&diagnostic),
                        }, OverflowPolicy::DropOldest);
                    }
                }
                pipeline.reset(ResetReason::Disconnect);
            }).map_err(|error| ToolboxError::new(ErrorCode::Internal, "runtime.start", "parser_thread_failed").cause(error))?);
        }

        // A single IPC worker establishes envelope ordering for all payload types.
        {
            let queue = Arc::clone(&ipc);
            let worker_stop = Arc::clone(&stop);
            workers.push(
                thread::Builder::new()
                    .name("etb-ipc".into())
                    .spawn(move || {
                        let mut stream_sequence = 0u64;
                        while !worker_stop.load(Ordering::Relaxed) || queue.depth_items() > 0 {
                            let Some(message) = queue.pop_timeout(Duration::from_millis(20)) else {
                                continue;
                            };
                            stream_sequence = stream_sequence.saturating_add(1);
                            data.send(encode(
                                EnvelopeMeta {
                                    payload_type: message.payload_type,
                                    payload_version: message.payload_version,
                                    source_id,
                                    source_epoch,
                                    sequence: stream_sequence,
                                    monotonic_offset_ns: message.monotonic_offset_ns,
                                },
                                &message.payload,
                            ));
                        }
                    })
                    .map_err(|error| {
                        ToolboxError::new(ErrorCode::Internal, "runtime.start", "ipc_thread_failed")
                            .cause(error)
                    })?,
            );
        }

        let tx_fanout = Arc::clone(&fanout);
        let tx = TxScheduler::spawn(
            source_epoch,
            tx_target,
            Arc::clone(&state),
            Arc::clone(&stop),
            Arc::new(move |bytes, job_id| {
                tx_fanout.push(Direction::Tx, bytes, Some(job_id));
            }),
        );

        // Transport reader starts last so every consumer is ready before the first byte arrives.
        {
            let worker_stop = Arc::clone(&stop);
            let worker_fanout = Arc::clone(&fanout);
            let worker_state = Arc::clone(&state);
            let worker_session = Arc::clone(&session);
            let fault_snapshot = snapshot_base.clone();
            workers.push(
                thread::Builder::new()
                    .name("etb-reader".into())
                    .spawn(move || {
                        let result = match reader {
                            ReaderKind::Serial(mut port) => {
                                serial_read_loop(&mut *port, &worker_stop, &worker_fanout)
                            }
                            ReaderKind::Synthetic(config) => {
                                synthetic_read_loop(&config, &worker_stop, &worker_fanout)
                            }
                        };
                        if let Err(error) = result {
                            worker_stop.store(true, Ordering::Relaxed);
                            let _ = worker_fanout.flush_recorder(Duration::from_secs(2));
                            let _ = worker_session.suspend_epoch(
                                source_id,
                                source_epoch,
                                "TransportFault",
                            );
                            worker_state.push_error(error.clone());
                            let mut snapshot = fault_snapshot;
                            snapshot.status = SourceStatus::Faulted;
                            worker_state.set_source(snapshot, "source.faulted");
                        }
                    })
                    .map_err(|error| {
                        ToolboxError::new(
                            ErrorCode::Internal,
                            "runtime.start",
                            "reader_thread_failed",
                        )
                        .cause(error)
                    })?,
            );
        }

        Ok(Self {
            snapshot_base,
            fanout,
            ipc,
            stop,
            tx,
            workers,
        })
    }

    fn snapshot(&self) -> SourceSnapshot {
        let mut snapshot = self.snapshot_base.clone();
        snapshot.status = if self.stop.load(Ordering::Relaxed) {
            SourceStatus::Faulted
        } else {
            SourceStatus::Connected
        };
        snapshot.queue_depths = QueueDepths {
            recorder_bytes: self.fanout.recorder.depth_bytes(),
            parser_bytes: self.fanout.parser.depth_bytes(),
            terminal_bytes: self.fanout.terminal.depth_bytes(),
            ipc_bytes: self.ipc.depth_bytes(),
            tx_bytes: self.tx.depth_bytes(),
        };
        snapshot.stats = self.fanout.stats.lock().clone();
        snapshot
    }

    fn set_recording(&self, enabled: bool) {
        self.fanout
            .recording_accepting
            .store(enabled, Ordering::Relaxed);
    }
    fn flush_recorder(&self) -> bool {
        self.fanout.flush_recorder(Duration::from_secs(5))
    }

    fn is_stopped(&self) -> bool {
        self.stop.load(Ordering::Relaxed)
    }

    fn stop(mut self) {
        self.stop.store(true, Ordering::Relaxed);
        self.set_recording(false);
        self.tx.stop();
        self.fanout.recorder.close();
        self.fanout.parser.close();
        self.fanout.terminal.close();
        self.ipc.close();
        for worker in self.workers.drain(..) {
            let _ = worker.join();
        }
    }
}

enum ReaderKind {
    Serial(Box<dyn serialport::SerialPort>),
    Synthetic(SyntheticConfig),
}

pub struct AppCore {
    state: Arc<StateStore>,
    data: Arc<DataHub>,
    session: Arc<SessionManager>,
    sources: Mutex<HashMap<Uuid, SourceRuntime>>,
    epoch_counters: Mutex<HashMap<Uuid, u64>>,
    source_lifecycle_guard: Mutex<()>,
    auto_reconnect: Mutex<Option<SerialReconnectPlan>>,
    runtime_start: Instant,
    replay_stop: Arc<AtomicBool>,
    replay_running: Arc<AtomicBool>,
    replay_seek_ns: Arc<AtomicI64>,
    project_write_guard: RwLock<()>,
}

#[derive(Clone)]
struct SerialReconnectPlan {
    source_id: Uuid,
    config: SerialConfig,
    device: Option<SerialPortDescriptor>,
    next_attempt: Instant,
}

impl AppCore {
    pub fn new() -> Arc<Self> {
        let state = Arc::new(StateStore::new(ToolboxProject::demo()));
        let session = Arc::new(SessionManager::new(Arc::clone(&state)));
        let core = Arc::new(Self {
            state,
            data: Arc::new(DataHub::default()),
            session,
            sources: Mutex::new(HashMap::new()),
            epoch_counters: Mutex::new(HashMap::new()),
            source_lifecycle_guard: Mutex::new(()),
            auto_reconnect: Mutex::new(None),
            runtime_start: Instant::now(),
            replay_stop: Arc::new(AtomicBool::new(false)),
            replay_running: Arc::new(AtomicBool::new(false)),
            replay_seek_ns: Arc::new(AtomicI64::new(-1)),
            project_write_guard: RwLock::new(()),
        });
        core.start_auto_reconnect_worker();
        core
    }

    pub fn get_snapshot(&self) -> AppSnapshot {
        for runtime in self.sources.lock().values() {
            self.state.update_source_silent(runtime.snapshot());
        }
        self.state.snapshot()
    }

    pub fn subscribe_events(
        &self,
        resume: Option<EventResume>,
        channel: Channel<AppEvent>,
    ) -> EventSubscription {
        self.state.subscribe(resume, channel)
    }

    pub fn subscribe_stream(&self, channel: Channel<InvokeResponseBody>) -> usize {
        self.data.subscribe(channel)
    }
    pub fn list_ports(&self) -> ToolboxResult<Vec<SerialPortDescriptor>> {
        list_ports()
    }

    pub fn connect_serial(&self, config: SerialConfig) -> ToolboxResult<SourceSnapshot> {
        let _lifecycle = self.source_lifecycle_guard.lock();
        self.connect_serial_locked(config)
    }

    fn connect_serial_locked(&self, config: SerialConfig) -> ToolboxResult<SourceSnapshot> {
        self.enforce_product_source_limit()?;
        let _guard = self.project_write_guard.read();
        let mut project = self.state.project();
        if self.session.snapshot().status == SessionStatus::Suspended
            && !serial_semantics_equal(&project.source.serial, &config)
        {
            return Err(ToolboxError::new(
                ErrorCode::SessionState,
                "device.connect",
                "pipeline_frozen_during_session",
            ));
        }
        let port_changed = project.source.serial.port_name != config.port_name;
        project.source.serial = config.clone();
        if !self.session.is_active() || port_changed {
            self.state.set_project(project.clone());
        }
        let SerialPair { reader, writer } = open_serial(&config)?;
        let snapshot = self.connect_runtime(
            project,
            "serial".into(),
            config.port_name.clone(),
            ReaderKind::Serial(reader),
            Box::new(SerialTxTarget(writer)),
        )?;
        self.remember_serial_reconnect(snapshot.source_id, config);
        Ok(snapshot)
    }

    pub fn connect_synthetic(&self, config: SyntheticConfig) -> ToolboxResult<SourceSnapshot> {
        let _lifecycle = self.source_lifecycle_guard.lock();
        self.enforce_product_source_limit()?;
        self.auto_reconnect.lock().take();
        let project = self.state.project();
        self.connect_runtime(
            project,
            "synthetic".into(),
            format!("seed:{} @ {} Hz", config.seed, config.rate_hz),
            ReaderKind::Synthetic(config.clone()),
            Box::new(SyntheticTxTarget::new(config.faults)),
        )
    }

    fn connect_runtime(
        &self,
        project: ToolboxProject,
        transport: String,
        endpoint: String,
        reader: ReaderKind,
        tx_target: Box<dyn TxTarget>,
    ) -> ToolboxResult<SourceSnapshot> {
        let source_id = project.source.id;
        let source_epoch = {
            let mut counters = self.epoch_counters.lock();
            let counter = counters.entry(source_id).or_insert(0);
            *counter += 1;
            *counter
        };
        let clock_anchor = ClockAnchor {
            utc_anchor_unix_ns: utc_now_ns(),
            monotonic_anchor_ns: self
                .runtime_start
                .elapsed()
                .as_nanos()
                .min(i64::MAX as u128) as i64,
        };
        let connecting = SourceSnapshot {
            source_id,
            name: project.source.name.clone(),
            status: SourceStatus::Connecting,
            transport: transport.clone(),
            endpoint: endpoint.clone(),
            source_epoch,
            epoch_id: None,
            clock_anchor: Some(clock_anchor.clone()),
            queue_depths: QueueDepths {
                recorder_bytes: 0,
                parser_bytes: 0,
                terminal_bytes: 0,
                ipc_bytes: 0,
                tx_bytes: 0,
            },
            stats: SourceStats::default(),
        };
        self.state.set_source(connecting, "source.connecting");
        let runtime = SourceRuntime::start(
            project,
            source_epoch,
            transport,
            endpoint,
            clock_anchor,
            reader,
            tx_target,
            Arc::clone(&self.state),
            Arc::clone(&self.data),
            Arc::clone(&self.session),
        )?;
        let snapshot = runtime.snapshot();
        if self.session.snapshot().status == SessionStatus::Suspended {
            self.session.resume_epoch(&snapshot)?;
            runtime.set_recording(true);
        }
        self.state.set_source(snapshot.clone(), "source.connected");
        self.sources.lock().insert(source_id, runtime);
        Ok(snapshot)
    }

    pub fn disconnect(&self, source_id: Uuid) -> ToolboxResult<SourceSnapshot> {
        let _lifecycle = self.source_lifecycle_guard.lock();
        self.auto_reconnect.lock().take();
        let runtime = self.sources.lock().remove(&source_id).ok_or_else(|| {
            ToolboxError::new(
                ErrorCode::DeviceDisconnected,
                "device.disconnect",
                "source_not_connected",
            )
            .source(source_id)
        })?;
        let source_epoch = runtime.snapshot_base.source_epoch;
        runtime.set_recording(false);
        if !runtime.is_stopped() {
            let _ = runtime.flush_recorder();
        }
        runtime.stop();
        self.session
            .suspend_epoch(source_id, source_epoch, "UserDisconnect")?;
        let mut snapshot = self
            .state
            .snapshot()
            .sources
            .into_iter()
            .find(|source| source.source_id == source_id)
            .unwrap_or_else(|| disconnected_snapshot(source_id));
        snapshot.status = SourceStatus::Disconnected;
        snapshot.queue_depths = QueueDepths {
            recorder_bytes: 0,
            parser_bytes: 0,
            terminal_bytes: 0,
            ipc_bytes: 0,
            tx_bytes: 0,
        };
        self.state
            .set_source(snapshot.clone(), "source.disconnected");
        Ok(snapshot)
    }

    pub fn send(&self, request: TxRequest) -> ToolboxResult<TxAccepted> {
        let sources = self.sources.lock();
        let runtime = sources.get(&request.source_id).ok_or_else(|| {
            ToolboxError::new(
                ErrorCode::DeviceDisconnected,
                "tx.enqueue",
                "source_not_connected",
            )
            .source(request.source_id)
        })?;
        let origin = request.origin.clone();
        let accepted = runtime.tx.enqueue(request)?;
        self.state.emit("tx.queued", serde_json::json!({"jobId": accepted.job_id, "sourceId": runtime.snapshot_base.source_id, "sourceEpoch": runtime.snapshot_base.source_epoch, "origin": origin}));
        Ok(accepted)
    }

    pub fn cancel_tx(&self, source_id: Uuid, job_id: Uuid) -> ToolboxResult<()> {
        let sources = self.sources.lock();
        let runtime = sources.get(&source_id).ok_or_else(|| {
            ToolboxError::new(
                ErrorCode::DeviceDisconnected,
                "tx.cancel",
                "source_not_connected",
            )
            .source(source_id)
        })?;
        runtime.tx.cancel(job_id);
        Ok(())
    }

    pub fn start_session(&self, path: PathBuf) -> ToolboxResult<SessionSnapshot> {
        let sources = self.sources.lock();
        let runtime = sources.values().next().ok_or_else(|| {
            ToolboxError::new(
                ErrorCode::SessionState,
                "session.start",
                "source_not_connected",
            )
        })?;
        let snapshot = runtime.snapshot();
        let result = self.session.start(
            path,
            &self.state.project(),
            &snapshot,
            self.runtime_start
                .elapsed()
                .as_nanos()
                .min(i64::MAX as u128) as i64,
        )?;
        runtime.set_recording(true);
        Ok(result)
    }

    pub fn stop_session(&self) -> ToolboxResult<SessionSnapshot> {
        for runtime in self.sources.lock().values() {
            runtime.set_recording(false);
            if !runtime.flush_recorder() {
                return Err(ToolboxError::new(
                    ErrorCode::SessionWrite,
                    "session.stop",
                    "recorder_drain_timeout",
                ));
            }
        }
        self.session.stop()
    }

    pub fn export_csv(&self, session_path: &Path, csv_path: &Path) -> ToolboxResult<u64> {
        self.session.export_csv(session_path, csv_path)
    }

    pub fn start_replay(&self, path: PathBuf, speed: f64) -> ToolboxResult<()> {
        if self.session.is_active()
            && self.session.snapshot().path.as_deref() == Some(&path.display().to_string())
        {
            return Err(ToolboxError::new(
                ErrorCode::ReplayActiveSession,
                "replay.start",
                "active_session_replay_forbidden",
            ));
        }
        let replay = load_replay_data(&path)?;
        if self
            .replay_running
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_err()
        {
            return Err(ToolboxError::new(
                ErrorCode::ReplayFailed,
                "replay.start",
                "replay_already_running",
            ));
        }
        self.replay_stop.store(false, Ordering::Relaxed);
        self.replay_seek_ns.store(-1, Ordering::Relaxed);
        let stop = Arc::clone(&self.replay_stop);
        let running = Arc::clone(&self.replay_running);
        let seek_ns = Arc::clone(&self.replay_seek_ns);
        let data = Arc::clone(&self.data);
        let state = Arc::clone(&self.state);
        let speed = speed.clamp(0.1, 10.0);
        let spawn_result = thread::Builder::new()
            .name("etb-replay".into())
            .spawn(move || {
                state.emit(
                    "replay.started",
                    serde_json::json!({"path": path.display().to_string(), "speed": speed}),
                );
                let mut pipeline = match Pipeline::new(replay.project) {
                    Ok(value) => value,
                    Err(error) => {
                        state.push_error(error);
                        running.store(false, Ordering::Release);
                        return;
                    }
                };
                pipeline.reset(ResetReason::ReplayStart);
                let mut envelope_sequence = 0u64;
                let mut previous_session_ns = 0i64;
                let mut previous_epoch: Option<(Uuid, u64)> = None;
                let mut index = 0usize;
                let mut warming = false;
                while index < replay.chunks.len() {
                    if stop.load(Ordering::Relaxed) {
                        break;
                    }
                    let requested_seek = seek_ns.swap(-1, Ordering::AcqRel);
                    if requested_seek >= 0 {
                        index = replay
                            .chunks
                            .partition_point(|chunk| chunk.session_offset_ns < requested_seek);
                        if index >= replay.chunks.len() {
                            break;
                        }
                        pipeline.reset(ResetReason::ReplaySeek);
                        previous_session_ns = replay.chunks[index].session_offset_ns;
                        previous_epoch = None;
                        warming = true;
                        state.emit(
                            "replay.seeked",
                            serde_json::json!({"sessionOffsetNs": requested_seek, "warming": true}),
                        );
                    }
                    let replay_chunk = replay.chunks[index].clone();
                    let epoch = (replay_chunk.source_id, replay_chunk.source_epoch);
                    if previous_epoch != Some(epoch) {
                        if let Some((source_id, source_epoch)) = previous_epoch {
                            state.emit("replay.epochEnd", serde_json::json!({"sourceId": source_id, "sourceEpoch": source_epoch}));
                            state.emit("replay.streamGap", serde_json::json!({"sourceId": source_id, "sourceEpoch": source_epoch, "reason": "EpochChanged"}));
                            pipeline.reset(ResetReason::EpochChanged);
                        }
                        state.emit("replay.epochStart", serde_json::json!({"sourceId": replay_chunk.source_id, "sourceEpoch": replay_chunk.source_epoch}));
                        previous_epoch = Some(epoch);
                    }
                    let delta = replay_chunk
                        .session_offset_ns
                        .saturating_sub(previous_session_ns)
                        .max(0);
                    if delta > 0 {
                        let scaled = ((delta as f64) / speed).min(u64::MAX as f64) as u64;
                        if replay_wait_interrupted(
                            &stop,
                            &seek_ns,
                            Duration::from_nanos(scaled),
                        ) {
                            if stop.load(Ordering::Relaxed) {
                                break;
                            }
                            continue;
                        }
                    }
                    previous_session_ns = replay_chunk.session_offset_ns;
                    envelope_sequence += 1;
                    data.send(encode(
                        EnvelopeMeta {
                            payload_type: PayloadType::RawBatch,
                            payload_version: 1,
                            source_id: replay_chunk.source_id,
                            source_epoch: replay_chunk.source_epoch,
                            sequence: envelope_sequence,
                            monotonic_offset_ns: replay_chunk.monotonic_offset_ns,
                        },
                        &encode_raw_payload(
                            replay_chunk.direction,
                            replay_chunk.sequence,
                            &replay_chunk.bytes,
                        ),
                    ));
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
                    let output = pipeline.process(&chunk);
                    if !output.frames.is_empty() {
                        envelope_sequence += 1;
                        data.send(encode(
                            EnvelopeMeta {
                                payload_type: PayloadType::PacketBatch,
                                payload_version: 1,
                                source_id: chunk.source_id,
                                source_epoch: chunk.source_epoch,
                                sequence: envelope_sequence,
                                monotonic_offset_ns: chunk.monotonic_offset_ns,
                            },
                            &encode_json_payload(&output.frames),
                        ));
                    }
                    if !output.samples.is_empty() {
                        envelope_sequence += 1;
                        data.send(encode(
                            EnvelopeMeta {
                                payload_type: PayloadType::SampleBatch,
                                payload_version: 1,
                                source_id: chunk.source_id,
                                source_epoch: chunk.source_epoch,
                                sequence: envelope_sequence,
                                monotonic_offset_ns: chunk.monotonic_offset_ns,
                            },
                            &encode_json_payload(&output.samples),
                        ));
                        if warming {
                            warming = false;
                            state.emit(
                                "replay.warmed",
                                serde_json::json!({"sessionOffsetNs": replay_chunk.session_offset_ns, "warming": false}),
                            );
                        }
                    }
                    index += 1;
                }
                if let Some((source_id, source_epoch)) = previous_epoch {
                    state.emit("replay.epochEnd", serde_json::json!({"sourceId": source_id, "sourceEpoch": source_epoch}));
                }
                state.emit(
                    "replay.completed",
                    serde_json::json!({"stopped": stop.load(Ordering::Relaxed)}),
                );
                running.store(false, Ordering::Release);
            })
            .map_err(|error| {
                ToolboxError::new(
                    ErrorCode::ReplayFailed,
                    "replay.start",
                    "replay_thread_failed",
                )
                .cause(error)
            });
        if let Err(error) = spawn_result {
            self.replay_running.store(false, Ordering::Release);
            return Err(error);
        }
        Ok(())
    }

    pub fn stop_replay(&self) {
        self.replay_stop.store(true, Ordering::Relaxed);
    }

    pub fn seek_replay(&self, session_offset_ns: i64) -> ToolboxResult<()> {
        if session_offset_ns < 0 {
            return Err(ToolboxError::new(
                ErrorCode::ReplayFailed,
                "replay.seek",
                "seek_offset_invalid",
            ));
        }
        if !self.replay_running.load(Ordering::Acquire) {
            return Err(ToolboxError::new(
                ErrorCode::ReplayFailed,
                "replay.seek",
                "replay_not_running",
            ));
        }
        self.replay_seek_ns
            .store(session_offset_ns, Ordering::Release);
        self.state.emit(
            "replay.seekRequested",
            serde_json::json!({"sessionOffsetNs": session_offset_ns}),
        );
        Ok(())
    }

    pub fn set_project(&self, project: ToolboxProject) -> ToolboxResult<()> {
        if self.session.is_active() {
            return Err(ToolboxError::new(
                ErrorCode::SessionState,
                "project.set",
                "project_frozen_during_session",
            ));
        }
        validate_project(&project)?;
        let _guard = self.project_write_guard.write();
        self.state.set_project(project);
        Ok(())
    }

    pub fn load_project(&self, path: &Path) -> ToolboxResult<ToolboxProject> {
        let bytes = fs::read(path).map_err(|error| {
            ToolboxError::new(
                ErrorCode::ProjectSchemaInvalid,
                "project.load",
                "project_read_failed",
            )
            .cause(error)
        })?;
        let project: ToolboxProject = serde_json::from_slice(&bytes).map_err(|error| {
            ToolboxError::new(
                ErrorCode::ProjectSchemaInvalid,
                "project.load",
                "project_json_invalid",
            )
            .cause(error)
        })?;
        self.set_project(project.clone())?;
        Ok(project)
    }

    pub fn save_project(&self, path: &Path) -> ToolboxResult<()> {
        let project = self.state.project();
        let bytes = serde_json::to_vec_pretty(&project).map_err(|error| {
            ToolboxError::new(
                ErrorCode::ProjectSchemaInvalid,
                "project.save",
                "project_serialize_failed",
            )
            .cause(error)
        })?;
        fs::write(path, bytes).map_err(|error| {
            ToolboxError::new(
                ErrorCode::ProjectSchemaInvalid,
                "project.save",
                "project_write_failed",
            )
            .cause(error)
        })
    }

    fn start_auto_reconnect_worker(self: &Arc<Self>) {
        let weak_core = Arc::downgrade(self);
        let _ = thread::Builder::new()
            .name("etb-serial-reconnect".into())
            .spawn(move || {
                loop {
                    let Some(core) = weak_core.upgrade() else {
                        break;
                    };
                    core.try_auto_reconnect();
                    drop(core);
                    thread::sleep(AUTO_RECONNECT_POLL_INTERVAL);
                }
            });
    }

    fn remember_serial_reconnect(&self, source_id: Uuid, config: SerialConfig) {
        let device = list_ports().ok().and_then(|ports| {
            ports
                .into_iter()
                .find(|port| port.name.eq_ignore_ascii_case(&config.port_name))
        });
        *self.auto_reconnect.lock() = Some(SerialReconnectPlan {
            source_id,
            config,
            device,
            next_attempt: Instant::now(),
        });
    }

    fn try_auto_reconnect(&self) {
        let now = Instant::now();
        let Some(plan) = self.auto_reconnect.lock().clone() else {
            return;
        };
        if now < plan.next_attempt {
            return;
        }

        let _lifecycle = self.source_lifecycle_guard.lock();
        let Some(current_plan) = self.auto_reconnect.lock().clone() else {
            return;
        };
        if current_plan.source_id != plan.source_id || now < current_plan.next_attempt {
            return;
        }

        let stopped_runtime = {
            let mut sources = self.sources.lock();
            match sources.get(&plan.source_id) {
                Some(runtime) if !runtime.is_stopped() => return,
                Some(_) => sources.remove(&plan.source_id),
                None if sources.is_empty() => None,
                None => return,
            }
        };
        if let Some(runtime) = stopped_runtime {
            runtime.stop();
            self.state.emit(
                "source.reconnectWaiting",
                serde_json::json!({"sourceId": plan.source_id, "endpoint": plan.config.port_name}),
            );
        }

        let Some(config) = select_reconnect_config(&plan) else {
            self.defer_auto_reconnect(plan.source_id);
            return;
        };
        self.state.emit(
            "source.reconnectAttempt",
            serde_json::json!({"sourceId": plan.source_id, "endpoint": config.port_name}),
        );
        match self.connect_serial_locked(config) {
            Ok(snapshot) => {
                self.state.emit(
                    "source.reconnected",
                    serde_json::json!({
                        "sourceId": snapshot.source_id,
                        "sourceEpoch": snapshot.source_epoch,
                        "endpoint": snapshot.endpoint,
                    }),
                );
            }
            Err(_) => self.defer_auto_reconnect(plan.source_id),
        }
    }

    fn defer_auto_reconnect(&self, source_id: Uuid) {
        if let Some(plan) = self.auto_reconnect.lock().as_mut() {
            if plan.source_id == source_id {
                plan.next_attempt = Instant::now() + AUTO_RECONNECT_RETRY_INTERVAL;
            }
        }
    }

    fn enforce_product_source_limit(&self) -> ToolboxResult<()> {
        if self.sources.lock().is_empty() {
            Ok(())
        } else {
            Err(ToolboxError::new(
                ErrorCode::ProductSourceLimit,
                "device.connect",
                "single_source_product_limit",
            ))
        }
    }
}

impl Drop for AppCore {
    fn drop(&mut self) {
        self.replay_stop.store(true, Ordering::Relaxed);
        let sources = std::mem::take(&mut *self.sources.lock());
        for (_, runtime) in sources {
            runtime.stop();
        }
        if self.session.is_active() {
            let _ = self.session.stop();
        }
    }
}

fn serial_read_loop(
    port: &mut dyn serialport::SerialPort,
    stop: &AtomicBool,
    fanout: &Fanout,
) -> ToolboxResult<()> {
    let mut buffer = vec![0u8; 16 * 1024];
    while !stop.load(Ordering::Relaxed) {
        match port.read(&mut buffer) {
            Ok(0) => {}
            Ok(length) => fanout.push(Direction::Rx, buffer[..length].to_vec(), None),
            Err(error) if error.kind() == std::io::ErrorKind::TimedOut => {}
            Err(error) => {
                return Err(ToolboxError::new(
                    ErrorCode::DeviceRead,
                    "device.read",
                    "serial_read_failed",
                )
                .source(fanout.source_id)
                .cause(error));
            }
        }
    }
    Ok(())
}

fn replay_wait_interrupted(stop: &AtomicBool, seek_ns: &AtomicI64, duration: Duration) -> bool {
    let started = Instant::now();
    while started.elapsed() < duration {
        if stop.load(Ordering::Relaxed) || seek_ns.load(Ordering::Acquire) >= 0 {
            return true;
        }
        thread::sleep((duration - started.elapsed()).min(Duration::from_millis(20)));
    }
    stop.load(Ordering::Relaxed) || seek_ns.load(Ordering::Acquire) >= 0
}

fn synthetic_read_loop(
    config: &SyntheticConfig,
    stop: &AtomicBool,
    fanout: &Fanout,
) -> ToolboxResult<()> {
    let rate = config.rate_hz.clamp(1, 100_000);
    let period = Duration::from_nanos(1_000_000_000u64 / rate as u64);
    let mut frame = 0u64;
    let mut measured = 20.0f64;
    let mut rng = config.seed.max(1);
    while !stop.load(Ordering::Relaxed) {
        frame += 1;
        if config
            .faults
            .disconnect_after_frames
            .is_some_and(|n| frame >= n)
        {
            return Err(ToolboxError::new(
                ErrorCode::DeviceDisconnected,
                "synthetic.read",
                "fault_disconnect_injected",
            )
            .source(fanout.source_id));
        }
        if config
            .faults
            .stall_every_frames
            .is_some_and(|n| n != 0 && frame.is_multiple_of(n))
        {
            thread::sleep(Duration::from_millis(250));
        }
        let setpoint: f64 = if (frame / (rate as u64 * 4).max(1)).is_multiple_of(2) {
            25.0
        } else {
            75.0
        };
        rng ^= rng << 13;
        rng ^= rng >> 7;
        rng ^= rng << 17;
        let noise = ((rng % 1000) as f64 / 1000.0 - 0.5) * 0.8;
        measured += (setpoint - measured) * 0.045 + noise;
        let output = ((setpoint - measured) * 1.8 + 50.0).clamp(0.0, 100.0);
        let mut bytes = format!("{setpoint:.3},{measured:.3},{output:.3}\n").into_bytes();
        if config
            .faults
            .corrupt_every_frames
            .is_some_and(|n| n != 0 && frame.is_multiple_of(n))
            && !bytes.is_empty()
        {
            bytes[0] = b'X';
        }
        let dropped = config
            .faults
            .drop_every_frames
            .is_some_and(|n| n != 0 && frame.is_multiple_of(n));
        if !dropped {
            if let Some(fragment) = config.faults.fragment_max_bytes.filter(|value| *value > 0) {
                for part in bytes.chunks(fragment) {
                    fanout.push(Direction::Rx, part.to_vec(), None);
                }
            } else {
                fanout.push(Direction::Rx, bytes.clone(), None);
            }
            if config
                .faults
                .duplicate_every_frames
                .is_some_and(|n| n != 0 && frame.is_multiple_of(n))
            {
                fanout.push(Direction::Rx, bytes.clone(), None);
            }
            if config
                .faults
                .burst_every_frames
                .is_some_and(|n| n != 0 && frame.is_multiple_of(n))
            {
                for _ in 0..9 {
                    fanout.push(Direction::Rx, bytes.clone(), None);
                }
            }
        }
        thread::sleep(period);
    }
    Ok(())
}

fn validate_project(project: &ToolboxProject) -> ToolboxResult<()> {
    if project.project_schema_version != PROJECT_SCHEMA_VERSION {
        return Err(ToolboxError::new(
            ErrorCode::ProjectSchemaInvalid,
            "project.validate",
            "project_schema_unsupported",
        )
        .context("version", &project.project_schema_version));
    }
    if project.pipeline_semantic_version != PIPELINE_SEMANTIC_VERSION {
        return Err(ToolboxError::new(
            ErrorCode::PipelineVersionUnsupported,
            "project.validate",
            "pipeline_version_unsupported",
        )
        .context("version", &project.pipeline_semantic_version));
    }
    let mut ids = HashSet::new();
    let mut insert = |id: Uuid| {
        if ids.insert(id) {
            Ok(())
        } else {
            Err(ToolboxError::new(
                ErrorCode::ProjectSchemaInvalid,
                "project.validate",
                "duplicate_stable_id",
            )
            .context("id", id))
        }
    };
    insert(project.id)?;
    insert(project.source.id)?;
    insert(project.source.serial.id)?;
    match &project.framer {
        FramerSpec::EndDelimiter { id, .. }
        | FramerSpec::StartEnd { id, .. }
        | FramerSpec::FixedLength { id, .. }
        | FramerSpec::LengthField { id, .. } => insert(*id)?,
    }
    if let Some(checksum) = &project.checksum {
        insert(checksum.id)?;
    }
    insert(project.decoder.id())?;
    if let DecoderSpec::Binary { fields, .. } = &project.decoder {
        for field in fields {
            insert(field.id)?;
        }
    }
    for channel in &project.channels {
        insert(channel.id)?;
        for transform in &channel.transforms {
            insert(transform.id())?;
        }
    }
    for command in &project.commands {
        insert(command.id)?;
    }
    for profile in &project.pid_profiles {
        insert(profile.id)?;
    }
    for view in &project.views {
        insert(view.id)?;
    }
    Ok(())
}

fn serial_semantics_equal(left: &SerialConfig, right: &SerialConfig) -> bool {
    left.baud_rate == right.baud_rate
        && left.data_bits == right.data_bits
        && left.stop_bits == right.stop_bits
        && std::mem::discriminant(&left.parity) == std::mem::discriminant(&right.parity)
        && std::mem::discriminant(&left.flow_control) == std::mem::discriminant(&right.flow_control)
}

fn select_reconnect_config(plan: &SerialReconnectPlan) -> Option<SerialConfig> {
    let ports = list_ports().ok()?;
    select_reconnect_config_from_ports(plan, &ports)
}

fn select_reconnect_config_from_ports(
    plan: &SerialReconnectPlan,
    ports: &[SerialPortDescriptor],
) -> Option<SerialConfig> {
    let port = plan
        .device
        .as_ref()
        .and_then(|device| {
            ports
                .iter()
                .find(|candidate| same_serial_device(device, candidate))
        })
        .or_else(|| {
            ports
                .iter()
                .find(|candidate| candidate.name.eq_ignore_ascii_case(&plan.config.port_name))
        })?;
    let mut config = plan.config.clone();
    config.port_name = port.name.clone();
    Some(config)
}

fn same_serial_device(left: &SerialPortDescriptor, right: &SerialPortDescriptor) -> bool {
    if left.kind != right.kind {
        return false;
    }
    if let Some(serial_number) = &left.serial_number {
        return left.vid == right.vid
            && left.pid == right.pid
            && right.serial_number.as_ref() == Some(serial_number);
    }
    if left.vid.is_some() || left.pid.is_some() {
        return left.vid == right.vid
            && left.pid == right.pid
            && optional_text_matches(&left.manufacturer, &right.manufacturer)
            && optional_text_matches(&left.product, &right.product);
    }
    left.name.eq_ignore_ascii_case(&right.name)
}

fn optional_text_matches(left: &Option<String>, right: &Option<String>) -> bool {
    match (left, right) {
        (Some(left), Some(right)) => left == right,
        _ => true,
    }
}

fn disconnected_snapshot(source_id: Uuid) -> SourceSnapshot {
    SourceSnapshot {
        source_id,
        name: "Source".into(),
        status: SourceStatus::Disconnected,
        transport: "unknown".into(),
        endpoint: String::new(),
        source_epoch: 0,
        epoch_id: None,
        clock_anchor: None,
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

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn product_limits_active_source_but_epoch_counter_advances() {
        let core = AppCore::new();
        let first = core
            .connect_synthetic(SyntheticConfig {
                rate_hz: 2,
                ..SyntheticConfig::default()
            })
            .unwrap();
        assert_eq!(first.source_epoch, 1);
        assert_eq!(
            core.connect_synthetic(SyntheticConfig::default())
                .unwrap_err()
                .code,
            ErrorCode::ProductSourceLimit.as_str()
        );
        core.disconnect(first.source_id).unwrap();
        let second = core
            .connect_synthetic(SyntheticConfig {
                rate_hz: 2,
                ..SyntheticConfig::default()
            })
            .unwrap();
        assert_eq!(second.source_epoch, 2);
        core.disconnect(second.source_id).unwrap();
    }

    #[test]
    fn bundled_projects_deserialize_and_validate() {
        let examples = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../examples");
        for name in ["pid-loop.etp", "csv-telemetry.etp", "binary-crc.etp"] {
            let bytes = fs::read(examples.join(name)).unwrap();
            let project: ToolboxProject = serde_json::from_slice(&bytes).unwrap();
            validate_project(&project).unwrap();
        }
    }

    #[test]
    fn recording_spans_disconnect_and_reconnect_epochs() {
        let directory = tempdir().unwrap();
        let session_path = directory.path().join("multi-epoch.etdb");
        let core = AppCore::new();
        let first = core
            .connect_synthetic(SyntheticConfig {
                rate_hz: 500,
                ..SyntheticConfig::default()
            })
            .unwrap();
        core.start_session(session_path.clone()).unwrap();
        thread::sleep(Duration::from_millis(30));
        core.disconnect(first.source_id).unwrap();
        assert_eq!(core.session.snapshot().status, SessionStatus::Suspended);

        let second = core
            .connect_synthetic(SyntheticConfig {
                rate_hz: 500,
                ..SyntheticConfig::default()
            })
            .unwrap();
        assert_eq!(second.source_epoch, 2);
        assert_eq!(core.session.snapshot().epoch_ordinal, Some(2));
        thread::sleep(Duration::from_millis(30));
        assert_eq!(core.stop_session().unwrap().status, SessionStatus::Closed);
        core.disconnect(second.source_id).unwrap();

        let conn = rusqlite::Connection::open(session_path).unwrap();
        let epochs: i64 = conn
            .query_row("SELECT COUNT(*) FROM epochs", [], |row| row.get(0))
            .unwrap();
        let gaps: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM session_events WHERE event_type='StreamGap'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(epochs, 2);
        assert_eq!(gaps, 1);
    }

    #[test]
    fn reconnect_uses_usb_identity_when_the_com_name_changes() {
        let source_id = Uuid::now_v7();
        let config = SerialConfig {
            port_name: "COM10".into(),
            ..SerialConfig::default()
        };
        let plan = SerialReconnectPlan {
            source_id,
            config,
            device: Some(SerialPortDescriptor {
                name: "COM10".into(),
                kind: "usb".into(),
                vid: Some(0x1A86),
                pid: Some(0x7523),
                manufacturer: Some("wch.cn".into()),
                product: Some("USB-SERIAL CH340".into()),
                serial_number: None,
            }),
            next_attempt: Instant::now(),
        };
        let ports = vec![SerialPortDescriptor {
            name: "COM11".into(),
            kind: "usb".into(),
            vid: Some(0x1A86),
            pid: Some(0x7523),
            manufacturer: Some("wch.cn".into()),
            product: Some("USB-SERIAL CH340".into()),
            serial_number: None,
        }];

        let selected = select_reconnect_config_from_ports(&plan, &ports).unwrap();
        assert_eq!(selected.port_name, "COM11");
    }
}
