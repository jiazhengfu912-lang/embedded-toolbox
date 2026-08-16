use crate::core::error::{ErrorCode, ToolboxError, ToolboxResult};
use crate::core::event::StateStore;
use crate::core::model::{FaultPlan, TxAccepted, TxRequest};
use crate::core::queue::{ByteQueue, ByteSize, OverflowPolicy};
use crate::core::session::utc_now_ns;
use parking_lot::Mutex;
use serialport::SerialPort;
use std::collections::HashSet;
use std::io::Write;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};
use uuid::Uuid;

pub trait TxTarget: Send + 'static {
    fn write_payload(&mut self, payload: &[u8]) -> std::io::Result<usize>;
}

pub struct SerialTxTarget(pub Box<dyn SerialPort>);
impl TxTarget for SerialTxTarget {
    fn write_payload(&mut self, payload: &[u8]) -> std::io::Result<usize> {
        self.0.write(payload)
    }
}

pub struct SyntheticTxTarget {
    faults: FaultPlan,
    jobs: u64,
}

impl SyntheticTxTarget {
    pub fn new(faults: FaultPlan) -> Self {
        Self { faults, jobs: 0 }
    }
}

impl TxTarget for SyntheticTxTarget {
    fn write_payload(&mut self, payload: &[u8]) -> std::io::Result<usize> {
        self.jobs += 1;
        if self
            .faults
            .fail_write_every_jobs
            .is_some_and(|n| n != 0 && self.jobs.is_multiple_of(n))
        {
            return Err(std::io::Error::new(
                std::io::ErrorKind::BrokenPipe,
                "synthetic injected write failure",
            ));
        }
        if self
            .faults
            .partial_write_every_jobs
            .is_some_and(|n| n != 0 && self.jobs.is_multiple_of(n))
        {
            return Ok((payload.len() / 2).max(1));
        }
        Ok(payload.len())
    }
}

#[derive(Clone)]
struct TxJob {
    id: Uuid,
    request: TxRequest,
}

impl ByteSize for TxJob {
    fn byte_size(&self) -> usize {
        self.request.payload.len().max(1)
    }
}

pub struct TxScheduler {
    queue: Arc<ByteQueue<TxJob>>,
    cancelled: Arc<Mutex<HashSet<Uuid>>>,
    stop: Arc<AtomicBool>,
    join: Option<JoinHandle<()>>,
}

impl TxScheduler {
    pub fn spawn(
        source_epoch: u64,
        mut target: Box<dyn TxTarget>,
        state: Arc<StateStore>,
        stop: Arc<AtomicBool>,
        on_written: Arc<dyn Fn(Vec<u8>, Uuid) + Send + Sync>,
    ) -> Self {
        let queue = Arc::new(ByteQueue::<TxJob>::new(1024 * 1024, 1024));
        let cancelled = Arc::new(Mutex::new(HashSet::new()));
        let worker_queue = Arc::clone(&queue);
        let worker_cancelled = Arc::clone(&cancelled);
        let worker_stop = Arc::clone(&stop);
        let join = thread::Builder::new().name("etb-tx".into()).spawn(move || {
            let mut last_sent = Instant::now() - Duration::from_secs(60);
            'jobs: while !worker_stop.load(Ordering::Relaxed) || worker_queue.depth_items() > 0 {
                let Some(job) = worker_queue.pop_timeout(Duration::from_millis(20)) else { continue };
                if worker_cancelled.lock().remove(&job.id) || worker_stop.load(Ordering::Relaxed) {
                    state.emit("tx.cancelled", serde_json::json!({"jobId": job.id, "sourceId": job.request.source_id, "sourceEpoch": source_epoch}));
                    continue;
                }
                loop {
                    if worker_cancelled.lock().remove(&job.id) || worker_stop.load(Ordering::Relaxed) {
                        state.emit("tx.cancelled", serde_json::json!({"jobId": job.id, "sourceId": job.request.source_id, "sourceEpoch": source_epoch}));
                        continue 'jobs;
                    }
                    let now_ms = utc_now_ns() / 1_000_000;
                    if job.request.deadline_unix_ms.is_some_and(|deadline| now_ms >= deadline) {
                        state.emit("tx.failed", serde_json::json!({"jobId": job.id, "code": ErrorCode::TxTimeout.as_str()}));
                        continue 'jobs;
                    }
                    let Some(not_before) = job.request.not_before_unix_ms.filter(|value| *value > now_ms) else { break };
                    thread::sleep(Duration::from_millis(not_before.saturating_sub(now_ms).min(20) as u64));
                }
                let gap = Duration::from_millis(job.request.min_gap_ms.unwrap_or(0));
                while last_sent.elapsed() < gap {
                    if worker_cancelled.lock().remove(&job.id) || worker_stop.load(Ordering::Relaxed) {
                        state.emit("tx.cancelled", serde_json::json!({"jobId": job.id, "sourceId": job.request.source_id, "sourceEpoch": source_epoch}));
                        continue 'jobs;
                    }
                    if job.request.deadline_unix_ms.is_some_and(|deadline| utc_now_ns() / 1_000_000 >= deadline) {
                        state.emit("tx.failed", serde_json::json!({"jobId": job.id, "code": ErrorCode::TxTimeout.as_str()}));
                        continue 'jobs;
                    }
                    thread::sleep((gap - last_sent.elapsed()).min(Duration::from_millis(20)));
                }
                state.emit("tx.sending", serde_json::json!({"jobId": job.id, "sourceId": job.request.source_id, "sourceEpoch": source_epoch, "origin": job.request.origin}));
                match target.write_payload(&job.request.payload) {
                    Ok(written) if written == job.request.payload.len() => {
                        on_written(job.request.payload.clone(), job.id);
                        last_sent = Instant::now();
                        state.emit("tx.sent", serde_json::json!({"jobId": job.id, "bytes": written, "sourceEpoch": source_epoch}));
                    }
                    Ok(written) => {
                        if written > 0 { on_written(job.request.payload[..written].to_vec(), job.id); }
                        let error = ToolboxError::new(ErrorCode::TxPartialWrite, "tx.write", "partial_write")
                            .source(job.request.source_id).context("jobId", job.id).context("written", written).context("expected", job.request.payload.len());
                        state.push_error(error);
                        state.emit("tx.failed", serde_json::json!({"jobId": job.id, "code": ErrorCode::TxPartialWrite.as_str(), "written": written}));
                    }
                    Err(cause) => {
                        let error = ToolboxError::new(ErrorCode::DeviceWrite, "tx.write", "device_write_failed")
                            .source(job.request.source_id).context("jobId", job.id).cause(cause);
                        state.push_error(error);
                        state.emit("tx.failed", serde_json::json!({"jobId": job.id, "code": ErrorCode::DeviceWrite.as_str()}));
                    }
                }
            }
        }).expect("failed to spawn TX scheduler");
        Self {
            queue,
            cancelled,
            stop,
            join: Some(join),
        }
    }

    pub fn enqueue(&self, request: TxRequest) -> ToolboxResult<TxAccepted> {
        let id = Uuid::now_v7();
        let source_id = request.source_id;
        self.queue
            .push(TxJob { id, request }, OverflowPolicy::Reject)
            .map_err(|_| {
                ToolboxError::new(ErrorCode::TxQueueFull, "tx.enqueue", "tx_queue_full")
                    .source(source_id)
            })?;
        Ok(TxAccepted { job_id: id })
    }

    pub fn cancel(&self, job_id: Uuid) {
        self.cancelled.lock().insert(job_id);
    }
    pub fn depth_bytes(&self) -> usize {
        self.queue.depth_bytes()
    }

    pub fn stop(&mut self) {
        self.stop.store(true, Ordering::Relaxed);
        self.queue.close();
        if let Some(join) = self.join.take() {
            let _ = join.join();
        }
    }
}

impl Drop for TxScheduler {
    fn drop(&mut self) {
        self.stop();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    struct RecordingTarget(Arc<Mutex<Vec<Vec<u8>>>>);

    impl TxTarget for RecordingTarget {
        fn write_payload(&mut self, payload: &[u8]) -> std::io::Result<usize> {
            self.0.lock().push(payload.to_vec());
            Ok(payload.len())
        }
    }

    #[test]
    fn synthetic_target_injects_partial_write() {
        let mut target = SyntheticTxTarget::new(FaultPlan {
            partial_write_every_jobs: Some(1),
            ..FaultPlan::default()
        });
        assert_eq!(target.write_payload(b"1234").unwrap(), 2);
    }

    #[test]
    fn scheduler_preserves_fifo_and_skips_cancelled_or_expired_jobs() {
        let state = Arc::new(StateStore::new(crate::core::model::ToolboxProject::demo()));
        let stop = Arc::new(AtomicBool::new(false));
        let writes = Arc::new(Mutex::new(Vec::new()));
        let mut scheduler = TxScheduler::spawn(
            1,
            Box::new(RecordingTarget(Arc::clone(&writes))),
            state,
            stop,
            Arc::new(|_, _| {}),
        );
        let source_id = Uuid::now_v7();
        let request = |payload, not_before, deadline| TxRequest {
            source_id,
            payload,
            origin: "test".into(),
            not_before_unix_ms: not_before,
            deadline_unix_ms: deadline,
            min_gap_ms: Some(0),
        };
        scheduler.enqueue(request(vec![1], None, None)).unwrap();
        let cancelled = scheduler
            .enqueue(request(vec![2], Some(utc_now_ns() / 1_000_000 + 500), None))
            .unwrap();
        scheduler.cancel(cancelled.job_id);
        scheduler
            .enqueue(request(vec![3], None, Some(utc_now_ns() / 1_000_000 - 1)))
            .unwrap();
        thread::sleep(Duration::from_millis(80));
        scheduler.stop();
        assert_eq!(*writes.lock(), vec![vec![1]]);
    }
}
