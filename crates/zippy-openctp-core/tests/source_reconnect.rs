use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};

use crossbeam_channel::Sender;
use zippy_core::{Result as CoreResult, Source, SourceEvent, SourceSink};
use zippy_openctp_core::source::{MdDriverEvent, MdDriverHandle, ReconnectState};
use zippy_openctp_core::{
    MdDriver, OpenCtpMarketDataSource, OpenCtpMarketDataSourceConfig, OpenCtpSourceStatus,
};

#[test]
fn reconnect_state_marks_degraded_then_running_after_reconnect() {
    let start = Instant::now();
    let mut state = ReconnectState::new(Duration::from_secs(3));

    state.mark_disconnected_at(start);
    assert_eq!(state.status(), OpenCtpSourceStatus::Degraded);
    assert_eq!(state.reconnects_total(), 0);
    assert!(!state.ready_to_reconnect_at(start + Duration::from_secs(2)));
    assert!(state.ready_to_reconnect_at(start + Duration::from_secs(3)));

    state.mark_reconnected();
    assert_eq!(state.status(), OpenCtpSourceStatus::Running);
    assert_eq!(state.reconnects_total(), 1);
}

#[test]
fn source_applies_reconnect_updates_without_failing() {
    let sink = Arc::new(RecordingSink::default());
    let source = OpenCtpMarketDataSource::from_driver(
        default_source_config(),
        Box::new(FakeMdDriver {
            timed_events: vec![
                TimedEvent::new(
                    Duration::ZERO,
                    MdDriverEvent::ReconnectUpdate(zippy_openctp_core::source::ReconnectUpdate {
                        reconnects_total: 1,
                        status: OpenCtpSourceStatus::Degraded,
                    }),
                ),
                TimedEvent::new(
                    Duration::from_millis(20),
                    MdDriverEvent::ReconnectUpdate(zippy_openctp_core::source::ReconnectUpdate {
                        reconnects_total: 1,
                        status: OpenCtpSourceStatus::Running,
                    }),
                ),
                TimedEvent::new(Duration::from_millis(20), MdDriverEvent::Stop),
            ],
        }),
    );
    let status_handle = source.status_handle();
    let metrics_handle = source.metrics_handle();

    let handle = Box::new(source)
        .start(sink.clone())
        .expect("source start should succeed");
    wait_for_status(
        &status_handle,
        OpenCtpSourceStatus::Degraded,
        Duration::from_secs(1),
    );
    wait_for_status(
        &status_handle,
        OpenCtpSourceStatus::Running,
        Duration::from_secs(1),
    );
    handle.join().expect("source thread should exit cleanly");

    assert_eq!(metrics_handle.lock().unwrap().reconnects_total, 1);
    assert_eq!(*status_handle.lock().unwrap(), OpenCtpSourceStatus::Stopped);
    assert_eq!(
        sink.snapshot(),
        vec![
            RecordedEvent::Hello {
                stream_name: "openctp.tick".to_string(),
            },
            RecordedEvent::Stop,
        ]
    );
}

#[derive(Default)]
struct RecordingSink {
    events: Mutex<Vec<RecordedEvent>>,
}

impl RecordingSink {
    fn snapshot(&self) -> Vec<RecordedEvent> {
        self.events.lock().unwrap().clone()
    }
}

impl SourceSink for RecordingSink {
    fn emit(&self, event: SourceEvent) -> CoreResult<()> {
        let recorded = match event {
            SourceEvent::Hello(hello) => RecordedEvent::Hello {
                stream_name: hello.stream_name,
            },
            SourceEvent::Data(batch) => RecordedEvent::Data {
                rows: batch.num_rows(),
            },
            SourceEvent::Flush => RecordedEvent::Flush,
            SourceEvent::Stop => RecordedEvent::Stop,
            SourceEvent::Error(reason) => RecordedEvent::Error { reason },
        };
        self.events.lock().unwrap().push(recorded);
        Ok(())
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
enum RecordedEvent {
    Hello { stream_name: String },
    Data { rows: usize },
    Flush,
    Stop,
    Error { reason: String },
}

struct FakeMdDriver {
    timed_events: Vec<TimedEvent>,
}

impl MdDriver for FakeMdDriver {
    fn start(self: Box<Self>, tx: Sender<MdDriverEvent>) -> CoreResult<MdDriverHandle> {
        let join_handle = thread::spawn(move || -> CoreResult<()> {
            for timed_event in self.timed_events {
                if !timed_event.delay.is_zero() {
                    thread::sleep(timed_event.delay);
                }
                tx.send(timed_event.event)
                    .map_err(|_| zippy_core::ZippyError::ChannelSend)?;
            }
            Ok(())
        });

        Ok(MdDriverHandle::new(join_handle))
    }
}

fn default_source_config() -> OpenCtpMarketDataSourceConfig {
    OpenCtpMarketDataSourceConfig::new(
        "tcp://127.0.0.1:12345".to_string(),
        "9999".to_string(),
        "000001".to_string(),
        "secret".to_string(),
        vec!["IF2506".to_string()],
        ".cache/openctp/md".to_string(),
    )
}

struct TimedEvent {
    delay: Duration,
    event: MdDriverEvent,
}

impl TimedEvent {
    fn new(delay: Duration, event: MdDriverEvent) -> Self {
        Self { delay, event }
    }
}

fn wait_for_status(
    status_handle: &Arc<Mutex<OpenCtpSourceStatus>>,
    expected: OpenCtpSourceStatus,
    timeout: Duration,
) {
    let start = Instant::now();
    while start.elapsed() <= timeout {
        if *status_handle.lock().unwrap() == expected {
            return;
        }
        thread::sleep(Duration::from_millis(5));
    }

    panic!(
        "source did not reach expected status expected=[{}] actual=[{}]",
        expected.as_str(),
        status_handle.lock().unwrap().as_str(),
    );
}
