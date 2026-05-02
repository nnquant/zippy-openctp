use std::sync::{
    atomic::{AtomicBool, Ordering},
    mpsc, Arc, Mutex,
};
use std::thread;
use std::time::{Duration, Instant};

use crossbeam_channel::Sender;
use zippy_core::{Result as CoreResult, Source, SourceEvent, SourceSink};
use zippy_openctp_core::source::{MdDriverEvent, MdDriverHandle};
use zippy_openctp_core::{
    MdDriver, OpenCtpMarketDataSource, OpenCtpMarketDataSourceConfig, OpenCtpSourceStatus,
    RawTickSnapshot,
};

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
    events: Vec<MdDriverEvent>,
    delay_before_stop: Duration,
}

impl FakeMdDriver {
    fn sample_sequence() -> Vec<MdDriverEvent> {
        vec![
            MdDriverEvent::Tick(sample_tick("IF2506", 1)),
            MdDriverEvent::Stop,
        ]
    }
}

impl MdDriver for FakeMdDriver {
    fn start(self: Box<Self>, tx: Sender<MdDriverEvent>) -> CoreResult<MdDriverHandle> {
        let join_handle = thread::spawn(move || -> CoreResult<()> {
            for event in self.events {
                if matches!(event, MdDriverEvent::Stop) {
                    thread::sleep(self.delay_before_stop);
                }
                tx.send(event)
                    .map_err(|_| zippy_core::ZippyError::ChannelSend)?;
            }
            Ok(())
        });

        Ok(MdDriverHandle::new(join_handle))
    }
}

#[test]
fn fake_md_driver_emits_data_and_stop_events() {
    let sink = Arc::new(RecordingSink::default());
    let source = OpenCtpMarketDataSource::from_driver(
        default_source_config(),
        Box::new(FakeMdDriver {
            events: FakeMdDriver::sample_sequence(),
            delay_before_stop: Duration::from_millis(50),
        }),
    );
    let status = source.status_handle();

    assert_eq!(*status.lock().unwrap(), OpenCtpSourceStatus::Created);

    let handle = Box::new(source)
        .start(sink.clone())
        .expect("source start should succeed");

    wait_for_status(&status, OpenCtpSourceStatus::Running);

    handle.join().expect("source thread should exit cleanly");

    assert_eq!(*status.lock().unwrap(), OpenCtpSourceStatus::Stopped);

    assert_eq!(
        sink.snapshot(),
        vec![
            RecordedEvent::Hello {
                stream_name: "openctp.tick".to_string(),
            },
            RecordedEvent::Data { rows: 1 },
            RecordedEvent::Stop,
        ]
    );
}

#[test]
fn source_skips_malformed_tick_and_continues_emitting_valid_rows() {
    let mut malformed_tick = sample_tick("IF2506", 1);
    malformed_tick.update_time = "invalid".to_string();

    let sink = Arc::new(RecordingSink::default());
    let source = OpenCtpMarketDataSource::from_driver(
        default_source_config(),
        Box::new(FakeMdDriver {
            events: vec![
                MdDriverEvent::Tick(malformed_tick),
                MdDriverEvent::Tick(sample_tick("IF2506", 2)),
                MdDriverEvent::Stop,
            ],
            delay_before_stop: Duration::from_millis(50),
        }),
    );
    let status = source.status_handle();
    let metrics = source.metrics_handle();

    let handle = Box::new(source)
        .start(sink.clone())
        .expect("source start should succeed");

    wait_for_status(&status, OpenCtpSourceStatus::Running);

    handle.join().expect("source thread should exit cleanly");

    assert_eq!(*status.lock().unwrap(), OpenCtpSourceStatus::Stopped);

    assert_eq!(
        sink.snapshot(),
        vec![
            RecordedEvent::Hello {
                stream_name: "openctp.tick".to_string(),
            },
            RecordedEvent::Data { rows: 1 },
            RecordedEvent::Stop,
        ]
    );

    let metrics = metrics.lock().unwrap().clone();
    assert_eq!(metrics.ticks_received_total, 2);
    assert_eq!(metrics.ticks_emitted_total, 1);
    assert_eq!(metrics.batches_emitted_total, 1);
    assert_eq!(metrics.normalize_failures_total, 1);
}

#[test]
fn source_requests_driver_stop_before_joining_after_sink_failure() {
    let stop_called = Arc::new(AtomicBool::new(false));
    let sink = Arc::new(FailingDataSink::default());
    let source = OpenCtpMarketDataSource::from_driver(
        default_source_config(),
        Box::new(StopAwareTickDriver {
            stop_called: stop_called.clone(),
        }),
    );
    let status = source.status_handle();

    let handle = Box::new(source)
        .start(sink)
        .expect("source start should succeed");

    let err = handle.join().unwrap_err();

    assert!(err.to_string().contains("forced sink failure"));
    assert!(stop_called.load(Ordering::SeqCst));
    assert_eq!(*status.lock().unwrap(), OpenCtpSourceStatus::Failed);
}

#[test]
fn source_stop_exits_without_draining_queued_ticks() {
    let stop_called = Arc::new(AtomicBool::new(false));
    let sink = Arc::new(SlowDataSink::default());
    let source = OpenCtpMarketDataSource::from_driver(
        default_source_config(),
        Box::new(FloodingTickDriver {
            stop_called: stop_called.clone(),
        }),
    );
    let status = source.status_handle();

    let handle = Box::new(source)
        .start(sink.clone())
        .expect("source start should succeed");

    wait_for_status(&status, OpenCtpSourceStatus::Running);
    sink.wait_for_data_event();

    handle.stop().expect("source stop should be requested");
    let (join_tx, join_rx) = mpsc::channel();
    thread::spawn(move || {
        join_tx.send(handle.join()).unwrap();
    });
    let join_result = join_rx
        .recv_timeout(Duration::from_secs(1))
        .expect("source join should not drain queued ticks indefinitely");

    assert!(join_result.is_ok());
    assert!(stop_called.load(Ordering::SeqCst));
    assert_eq!(*status.lock().unwrap(), OpenCtpSourceStatus::Stopped);
}

fn wait_for_status(status: &Arc<Mutex<OpenCtpSourceStatus>>, expected: OpenCtpSourceStatus) {
    let deadline = Instant::now() + Duration::from_secs(1);
    loop {
        if *status.lock().unwrap() == expected {
            return;
        }

        if Instant::now() >= deadline {
            panic!("timed out waiting for status [{expected:?}]");
        }

        thread::sleep(Duration::from_millis(10));
    }
}

#[derive(Default)]
struct SlowDataSink {
    data_events: Mutex<usize>,
}

impl SlowDataSink {
    fn wait_for_data_event(&self) {
        let deadline = Instant::now() + Duration::from_secs(1);
        loop {
            if *self.data_events.lock().unwrap() > 0 {
                return;
            }

            if Instant::now() >= deadline {
                panic!("timed out waiting for data event");
            }

            thread::sleep(Duration::from_millis(10));
        }
    }
}

impl SourceSink for SlowDataSink {
    fn emit(&self, event: SourceEvent) -> CoreResult<()> {
        if matches!(event, SourceEvent::Data(_)) {
            *self.data_events.lock().unwrap() += 1;
            thread::sleep(Duration::from_millis(5));
        }
        Ok(())
    }
}

#[derive(Default)]
struct FailingDataSink {
    events: Mutex<Vec<RecordedEvent>>,
}

impl SourceSink for FailingDataSink {
    fn emit(&self, event: SourceEvent) -> CoreResult<()> {
        let recorded = match event {
            SourceEvent::Hello(hello) => RecordedEvent::Hello {
                stream_name: hello.stream_name,
            },
            SourceEvent::Data(batch) => {
                let recorded = RecordedEvent::Data {
                    rows: batch.num_rows(),
                };
                self.events.lock().unwrap().push(recorded);
                return Err(zippy_core::ZippyError::Io {
                    reason: "forced sink failure".to_string(),
                });
            }
            SourceEvent::Flush => RecordedEvent::Flush,
            SourceEvent::Stop => RecordedEvent::Stop,
            SourceEvent::Error(reason) => RecordedEvent::Error { reason },
        };
        self.events.lock().unwrap().push(recorded);
        Ok(())
    }
}

struct StopAwareTickDriver {
    stop_called: Arc<AtomicBool>,
}

impl MdDriver for StopAwareTickDriver {
    fn start(self: Box<Self>, tx: Sender<MdDriverEvent>) -> CoreResult<MdDriverHandle> {
        let stop_called_for_thread = self.stop_called.clone();
        let join_handle = thread::spawn(move || -> CoreResult<()> {
            tx.send(MdDriverEvent::Tick(sample_tick("IF2506", 1)))
                .map_err(|_| zippy_core::ZippyError::ChannelSend)?;

            let deadline = Instant::now() + Duration::from_millis(500);
            while Instant::now() < deadline {
                if stop_called_for_thread.load(Ordering::SeqCst) {
                    return Ok(());
                }
                thread::sleep(Duration::from_millis(5));
            }

            Err(zippy_core::ZippyError::Io {
                reason: "driver stop was not requested".to_string(),
            })
        });
        let stop_called_for_stop = self.stop_called.clone();
        Ok(MdDriverHandle::new_with_stop(
            join_handle,
            Box::new(move || {
                stop_called_for_stop.store(true, Ordering::SeqCst);
                Ok(())
            }),
        ))
    }
}

struct FloodingTickDriver {
    stop_called: Arc<AtomicBool>,
}

impl MdDriver for FloodingTickDriver {
    fn start(self: Box<Self>, tx: Sender<MdDriverEvent>) -> CoreResult<MdDriverHandle> {
        let stop_called_for_thread = self.stop_called.clone();
        let join_handle = thread::spawn(move || -> CoreResult<()> {
            let tick = sample_tick("IF2506", 1);
            while !stop_called_for_thread.load(Ordering::SeqCst) {
                tx.send(MdDriverEvent::Ticks(vec![tick.clone(); 1_000]))
                    .map_err(|_| zippy_core::ZippyError::ChannelSend)?;
            }
            tx.send(MdDriverEvent::Stop)
                .map_err(|_| zippy_core::ZippyError::ChannelSend)?;
            Ok(())
        });
        let stop_called_for_stop = self.stop_called.clone();
        Ok(MdDriverHandle::new_with_stop(
            join_handle,
            Box::new(move || {
                stop_called_for_stop.store(true, Ordering::SeqCst);
                Ok(())
            }),
        ))
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

fn sample_tick(instrument_id: &str, volume: i64) -> RawTickSnapshot {
    RawTickSnapshot {
        instrument_id: instrument_id.to_string(),
        exchange_id: "CFFEX".to_string(),
        trading_day: "20260408".to_string(),
        action_day: "20260408".to_string(),
        update_time: "09:30:00".to_string(),
        update_millisec: 500,
        last_price: 3912.4,
        volume,
        turnover: 987654.0,
        open_interest: 56789.0,
        bid_price_1: 3912.2,
        bid_volume_1: 10,
        ask_price_1: 3912.6,
        ask_volume_1: 8,
    }
}
