use std::sync::{Arc, Mutex};
use std::thread;

use crossbeam_channel::Sender;
use zippy_core::{Result as CoreResult, Source, SourceEvent, SourceSink};
use zippy_openctp_core::source::{
    evaluate_subscription_results, MdDriverEvent, MdDriverHandle, SubscriptionOutcome,
};
use zippy_openctp_core::{
    MdDriver, OpenCtpMarketDataSource, OpenCtpMarketDataSourceConfig, OpenCtpSourceStatus,
    OpenCtpSourceMetrics,
};

#[test]
fn evaluate_subscription_results_marks_partial_success_as_degraded() {
    let outcome = evaluate_subscription_results(
        &["IF2506".to_string(), "IH2506".to_string()],
        &["IF2506".to_string()],
    );

    assert_eq!(
        outcome,
        SubscriptionOutcome {
            succeeded_instruments: vec!["IF2506".to_string()],
            failed_instruments: vec!["IH2506".to_string()],
            subscribe_failures_total: 1,
            status: OpenCtpSourceStatus::Degraded,
        }
    );
}

#[test]
fn source_tracks_partial_subscription_failures_without_failing_runtime() {
    let sink = Arc::new(RecordingSink::default());
    let source = OpenCtpMarketDataSource::from_driver(
        default_source_config(),
        Box::new(FakeMdDriver {
            events: vec![
                MdDriverEvent::SubscriptionOutcome(SubscriptionOutcome {
                    succeeded_instruments: vec!["IF2506".to_string()],
                    failed_instruments: vec!["IH2506".to_string()],
                    subscribe_failures_total: 1,
                    status: OpenCtpSourceStatus::Degraded,
                }),
                MdDriverEvent::Stop,
            ],
        }),
    );

    let handle = Box::new(source)
        .start(sink.clone())
        .expect("source start should succeed");
    handle.join().expect("source thread should exit cleanly");

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

#[test]
fn metrics_count_partial_subscription_failures() {
    let mut metrics = OpenCtpSourceMetrics::default();
    let outcome = evaluate_subscription_results(
        &["IF2506".to_string(), "IH2506".to_string()],
        &["IF2506".to_string()],
    );

    metrics.subscribe_failures_total += outcome.subscribe_failures_total;

    assert_eq!(metrics.subscribe_failures_total, 1);
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
    events: Vec<MdDriverEvent>,
}

impl MdDriver for FakeMdDriver {
    fn start(self: Box<Self>, tx: Sender<MdDriverEvent>) -> CoreResult<MdDriverHandle> {
        let join_handle = thread::spawn(move || -> CoreResult<()> {
            for event in self.events {
                tx.send(event).map_err(|_| zippy_core::ZippyError::ChannelSend)?;
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
        vec!["IF2506".to_string(), "IH2506".to_string()],
        ".cache/openctp/md".to_string(),
    )
}
