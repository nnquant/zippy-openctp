use std::sync::{Arc, Mutex};
use std::thread;

use crossbeam_channel::Sender;
use zippy_core::{Result as CoreResult, Source, SourceEvent, SourceSink};
use zippy_openctp_core::source::{MdDriverEvent, MdDriverHandle};
use zippy_openctp_core::{
    MdDriver, OpenCtpMarketDataSource, OpenCtpMarketDataSourceConfig, RawTickSnapshot,
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
                tx.send(event).map_err(|_| zippy_core::ZippyError::ChannelSend)?;
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
            RecordedEvent::Data { rows: 1 },
            RecordedEvent::Stop,
        ]
    );
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
