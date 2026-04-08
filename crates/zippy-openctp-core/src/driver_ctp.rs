use zippy_core::{Result as CoreResult, ZippyError};

use crate::source::{MdDriver, MdDriverEvent, MdDriverHandle, OpenCtpMarketDataSourceConfig};

pub struct Ctp2rsMdDriver {
    config: OpenCtpMarketDataSourceConfig,
}

impl Ctp2rsMdDriver {
    pub fn new(config: OpenCtpMarketDataSourceConfig) -> Self {
        Self { config }
    }

    pub fn instruments(&self) -> &[String] {
        self.config.instruments.as_slice()
    }
}

impl MdDriver for Ctp2rsMdDriver {
    fn start(
        self: Box<Self>,
        _tx: crossbeam_channel::Sender<MdDriverEvent>,
    ) -> CoreResult<MdDriverHandle> {
        Err(ZippyError::Io {
            reason: "ctp2rs live driver wiring is not implemented yet".to_string(),
        })
    }
}
