use std::env;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::thread;

use crossbeam_channel::{select, unbounded, Sender};
use ctp2rs::ffi::{gb18030_cstr_i8_to_str, resolve_dynlib_path, DynLibKind, SetString};
use ctp2rs::v1alpha1::{
    CThostFtdcDepthMarketDataField, CThostFtdcReqUserLoginField, CThostFtdcRspInfoField, MdApi,
    MdApiBuilder, MdSpi,
};
use zippy_core::{Result as CoreResult, ZippyError};

use crate::normalize::RawTickSnapshot;
use crate::source::{MdDriver, MdDriverEvent, MdDriverHandle, OpenCtpMarketDataSourceConfig};

const DRIVER_UNIMPLEMENTED_REASON: &str = "ctp2rs live driver wiring is not implemented yet";
const LOGIN_REQUEST_ID: i32 = 1;

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
    fn start(self: Box<Self>, tx: Sender<MdDriverEvent>) -> CoreResult<MdDriverHandle> {
        let dynlib_path = resolve_live_md_dynlib_path()?;
        let config = self.config;
        let stopping = Arc::new(AtomicBool::new(false));
        let api_holder = Arc::new(std::sync::Mutex::new(None::<MdApi>));
        let (stop_tx, stop_rx) = unbounded::<()>();
        let join_stopping = stopping.clone();
        let join_api_holder = api_holder.clone();

        let join_handle = thread::spawn(move || -> CoreResult<()> {
            let api = MdApiBuilder::new()
                .with_dynlib(&dynlib_path)
                .flow_path(&config.flow_path)
                .using_udp(false)
                .multicast(false)
                .build()
                .map_err(|reason| ZippyError::Io {
                    reason: format!("failed to build ctp md api: {reason}"),
                })?;

            let (control_tx, control_rx) = unbounded();
            let spi = Box::new(LiveMdSpi::new(control_tx, tx.clone(), join_stopping.clone()));

            api.register_spi(Box::into_raw(spi));
            api.register_front(&config.front);
            api.init();

            {
                let mut guard = join_api_holder.lock().unwrap();
                *guard = Some(api);
            }

            let loop_result = run_live_driver_loop(
                &config,
                &join_api_holder,
                &join_stopping,
                &stop_rx,
                &control_rx,
                &tx,
            );

            let join_result = {
                let guard = join_api_holder.lock().unwrap();
                match guard.as_ref() {
                    Some(api) => api.join(),
                    None => 0,
                }
            };

            {
                let mut guard = join_api_holder.lock().unwrap();
                guard.take();
            }

            if join_result != 0 && loop_result.is_ok() && !join_stopping.load(Ordering::SeqCst) {
                return Err(ZippyError::Io {
                    reason: format!("ctp md api join returned non-zero [{join_result}]"),
                });
            }

            if join_stopping.load(Ordering::SeqCst) {
                tx.send(MdDriverEvent::Stop)
                    .map_err(|_| ZippyError::ChannelSend)?;
            }

            loop_result
        });

        let stop_fn = {
            let stop_tx = stop_tx.clone();
            let stop_api_holder = api_holder.clone();
            let stop_stopping = stopping.clone();

            Box::new(move || -> CoreResult<()> {
                stop_stopping.store(true, Ordering::SeqCst);
                stop_tx.send(()).map_err(|_| ZippyError::ChannelSend)?;

                if let Some(api) = stop_api_holder.lock().unwrap().as_ref() {
                    api.release();
                }

                Ok(())
            })
        };

        Ok(MdDriverHandle::new_with_stop(join_handle, stop_fn))
    }
}

#[derive(Debug)]
enum LiveMdControlEvent {
    FrontConnected,
    UserLoginSucceeded,
    DriverError(String),
}

struct LiveMdSpi {
    control_tx: Sender<LiveMdControlEvent>,
    data_tx: Sender<MdDriverEvent>,
    stopping: Arc<AtomicBool>,
}

impl LiveMdSpi {
    fn new(
        control_tx: Sender<LiveMdControlEvent>,
        data_tx: Sender<MdDriverEvent>,
        stopping: Arc<AtomicBool>,
    ) -> Self {
        Self {
            control_tx,
            data_tx,
            stopping,
        }
    }
}

impl MdSpi for LiveMdSpi {
    fn on_front_connected(&mut self) {
        let _ = self.control_tx.send(LiveMdControlEvent::FrontConnected);
    }

    fn on_front_disconnected(&mut self, reason: i32) {
        if self.stopping.load(Ordering::SeqCst) {
            return;
        }

        let _ = self.control_tx.send(LiveMdControlEvent::DriverError(format!(
            "ctp md front disconnected reason=[{reason}]"
        )));
    }

    fn on_rsp_user_login(
        &mut self,
        _rsp_user_login: Option<&ctp2rs::v1alpha1::CThostFtdcRspUserLoginField>,
        rsp_info: Option<&CThostFtdcRspInfoField>,
        _request_id: i32,
        is_last: bool,
    ) {
        if !is_last {
            return;
        }

        if let Some(error) = rsp_info_error_reason(rsp_info) {
            let _ = self.control_tx.send(LiveMdControlEvent::DriverError(error));
            return;
        }

        let _ = self
            .control_tx
            .send(LiveMdControlEvent::UserLoginSucceeded);
    }

    fn on_rsp_error(
        &mut self,
        rsp_info: Option<&CThostFtdcRspInfoField>,
        _request_id: i32,
        is_last: bool,
    ) {
        if !is_last || self.stopping.load(Ordering::SeqCst) {
            return;
        }

        let reason = rsp_info_error_reason(rsp_info)
            .unwrap_or_else(|| "ctp md rsp error without detailed info".to_string());
        let _ = self.control_tx.send(LiveMdControlEvent::DriverError(reason));
    }

    fn on_rtn_depth_market_data(
        &mut self,
        depth_market_data: Option<&CThostFtdcDepthMarketDataField>,
    ) {
        let Some(depth_market_data) = depth_market_data else {
            return;
        };

        let raw = match raw_tick_from_depth_market_data(depth_market_data) {
            Ok(raw) => raw,
            Err(error) => {
                let _ = self
                    .control_tx
                    .send(LiveMdControlEvent::DriverError(error.to_string()));
                return;
            }
        };

        let _ = self.data_tx.send(MdDriverEvent::Tick(raw));
    }
}

fn resolve_live_md_dynlib_path() -> CoreResult<PathBuf> {
    if let Some(path) = env::var_os("OPENCTP_MD_DYNLIB_PATH") {
        return Ok(PathBuf::from(path));
    }

    if let Some(dir) = env::var_os("OPENCTP_MD_DYNLIB_DIR") {
        return Ok(resolve_dynlib_path(PathBuf::from(dir), DynLibKind::MdApi));
    }

    Err(ZippyError::Io {
        reason: DRIVER_UNIMPLEMENTED_REASON.to_string(),
    })
}

fn run_live_driver_loop(
    config: &OpenCtpMarketDataSourceConfig,
    api_holder: &Arc<std::sync::Mutex<Option<MdApi>>>,
    stopping: &Arc<AtomicBool>,
    stop_rx: &crossbeam_channel::Receiver<()>,
    control_rx: &crossbeam_channel::Receiver<LiveMdControlEvent>,
    tx: &Sender<MdDriverEvent>,
) -> CoreResult<()> {
    loop {
        select! {
            recv(stop_rx) -> _ => {
                stopping.store(true, Ordering::SeqCst);
                if let Some(api) = api_holder.lock().unwrap().as_ref() {
                    api.release();
                }
                return Ok(());
            }
            recv(control_rx) -> event => {
                match event {
                    Ok(LiveMdControlEvent::FrontConnected) => {
                        let mut login = CThostFtdcReqUserLoginField::default();
                        login.BrokerID.set_str(&config.broker_id);
                        login.UserID.set_str(&config.user_id);
                        login.Password.set_str(&config.password);

                        let request_result = {
                            let guard = api_holder.lock().unwrap();
                            let api = guard.as_ref().ok_or_else(|| ZippyError::Io {
                                reason: "ctp md api not available after front connected".to_string(),
                            })?;
                            api.req_user_login(&mut login, LOGIN_REQUEST_ID)
                        };

                        if request_result != 0 {
                            let reason = format!("ctp md req_user_login failed code=[{request_result}]");
                            tx.send(MdDriverEvent::Error(reason.clone()))
                                .map_err(|_| ZippyError::ChannelSend)?;
                            return Err(ZippyError::Io { reason });
                        }
                    }
                    Ok(LiveMdControlEvent::UserLoginSucceeded) => {
                        let subscribe_result = {
                            let guard = api_holder.lock().unwrap();
                            let api = guard.as_ref().ok_or_else(|| ZippyError::Io {
                                reason: "ctp md api not available after login".to_string(),
                            })?;
                            api.subscribe_market_data(config.instruments.as_slice())
                        };

                        if subscribe_result != 0 {
                            let reason = format!("ctp md subscribe_market_data failed code=[{subscribe_result}]");
                            tx.send(MdDriverEvent::Error(reason.clone()))
                                .map_err(|_| ZippyError::ChannelSend)?;
                            return Err(ZippyError::Io { reason });
                        }
                    }
                    Ok(LiveMdControlEvent::DriverError(reason)) => {
                        if stopping.load(Ordering::SeqCst) {
                            return Ok(());
                        }

                        tx.send(MdDriverEvent::Error(reason.clone()))
                            .map_err(|_| ZippyError::ChannelSend)?;
                        return Err(ZippyError::Io { reason });
                    }
                    Err(_) => {
                        return Err(ZippyError::ChannelReceive);
                    }
                }
            }
        }
    }
}

fn raw_tick_from_depth_market_data(
    depth_market_data: &CThostFtdcDepthMarketDataField,
) -> CoreResult<RawTickSnapshot> {
    Ok(RawTickSnapshot {
        instrument_id: decode_ctp_text(&depth_market_data.InstrumentID)?,
        exchange_id: decode_ctp_text(&depth_market_data.ExchangeID)?,
        trading_day: decode_ctp_text(&depth_market_data.TradingDay)?,
        action_day: decode_ctp_text(&depth_market_data.ActionDay)?,
        update_time: decode_ctp_text(&depth_market_data.UpdateTime)?,
        update_millisec: depth_market_data.UpdateMillisec,
        last_price: depth_market_data.LastPrice,
        volume: i64::from(depth_market_data.Volume),
        turnover: depth_market_data.Turnover,
        open_interest: depth_market_data.OpenInterest,
        bid_price_1: depth_market_data.BidPrice1,
        bid_volume_1: i64::from(depth_market_data.BidVolume1),
        ask_price_1: depth_market_data.AskPrice1,
        ask_volume_1: i64::from(depth_market_data.AskVolume1),
    })
}

fn decode_ctp_text<const N: usize>(field: &[i8; N]) -> CoreResult<String> {
    gb18030_cstr_i8_to_str(field)
        .map(|value| value.into_owned())
        .map_err(|reason| ZippyError::Io {
            reason: format!("failed to decode ctp text field: {reason}"),
        })
}

fn rsp_info_error_reason(rsp_info: Option<&CThostFtdcRspInfoField>) -> Option<String> {
    let rsp_info = rsp_info?;
    if rsp_info.ErrorID == 0 {
        return None;
    }

    let detail = gb18030_cstr_i8_to_str(&rsp_info.ErrorMsg)
        .map(|value| value.into_owned())
        .unwrap_or_else(|_| "failed to decode rsp error message".to_string());

    Some(format!(
        "ctp md rsp error error_id=[{}] message=[{}]",
        rsp_info.ErrorID, detail
    ))
}
