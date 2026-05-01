use std::collections::HashSet;
use std::env;
use std::ffi::CString;
use std::fs;
use std::os::raw::c_char;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::Instant;

use crossbeam_channel::{after, select, unbounded, Sender};
use ctp2rs::ffi::{gb18030_cstr_i8_to_str, resolve_dynlib_path, DynLibKind, SetString};
use ctp2rs::v1alpha1::{
    CThostFtdcDepthMarketDataField, CThostFtdcReqUserLoginField, CThostFtdcRspInfoField, MdApi,
    MdApiBuilder, MdSpi,
};
use tracing::{debug, error, info, warn};
use zippy_core::{Result as CoreResult, ZippyError};

use crate::normalize::RawTickSnapshot;
use crate::source::{
    evaluate_subscription_results, MdDriver, MdDriverEvent, MdDriverHandle,
    OpenCtpMarketDataSourceConfig, ReconnectState, SubscriptionOutcome,
};

const DRIVER_DYNLIB_NOT_CONFIGURED_REASON: &str =
    "openctp md dynlib path is not configured set one of [OPENCTP_MD_DYNLIB_PATH, OPENCTP_MD_DYNLIB_DIR]";
const LOGIN_REQUEST_ID: i32 = 1;
const RECONNECT_INTERVAL_SECS: u64 = 3;

fn openctp_debug_enabled() -> bool {
    match env::var("OPENCTP_DEBUG") {
        Ok(value) => {
            let normalized = value.trim().to_ascii_lowercase();
            matches!(normalized.as_str(), "1" | "true" | "yes" | "on")
        }
        Err(_) => false,
    }
}

fn openctp_debug_log(message: &str) {
    if openctp_debug_enabled() {
        debug!(
            component = "openctp_source",
            event = "debug",
            message = message,
            "{message}"
        );
    }
}

struct SubscriptionRequestBuffer {
    _cstrings: Vec<CString>,
    ptrs: Vec<*mut c_char>,
}

impl SubscriptionRequestBuffer {
    fn new(instruments: &[String]) -> CoreResult<Self> {
        let cstrings = instruments
            .iter()
            .map(|instrument| {
                CString::new(instrument.as_str()).map_err(|error| ZippyError::Io {
                    reason: format!(
                        "failed to encode instrument for subscribe instrument=[{instrument}] error=[{error}]"
                    ),
                })
            })
            .collect::<CoreResult<Vec<_>>>()?;
        let ptrs = cstrings
            .iter()
            .map(|instrument| instrument.as_ptr() as *mut c_char)
            .collect::<Vec<_>>();
        Ok(Self {
            _cstrings: cstrings,
            ptrs,
        })
    }

    unsafe fn subscribe_market_data(&self, api: &MdApi) -> i32 {
        ((*(*api.api_ptr).vtable_).CThostFtdcMdApi_SubscribeMarketData)(
            api.api_ptr,
            self.ptrs.as_ptr() as *mut *mut c_char,
            self.ptrs.len() as i32,
        )
    }
}

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
        let join_front = config.front.clone();
        let join_instruments = config.instruments.clone();

        let join_handle = thread::spawn(move || -> CoreResult<()> {
            fs::create_dir_all(&config.flow_path).map_err(|error| ZippyError::Io {
                reason: format!(
                    "failed to create md flow path path=[{}] error=[{}]",
                    config.flow_path, error
                ),
            })?;

            let api = MdApiBuilder::new()
                .with_dynlib(&dynlib_path)
                .flow_path(&config.flow_path)
                .using_udp(false)
                .multicast(false)
                .build()
                .map_err(|reason| ZippyError::Io {
                    reason: format!("failed to build ctp md api: {reason}"),
                })?;
            info!(
                component = "openctp_source",
                event = "connect_start",
                front = %join_front,
                instrument_count = join_instruments.len(),
                message = "starting market data connection"
            );

            let (control_tx, control_rx) = unbounded();
            let spi = Box::new(LiveMdSpi::new(
                control_tx,
                tx.clone(),
                join_stopping.clone(),
            ));

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
                openctp_debug_log("driver thread emitting MdDriverEvent::Stop");
                info!(
                    component = "openctp_source",
                    event = "driver_stop",
                    message = "market data driver emitted stop event"
                );
                tx.send(MdDriverEvent::Stop)
                    .map_err(|_| ZippyError::ChannelSend)?;
            }

            loop_result
        });

        let stop_fn = {
            let stop_tx = stop_tx.clone();
            let stop_stopping = stopping.clone();

            Box::new(move || -> CoreResult<()> {
                openctp_debug_log("driver stop_fn invoked");
                stop_stopping.store(true, Ordering::SeqCst);
                stop_tx.send(()).map_err(|_| ZippyError::ChannelSend)?;
                openctp_debug_log("driver stop_fn sent stop signal");
                info!(
                    component = "openctp_source",
                    event = "driver_stop_request",
                    message = "market data driver stop requested"
                );

                Ok(())
            })
        };

        Ok(MdDriverHandle::new_with_stop(join_handle, stop_fn))
    }
}

#[derive(Debug)]
enum LiveMdControlEvent {
    FrontConnected,
    FrontDisconnected(i32),
    UserLoginSucceeded,
    SubscriptionResponse {
        request_id: i32,
        instrument_id: Option<String>,
        succeeded: bool,
        is_last: bool,
    },
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
        openctp_debug_log("spi on_front_connected");
        info!(
            component = "openctp_source",
            event = "front_connected",
            message = "market data front connected"
        );
        let _ = self.control_tx.send(LiveMdControlEvent::FrontConnected);
    }

    fn on_front_disconnected(&mut self, reason: i32) {
        if self.stopping.load(Ordering::SeqCst) {
            return;
        }

        openctp_debug_log(&format!("spi on_front_disconnected reason=[{reason}]"));
        warn!(
            component = "openctp_source",
            event = "front_disconnected",
            reason = reason,
            message = "market data front disconnected"
        );
        let _ = self
            .control_tx
            .send(LiveMdControlEvent::FrontDisconnected(reason));
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
            openctp_debug_log(&format!("spi on_rsp_user_login error=[{error}]"));
            error!(
                component = "openctp_source",
                event = "login_failure",
                error = %error,
                message = "market data login failed"
            );
            let _ = self.control_tx.send(LiveMdControlEvent::DriverError(error));
            return;
        }

        openctp_debug_log("spi on_rsp_user_login success");
        info!(
            component = "openctp_source",
            event = "login_success",
            message = "market data login succeeded"
        );
        let _ = self.control_tx.send(LiveMdControlEvent::UserLoginSucceeded);
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
        openctp_debug_log(&format!("spi on_rsp_error reason=[{reason}]"));
        error!(
            component = "openctp_source",
            event = "rsp_error",
            error = %reason,
            message = "market data response error received"
        );
        let _ = self
            .control_tx
            .send(LiveMdControlEvent::DriverError(reason));
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
                openctp_debug_log(&format!(
                    "spi on_rtn_depth_market_data decode_error=[{}]",
                    error
                ));
                error!(
                    component = "openctp_source",
                    event = "tick_decode_error",
                    error = %error,
                    message = "depth market data decode failed"
                );
                let _ = self
                    .control_tx
                    .send(LiveMdControlEvent::DriverError(error.to_string()));
                return;
            }
        };

        openctp_debug_log(&format!(
            "spi on_rtn_depth_market_data instrument_id=[{}] update_time=[{}]",
            raw.instrument_id, raw.update_time
        ));
        let _ = self.data_tx.send(MdDriverEvent::Tick(raw));
    }

    fn on_rsp_sub_market_data(
        &mut self,
        specific_instrument: Option<&ctp2rs::v1alpha1::CThostFtdcSpecificInstrumentField>,
        rsp_info: Option<&CThostFtdcRspInfoField>,
        request_id: i32,
        is_last: bool,
    ) {
        let instrument_id = specific_instrument
            .and_then(|instrument| decode_ctp_text(&instrument.InstrumentID).ok());
        let succeeded = rsp_info_error_reason(rsp_info).is_none();
        openctp_debug_log(&format!(
            "spi on_rsp_sub_market_data request_id=[{request_id}] instrument_id=[{}] succeeded=[{succeeded}] is_last=[{is_last}]",
            instrument_id.as_deref().unwrap_or("<none>")
        ));
        if !succeeded {
            warn!(
                component = "openctp_source",
                event = "subscribe_response_failure",
                request_id = request_id,
                instrument = %instrument_id.as_deref().unwrap_or("<none>"),
                is_last = is_last,
                message = "market data subscribe response reported failure"
            );
        }

        let _ = self
            .control_tx
            .send(LiveMdControlEvent::SubscriptionResponse {
                request_id,
                instrument_id,
                succeeded,
                is_last,
            });
    }
}

fn release_api(api_holder: &Arc<std::sync::Mutex<Option<MdApi>>>) {
    let api = {
        let mut guard = api_holder.lock().unwrap();
        guard.take()
    };

    // ctp2rs 0.1.9 已经把 RegisterSpi(null) + Release + SPI 释放封装进 MdApi::drop。
    // 这里如果显式再调一次 release()，随后 drop(api) 会重复走释放路径，
    // live stop 时会把底层指针打坏。
    drop(api);
}

fn resolve_live_md_dynlib_path() -> CoreResult<PathBuf> {
    if let Some(path) = env::var_os("OPENCTP_MD_DYNLIB_PATH") {
        return Ok(PathBuf::from(path));
    }

    if let Some(dir) = env::var_os("OPENCTP_MD_DYNLIB_DIR") {
        return Ok(resolve_dynlib_path(PathBuf::from(dir), DynLibKind::MdApi));
    }

    Err(ZippyError::Io {
        reason: DRIVER_DYNLIB_NOT_CONFIGURED_REASON.to_string(),
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
    openctp_debug_log(&format!(
        "driver loop started front=[{}] instruments=[{}]",
        config.front,
        config.instruments.join(",")
    ));
    info!(
        component = "openctp_source",
        event = "driver_loop_start",
        front = %config.front,
        instrument_count = config.instruments.len(),
        message = "market data driver loop started"
    );
    let mut pending_subscription = PendingSubscription::new(config.instruments.as_slice());
    let mut _active_subscription_buffer: Option<SubscriptionRequestBuffer> = None;
    let mut reconnect_state =
        ReconnectState::new(std::time::Duration::from_secs(RECONNECT_INTERVAL_SECS));

    loop {
        select! {
            recv(stop_rx) -> _ => {
                openctp_debug_log("driver loop received stop signal");
                stopping.store(true, Ordering::SeqCst);
                release_api(api_holder);
                openctp_debug_log("driver loop released api on stop");
                info!(
                    component = "openctp_source",
                    event = "driver_loop_stop",
                    message = "market data driver loop stopped"
                );
                return Ok(());
            }
            recv(control_rx) -> event => {
                match event {
                    Ok(LiveMdControlEvent::FrontConnected) => {
                        openctp_debug_log("driver event FrontConnected");
                        if let Some(reconnect_gate) =
                            reconnect_gate_for_front_connected(&reconnect_state, Instant::now())
                        {
                            if !reconnect_gate.sleep_for.is_zero() {
                                let wakeup = after(reconnect_gate.sleep_for);
                                select! {
                                    recv(stop_rx) -> _ => {
                                        openctp_debug_log("driver reconnect gate received stop signal");
                                        stopping.store(true, Ordering::SeqCst);
                                        release_api(api_holder);
                                        openctp_debug_log("driver reconnect gate released api on stop");
                                        return Ok(());
                                    }
                                    recv(wakeup) -> _ => {}
                                }
                            }
                        }

                        if stopping.load(Ordering::SeqCst) {
                            release_api(api_holder);
                            return Ok(());
                        }

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
                        openctp_debug_log(&format!(
                            "driver req_user_login broker_id=[{}] user_id=[{}] result=[{request_result}]",
                            config.broker_id, config.user_id
                        ));
                        info!(
                            component = "openctp_source",
                            event = "login_request",
                            broker_id = %config.broker_id,
                            result = request_result,
                            message = "market data login request sent"
                        );

                        if request_result != 0 {
                            let reason = format!("ctp md req_user_login failed code=[{request_result}]");
                            error!(
                                component = "openctp_source",
                                event = "login_request_failure",
                                error = %reason,
                                message = "market data login request failed"
                            );
                            tx.send(MdDriverEvent::Error(reason.clone()))
                                .map_err(|_| ZippyError::ChannelSend)?;
                            return Err(ZippyError::Io { reason });
                        }
                    }
                    Ok(LiveMdControlEvent::UserLoginSucceeded) => {
                        openctp_debug_log("driver event UserLoginSucceeded");
                        pending_subscription.reset();
                        let request_buffer =
                            SubscriptionRequestBuffer::new(config.instruments.as_slice())?;
                        let subscribe_result = {
                            let guard = api_holder.lock().unwrap();
                            let api = guard.as_ref().ok_or_else(|| ZippyError::Io {
                                reason: "ctp md api not available after login".to_string(),
                            })?;
                            unsafe { request_buffer.subscribe_market_data(api) }
                        };
                        _active_subscription_buffer = Some(request_buffer);
                        openctp_debug_log(&format!(
                            "driver subscribe_market_data instruments=[{}] result=[{subscribe_result}]",
                            config.instruments.join(",")
                        ));
                        info!(
                            component = "openctp_source",
                            event = "subscribe_request",
                            instrument_count = config.instruments.len(),
                            result = subscribe_result,
                            message = "market data subscribe request sent"
                        );

                        if subscribe_result != 0 {
                            let reason =
                                format!("ctp md subscribe_market_data failed code=[{subscribe_result}]");
                            error!(
                                component = "openctp_source",
                                event = "subscribe_request_failure",
                                error = %reason,
                                message = "market data subscribe request failed"
                            );
                            tx.send(MdDriverEvent::Error(reason.clone()))
                                .map_err(|_| ZippyError::ChannelSend)?;
                            return Err(ZippyError::Io { reason });
                        }
                    }
                    Ok(LiveMdControlEvent::FrontDisconnected(_reason)) => {
                        openctp_debug_log("driver event FrontDisconnected");
                        warn!(
                            component = "openctp_source",
                            event = "reconnect",
                            status = crate::source::OpenCtpSourceStatus::Degraded.as_str(),
                            message = "market data driver entered reconnect state"
                        );
                        reconnect_state.mark_disconnected_at(Instant::now());
                        tx.send(MdDriverEvent::ReconnectUpdate(reconnect_state.snapshot()))
                            .map_err(|_| ZippyError::ChannelSend)?;
                    }
                    Ok(LiveMdControlEvent::SubscriptionResponse {
                        request_id,
                        instrument_id,
                        succeeded,
                        is_last,
                    }) => {
                        openctp_debug_log(&format!(
                            "driver event SubscriptionResponse request_id=[{request_id}] instrument_id=[{}] succeeded=[{succeeded}] is_last=[{is_last}]",
                            instrument_id.as_deref().unwrap_or("<none>")
                        ));
                        pending_subscription.observe(request_id, instrument_id, succeeded);
                        if is_last {
                            if let Some(outcome) = pending_subscription.finish_and_reset(request_id) {
                                _active_subscription_buffer = None;
                                if reconnect_state.status() == crate::source::OpenCtpSourceStatus::Degraded {
                                    reconnect_state.mark_reconnected();
                                    tx.send(MdDriverEvent::ReconnectUpdate(crate::source::ReconnectUpdate {
                                        reconnects_total: reconnect_state.reconnects_total(),
                                        status: outcome.status,
                                    }))
                                        .map_err(|_| ZippyError::ChannelSend)?;
                                }
                                tx.send(MdDriverEvent::SubscriptionOutcome(outcome))
                                    .map_err(|_| ZippyError::ChannelSend)?;
                            }
                        }
                    }
                    Ok(LiveMdControlEvent::DriverError(reason)) => {
                        openctp_debug_log(&format!("driver event DriverError reason=[{reason}]"));
                        error!(
                            component = "openctp_source",
                            event = "driver_error",
                            error = %reason,
                            message = "market data driver reported error"
                        );
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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct ReconnectGate {
    sleep_for: std::time::Duration,
}

impl ReconnectGate {}

fn reconnect_gate_for_front_connected(
    reconnect_state: &ReconnectState,
    now: Instant,
) -> Option<ReconnectGate> {
    if reconnect_state.status() != crate::source::OpenCtpSourceStatus::Degraded {
        return None;
    }

    Some(ReconnectGate {
        sleep_for: reconnect_state.remaining_until_reconnect_at(now),
    })
}

struct PendingSubscription {
    requested: Vec<String>,
    succeeded: Vec<String>,
    active_request_id: Option<i32>,
    sealed_request_ids: HashSet<i32>,
}

impl PendingSubscription {
    fn new(requested: &[String]) -> Self {
        Self {
            requested: requested.to_vec(),
            succeeded: Vec::new(),
            active_request_id: None,
            sealed_request_ids: HashSet::new(),
        }
    }

    fn observe(&mut self, request_id: i32, instrument_id: Option<String>, succeeded: bool) {
        if !self.try_accept_request(request_id) {
            return;
        }

        if !succeeded {
            return;
        }

        let Some(instrument_id) = instrument_id else {
            return;
        };

        if self.succeeded.iter().any(|item| item == &instrument_id) {
            return;
        }

        self.succeeded.push(instrument_id);
    }

    fn finish_and_reset(&mut self, request_id: i32) -> Option<SubscriptionOutcome> {
        if !self.try_accept_request(request_id) {
            return None;
        }

        let outcome =
            evaluate_subscription_results(self.requested.as_slice(), self.succeeded.as_slice());
        self.reset_active_round(request_id);
        Some(outcome)
    }

    fn reset(&mut self) {
        if let Some(active_request_id) = self.active_request_id {
            self.sealed_request_ids.insert(active_request_id);
        }
        self.active_request_id = None;
        self.succeeded.clear();
    }

    fn try_accept_request(&mut self, request_id: i32) -> bool {
        if self.sealed_request_ids.contains(&request_id) {
            return false;
        }

        match self.active_request_id {
            Some(active_request_id) => active_request_id == request_id,
            None => {
                self.active_request_id = Some(request_id);
                true
            }
        }
    }

    fn reset_active_round(&mut self, request_id: i32) {
        self.sealed_request_ids.insert(request_id);
        self.active_request_id = None;
        self.succeeded.clear();
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

#[cfg(test)]
mod tests {
    use super::{reconnect_gate_for_front_connected, PendingSubscription};
    use std::time::{Duration, Instant};

    use crate::source::{ReconnectState, SubscriptionOutcome};

    #[test]
    fn pending_subscription_ignores_delayed_completed_request_id_after_reset() {
        let mut pending = PendingSubscription::new(&["IF2506".to_string(), "IH2506".to_string()]);

        pending.observe(101, Some("IF2506".to_string()), true);
        let first = pending
            .finish_and_reset(101)
            .expect("active request should produce subscription outcome");
        assert_eq!(
            first,
            SubscriptionOutcome {
                succeeded_instruments: vec!["IF2506".to_string()],
                failed_instruments: vec!["IH2506".to_string()],
                subscribe_failures_total: 1,
                status: crate::source::OpenCtpSourceStatus::Degraded,
            }
        );

        pending.observe(202, Some("IH2506".to_string()), true);
        pending.observe(101, Some("IF2506".to_string()), true);
        assert!(
            pending.finish_and_reset(101).is_none(),
            "completed old request_id must not emit a new outcome"
        );

        let second = pending
            .finish_and_reset(202)
            .expect("new request should remain isolated from delayed old responses");
        assert_eq!(
            second,
            SubscriptionOutcome {
                succeeded_instruments: vec!["IH2506".to_string()],
                failed_instruments: vec!["IF2506".to_string()],
                subscribe_failures_total: 1,
                status: crate::source::OpenCtpSourceStatus::Degraded,
            }
        );
    }

    #[test]
    fn reconnect_gate_waits_until_interval_before_recovering() {
        let start = Instant::now();
        let mut reconnect_state = ReconnectState::new(Duration::from_secs(3));
        reconnect_state.mark_disconnected_at(start);

        let reconnect_gate =
            reconnect_gate_for_front_connected(&reconnect_state, start + Duration::from_secs(1))
                .expect("degraded source should produce reconnect gate");

        assert_eq!(reconnect_gate.sleep_for, Duration::from_secs(2));
        assert_eq!(reconnect_state.reconnects_total(), 0);
        assert_eq!(
            reconnect_state.status(),
            crate::source::OpenCtpSourceStatus::Degraded
        );

        reconnect_state.mark_reconnected();
        assert_eq!(reconnect_state.reconnects_total(), 1);
        assert_eq!(
            reconnect_state.status(),
            crate::source::OpenCtpSourceStatus::Running
        );
    }
}
