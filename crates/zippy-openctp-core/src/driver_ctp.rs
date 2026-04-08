use std::collections::HashSet;
use std::env;
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
use zippy_core::{Result as CoreResult, ZippyError};

use crate::normalize::RawTickSnapshot;
use crate::source::{
    evaluate_subscription_results, MdDriver, MdDriverEvent, MdDriverHandle,
    OpenCtpMarketDataSourceConfig, ReconnectState, SubscriptionOutcome,
};

const DRIVER_UNIMPLEMENTED_REASON: &str = "ctp2rs live driver wiring is not implemented yet";
const LOGIN_REQUEST_ID: i32 = 1;
const RECONNECT_INTERVAL_SECS: u64 = 3;

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
                tx.send(MdDriverEvent::Stop)
                    .map_err(|_| ZippyError::ChannelSend)?;
            }

            loop_result
        });

        let stop_fn = {
            let stop_tx = stop_tx.clone();
            let stop_stopping = stopping.clone();

            Box::new(move || -> CoreResult<()> {
                stop_stopping.store(true, Ordering::SeqCst);
                stop_tx.send(()).map_err(|_| ZippyError::ChannelSend)?;

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
        let _ = self.control_tx.send(LiveMdControlEvent::FrontConnected);
    }

    fn on_front_disconnected(&mut self, reason: i32) {
        if self.stopping.load(Ordering::SeqCst) {
            return;
        }

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
            let _ = self.control_tx.send(LiveMdControlEvent::DriverError(error));
            return;
        }

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
                let _ = self
                    .control_tx
                    .send(LiveMdControlEvent::DriverError(error.to_string()));
                return;
            }
        };

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

    if let Some(api) = api {
        api.release();
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
    let mut pending_subscription = PendingSubscription::new(config.instruments.as_slice());
    let mut reconnect_state =
        ReconnectState::new(std::time::Duration::from_secs(RECONNECT_INTERVAL_SECS));

    loop {
        select! {
            recv(stop_rx) -> _ => {
                stopping.store(true, Ordering::SeqCst);
                release_api(api_holder);
                return Ok(());
            }
            recv(control_rx) -> event => {
                match event {
                    Ok(LiveMdControlEvent::FrontConnected) => {
                        if let Some(reconnect_gate) =
                            reconnect_gate_for_front_connected(&reconnect_state, Instant::now())
                        {
                            if !reconnect_gate.sleep_for.is_zero() {
                                let wakeup = after(reconnect_gate.sleep_for);
                                select! {
                                    recv(stop_rx) -> _ => {
                                        stopping.store(true, Ordering::SeqCst);
                                        release_api(api_holder);
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

                        if request_result != 0 {
                            let reason = format!("ctp md req_user_login failed code=[{request_result}]");
                            tx.send(MdDriverEvent::Error(reason.clone()))
                                .map_err(|_| ZippyError::ChannelSend)?;
                            return Err(ZippyError::Io { reason });
                        }
                    }
                    Ok(LiveMdControlEvent::UserLoginSucceeded) => {
                        pending_subscription.reset();
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
                    Ok(LiveMdControlEvent::FrontDisconnected(_reason)) => {
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
                        pending_subscription.observe(request_id, instrument_id, succeeded);
                        if is_last {
                            if let Some(outcome) = pending_subscription.finish_and_reset(request_id) {
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

impl ReconnectGate {
}

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

        let reconnect_gate = reconnect_gate_for_front_connected(
            &reconnect_state,
            start + Duration::from_secs(1),
        )
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
