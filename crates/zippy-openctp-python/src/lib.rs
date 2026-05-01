#![allow(clippy::useless_conversion)]

mod native_sink;

use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Duration;

use arrow::pyarrow::ToPyArrow;
use pyo3::exceptions::{PyRuntimeError, PyValueError};
use pyo3::prelude::*;
use pyo3::types::{PyAny, PyBytes, PyDict, PyModule, PyTuple};
use zippy_core::{Source, SourceEvent, SourceHandle, SourceSink, StreamHello, ZippyError};
use zippy_openctp_core::schema::{TickSchemaType, TICK_SCHEMA_FIELDS};
use zippy_openctp_core::{
    normalize_tick, openctp_segment_schema, OpenCtpMarketDataSource as CoreOpenCtpMarketDataSource,
    OpenCtpMarketDataSourceConfig, OpenCtpMarketGeneratorConfig,
    OpenCtpMarketGeneratorSource as CoreOpenCtpMarketGeneratorSource,
    OpenCtpSegmentDescriptorPublisher, OpenCtpSegmentIngress, OpenCtpSourceMetrics,
    OpenCtpSourceStatus, RawTickSnapshot,
};
use zippy_segment_store::{ActiveSegmentReader, LayoutPlan};

use crate::native_sink::NativeCapsuleSink;

#[pyfunction]
fn tick_data_schema_fields(py: Python<'_>) -> PyResult<PyObject> {
    let fields: Vec<PyObject> = TICK_SCHEMA_FIELDS
        .iter()
        .map(|field| {
            let type_name = match field.data_type {
                TickSchemaType::Utf8 => "utf8",
                TickSchemaType::TimestampNsShanghai => "timestamp_ns_shanghai",
                TickSchemaType::Float64 => "float64",
                TickSchemaType::Int64 => "int64",
            };
            PyTuple::new_bound(
                py,
                [
                    field.name.into_py(py),
                    type_name.into_py(py),
                    field.nullable.into_py(py),
                ],
            )
            .into_py(py)
        })
        .collect::<Vec<_>>();
    Ok(fields.into_py(py))
}

#[pyclass]
struct OpenCtpMarketDataSource {
    config: OpenCtpMarketDataSourceConfig,
    source: Option<CoreOpenCtpMarketDataSource>,
    metrics: Arc<Mutex<OpenCtpSourceMetrics>>,
    status: Arc<Mutex<OpenCtpSourceStatus>>,
}

#[pyclass]
struct OpenCtpMarketGeneratorSource {
    config: OpenCtpMarketGeneratorConfig,
    source: Option<CoreOpenCtpMarketGeneratorSource>,
    metrics: Arc<Mutex<OpenCtpSourceMetrics>>,
    status: Arc<Mutex<OpenCtpSourceStatus>>,
}

#[pyclass]
struct OpenCtpRuntimeHandle {
    handle: SourceHandle,
}

#[pyclass(unsendable)]
struct OpenCtpSegmentReader {
    reader: ActiveSegmentReader,
}

#[pyclass(name = "OpenCtpSegmentTestWriter", unsendable)]
struct PyOpenCtpSegmentTestWriter {
    ingress: OpenCtpSegmentIngress,
}

#[pymethods]
impl OpenCtpRuntimeHandle {
    fn stop(&self, py: Python<'_>) -> PyResult<()> {
        py.allow_threads(|| self.handle.stop())
            .map_err(|error| PyRuntimeError::new_err(error.to_string()))
    }

    fn join(&self, py: Python<'_>) -> PyResult<()> {
        py.allow_threads(|| self.handle.join())
            .map_err(|error| PyRuntimeError::new_err(error.to_string()))
    }
}

#[pymethods]
impl OpenCtpSegmentReader {
    #[new]
    fn new(py: Python<'_>, descriptor: &Bound<'_, PyAny>) -> PyResult<Self> {
        let (descriptor_envelope, row_capacity) = descriptor_envelope_from_py(py, descriptor)?;
        let schema = openctp_segment_schema().map_err(py_runtime_error)?;
        let layout = LayoutPlan::for_schema(&schema, row_capacity).map_err(py_value_error)?;
        let reader =
            ActiveSegmentReader::from_descriptor_envelope(&descriptor_envelope, schema, layout)
                .map_err(|error| py_runtime_error(error.to_string()))?;
        Ok(Self { reader })
    }

    fn update_descriptor(&mut self, py: Python<'_>, descriptor: &Bound<'_, PyAny>) -> PyResult<()> {
        let (descriptor_envelope, row_capacity) = descriptor_envelope_from_py(py, descriptor)?;
        let schema = openctp_segment_schema().map_err(py_runtime_error)?;
        let layout = LayoutPlan::for_schema(&schema, row_capacity).map_err(py_value_error)?;
        self.reader
            .update_descriptor_envelope(&descriptor_envelope, schema, layout)
            .map_err(|error| py_runtime_error(error.to_string()))
    }

    fn committed_row_count(&self) -> PyResult<usize> {
        self.reader
            .committed_row_count()
            .map_err(|error| py_runtime_error(error.to_string()))
    }

    fn read_available(&mut self, py: Python<'_>) -> PyResult<PyObject> {
        let Some(span) = self
            .reader
            .read_available()
            .map_err(|error| py_runtime_error(error.to_string()))?
        else {
            return Ok(py.None());
        };
        let batch = span
            .as_record_batch()
            .map_err(|error| py_runtime_error(error.to_string()))?;
        batch
            .to_pyarrow(py)
            .map_err(|error| py_runtime_error(error.to_string()))
    }
}

#[pymethods]
impl PyOpenCtpSegmentTestWriter {
    #[new]
    fn new() -> PyResult<Self> {
        let ingress = OpenCtpSegmentIngress::for_test().map_err(py_runtime_error)?;
        Ok(Self { ingress })
    }

    fn append_tick(&mut self, instrument_id: &str, last_price: f64) -> PyResult<()> {
        let tick = RawTickSnapshot::for_test(instrument_id, last_price);
        let row = normalize_tick(&tick).map_err(|error| py_runtime_error(error.to_string()))?;
        self.ingress.write_row(&row).map_err(py_runtime_error)
    }

    fn descriptor(&self, py: Python<'_>) -> PyResult<PyObject> {
        let descriptor = self
            .ingress
            .active_descriptor_envelope_bytes()
            .map_err(py_runtime_error)?;
        let descriptor = std::str::from_utf8(&descriptor)
            .map_err(|error| py_runtime_error(error.to_string()))?;
        Ok(python_json_loads(py, descriptor)?.into_py(py))
    }
}

struct PyCallbackSink {
    sink: Py<PyAny>,
}

struct PySegmentDescriptorPublisher {
    callback: Py<PyAny>,
}

impl SourceSink for PyCallbackSink {
    fn emit(&self, event: SourceEvent) -> zippy_core::Result<()> {
        Python::with_gil(|py| -> PyResult<()> {
            let sink = self.sink.bind(py);
            match event {
                SourceEvent::Hello(StreamHello {
                    protocol_version,
                    stream_name,
                    ..
                }) => {
                    sink.call_method1("emit_hello", (stream_name, protocol_version))?;
                }
                SourceEvent::Data(batch) => {
                    let batch = batch
                        .to_record_batch()
                        .map_err(|error| PyRuntimeError::new_err(error.to_string()))?;
                    let py_batch = batch
                        .to_pyarrow(py)
                        .map_err(|error| PyRuntimeError::new_err(error.to_string()))?;
                    sink.call_method1("emit_data", (py_batch,))?;
                }
                SourceEvent::Flush => {
                    sink.call_method0("emit_flush")?;
                }
                SourceEvent::Stop => {
                    sink.call_method0("emit_stop")?;
                }
                SourceEvent::Error(reason) => {
                    sink.call_method1("emit_error", (reason,))?;
                }
            }
            Ok(())
        })
        .map_err(map_py_bridge_error)
    }
}

impl OpenCtpSegmentDescriptorPublisher for PySegmentDescriptorPublisher {
    fn publish(&self, descriptor_envelope: Vec<u8>) -> zippy_core::Result<()> {
        Python::with_gil(|py| -> PyResult<()> {
            let callback = self.callback.bind(py);
            let payload = PyBytes::new_bound(py, &descriptor_envelope);
            callback.call1((payload,))?;
            Ok(())
        })
        .map_err(map_py_bridge_error)
    }
}

fn map_py_bridge_error(error: PyErr) -> ZippyError {
    ZippyError::Io {
        reason: format!("python source bridge failed error=[{}]", error),
    }
}

fn py_runtime_error(message: impl Into<String>) -> PyErr {
    PyRuntimeError::new_err(message.into())
}

fn py_value_error(message: impl Into<String>) -> PyErr {
    PyValueError::new_err(message.into())
}

fn python_json_dumps(py: Python<'_>, value: &Bound<'_, PyAny>) -> PyResult<String> {
    let json = PyModule::import_bound(py, "json")?;
    json.call_method1("dumps", (value,))?
        .extract::<String>()
        .map_err(|error| py_value_error(error.to_string()))
}

fn python_json_loads<'py>(py: Python<'py>, text: &str) -> PyResult<Bound<'py, PyAny>> {
    let json = PyModule::import_bound(py, "json")?;
    json.call_method1("loads", (text,))
        .map_err(|error| py_value_error(error.to_string()))
}

fn descriptor_envelope_from_py(
    py: Python<'_>,
    descriptor: &Bound<'_, PyAny>,
) -> PyResult<(Vec<u8>, usize)> {
    let descriptor_text = python_json_dumps(py, descriptor)?;
    let descriptor_value = serde_json::from_str::<serde_json::Value>(&descriptor_text)
        .map_err(|error| py_value_error(error.to_string()))?;
    let row_capacity = descriptor_value
        .get("row_capacity")
        .and_then(serde_json::Value::as_u64)
        .and_then(|value| usize::try_from(value).ok())
        .ok_or_else(|| py_value_error("segment descriptor is missing row_capacity"))?;
    Ok((descriptor_text.into_bytes(), row_capacity))
}

fn metrics_to_pydict(py: Python<'_>, metrics: &OpenCtpSourceMetrics) -> PyResult<PyObject> {
    let dict = PyDict::new_bound(py);
    dict.set_item("ticks_received_total", metrics.ticks_received_total)?;
    dict.set_item("ticks_emitted_total", metrics.ticks_emitted_total)?;
    dict.set_item("batches_emitted_total", metrics.batches_emitted_total)?;
    dict.set_item("reconnects_total", metrics.reconnects_total)?;
    dict.set_item("login_failures_total", metrics.login_failures_total)?;
    dict.set_item("subscribe_failures_total", metrics.subscribe_failures_total)?;
    Ok(dict.into_py(py))
}

fn blocking_join_handle() -> (OpenCtpRuntimeHandle, std::sync::mpsc::Sender<()>) {
    let (release_tx, release_rx) = std::sync::mpsc::channel();
    let join_handle = thread::spawn(move || {
        let _ = release_rx.recv();
        Ok(())
    });
    (
        OpenCtpRuntimeHandle {
            handle: SourceHandle::new(join_handle),
        },
        release_tx,
    )
}

fn blocking_stop_handle() -> (OpenCtpRuntimeHandle, std::sync::mpsc::Sender<()>) {
    let (release_tx, release_rx) = std::sync::mpsc::channel();
    let join_handle = thread::spawn(move || Ok(()));
    let stop_fn = Box::new(move || {
        let _ = release_rx.recv();
        Ok(())
    });
    (
        OpenCtpRuntimeHandle {
            handle: SourceHandle::new_with_stop(join_handle, stop_fn),
        },
        release_tx,
    )
}

#[pyfunction]
fn runtime_handle_releases_gil(py: Python<'_>, blocking_kind: &str) -> PyResult<bool> {
    let (runtime_handle, release_tx) = match blocking_kind {
        "join" => blocking_join_handle(),
        "stop" => blocking_stop_handle(),
        _ => {
            return Err(PyRuntimeError::new_err(
                "blocking_kind must be 'join' or 'stop'",
            ))
        }
    };

    let (entered_tx, entered_rx) = std::sync::mpsc::channel();
    let (probe_tx, probe_rx) = std::sync::mpsc::channel();

    let blocking_kind_owned = blocking_kind.to_string();
    let blocking_thread = thread::spawn(move || {
        Python::with_gil(|py| {
            entered_tx.send(()).unwrap();
            match blocking_kind_owned.as_str() {
                "join" => runtime_handle.join(py).unwrap(),
                "stop" => runtime_handle.stop(py).unwrap(),
                _ => unreachable!(),
            }
        });
    });

    let probe_thread = thread::spawn(move || {
        Python::with_gil(|_| {
            probe_tx.send(()).unwrap();
        });
    });

    let probe_result = py.allow_threads(move || {
        let started = entered_rx.recv_timeout(Duration::from_millis(200)).is_ok();
        if !started {
            let _ = release_tx.send(());
            let _ = blocking_thread.join();
            let _ = probe_thread.join();
            return Err(PyRuntimeError::new_err(
                "blocking runtime handle thread did not start",
            ));
        }

        let probe_result = probe_rx.recv_timeout(Duration::from_millis(200)).is_ok();
        let _ = release_tx.send(());
        let _ = blocking_thread.join();
        let _ = probe_thread.join();
        Ok(probe_result)
    })?;

    Ok(probe_result)
}

#[pymethods]
impl OpenCtpMarketDataSource {
    #[new]
    #[allow(clippy::too_many_arguments)]
    #[pyo3(signature = (
        front,
        broker_id,
        user_id,
        password,
        instruments,
        flow_path=".cache/openctp/md".to_string(),
        reconnect=true,
        login_timeout_sec=10,
        segment_descriptor_publisher=None,
    ))]
    fn new(
        py: Python<'_>,
        front: String,
        broker_id: String,
        user_id: String,
        password: String,
        instruments: Vec<String>,
        flow_path: String,
        reconnect: bool,
        login_timeout_sec: u64,
        segment_descriptor_publisher: Option<Py<PyAny>>,
    ) -> PyResult<Self> {
        let mut config = OpenCtpMarketDataSourceConfig::low_latency(
            front,
            broker_id,
            user_id,
            password,
            instruments,
            flow_path,
        );
        config.reconnect = reconnect;
        config.login_timeout_sec = login_timeout_sec;
        let mut source = CoreOpenCtpMarketDataSource::new(config.clone());
        if let Some(callback) = segment_descriptor_publisher {
            if !callback.bind(py).is_callable() {
                return Err(PyRuntimeError::new_err(
                    "segment_descriptor_publisher must be callable",
                ));
            }
            source =
                source.with_segment_descriptor_publisher(Arc::new(PySegmentDescriptorPublisher {
                    callback,
                }));
        }
        let metrics = source.metrics_handle();
        let status = source.status_handle();

        Ok(Self {
            source: Some(source),
            metrics,
            status,
            config,
        })
    }

    fn status(&self) -> &str {
        self.status.lock().unwrap().as_str()
    }

    fn config(&self, py: Python<'_>) -> PyResult<PyObject> {
        let dict = PyDict::new_bound(py);
        dict.set_item("front", &self.config.front)?;
        dict.set_item("broker_id", &self.config.broker_id)?;
        dict.set_item("user_id", &self.config.user_id)?;
        dict.set_item("password", "***redacted***")?;
        dict.set_item("instruments", &self.config.instruments)?;
        dict.set_item("flow_path", &self.config.flow_path)?;
        dict.set_item("reconnect", self.config.reconnect)?;
        dict.set_item("login_timeout_sec", self.config.login_timeout_sec)?;
        Ok(dict.into_py(py))
    }

    fn metrics(&self, py: Python<'_>) -> PyResult<PyObject> {
        let metrics = self.metrics.lock().unwrap().clone();
        metrics_to_pydict(py, &metrics)
    }

    fn _zippy_output_schema(&self, py: Python<'_>) -> PyResult<PyObject> {
        zippy_openctp_core::schema::tick_data_schema()
            .to_pyarrow(py)
            .map_err(|error| PyRuntimeError::new_err(error.to_string()))
    }

    fn _zippy_source_mode(&self) -> &str {
        "pipeline"
    }

    fn _zippy_source_name(&self) -> &str {
        "openctp-market-data-source"
    }

    fn _zippy_source_type(&self) -> &str {
        "openctp"
    }

    fn _zippy_start(
        &mut self,
        py: Python<'_>,
        sink: Py<PyAny>,
    ) -> PyResult<Py<OpenCtpRuntimeHandle>> {
        let source = self
            .source
            .take()
            .ok_or_else(|| PyRuntimeError::new_err("openctp source already started"))?;
        let handle = Box::new(source)
            .start(Arc::new(PyCallbackSink { sink }))
            .map_err(|error| PyRuntimeError::new_err(error.to_string()))?;
        Py::new(py, OpenCtpRuntimeHandle { handle })
    }

    fn _zippy_start_native(
        &mut self,
        py: Python<'_>,
        sink_capsule: &Bound<'_, PyAny>,
    ) -> PyResult<Py<OpenCtpRuntimeHandle>> {
        let source = self
            .source
            .take()
            .ok_or_else(|| PyRuntimeError::new_err("openctp source already started"))?;
        let sink = NativeCapsuleSink::from_capsule(sink_capsule)?;
        let handle = Box::new(source)
            .start(Arc::new(sink))
            .map_err(|error| PyRuntimeError::new_err(error.to_string()))?;
        Py::new(py, OpenCtpRuntimeHandle { handle })
    }
}

#[pymethods]
impl OpenCtpMarketGeneratorSource {
    #[new]
    #[allow(clippy::too_many_arguments)]
    #[pyo3(signature = (
        instruments,
        interval_ms,
        *,
        exchange_id="CFFEX".to_string(),
        trading_day=None,
        action_day=None,
        seed=None,
        base_price=4000.0,
        price_step=0.2,
        max_ticks=None,
        segment_descriptor_publisher=None,
    ))]
    fn new(
        py: Python<'_>,
        instruments: Vec<String>,
        interval_ms: u64,
        exchange_id: String,
        trading_day: Option<String>,
        action_day: Option<String>,
        seed: Option<u64>,
        base_price: f64,
        price_step: f64,
        max_ticks: Option<u64>,
        segment_descriptor_publisher: Option<Py<PyAny>>,
    ) -> PyResult<Self> {
        let mut config = OpenCtpMarketGeneratorConfig::new(instruments, interval_ms)
            .map_err(|error| py_value_error(error.to_string()))?;
        config.exchange_id = exchange_id;
        if let Some(trading_day) = trading_day {
            if action_day.is_none() {
                config.action_day = trading_day.clone();
            }
            config.trading_day = trading_day;
        }
        if let Some(action_day) = action_day {
            config.action_day = action_day;
        }
        if let Some(seed) = seed {
            config.seed = seed;
        }
        config
            .set_price_model(base_price, price_step)
            .map_err(|error| py_value_error(error.to_string()))?;
        config.max_ticks = max_ticks;

        let mut source = CoreOpenCtpMarketGeneratorSource::new(config.clone());
        if let Some(callback) = segment_descriptor_publisher {
            if !callback.bind(py).is_callable() {
                return Err(PyRuntimeError::new_err(
                    "segment_descriptor_publisher must be callable",
                ));
            }
            source =
                source.with_segment_descriptor_publisher(Arc::new(PySegmentDescriptorPublisher {
                    callback,
                }));
        }
        let metrics = source.metrics_handle();
        let status = source.status_handle();

        Ok(Self {
            source: Some(source),
            metrics,
            status,
            config,
        })
    }

    fn status(&self) -> &str {
        self.status.lock().unwrap().as_str()
    }

    fn config(&self, py: Python<'_>) -> PyResult<PyObject> {
        let dict = PyDict::new_bound(py);
        dict.set_item("instruments", &self.config.instruments)?;
        dict.set_item("interval_ms", self.config.interval_ms)?;
        dict.set_item("exchange_id", &self.config.exchange_id)?;
        dict.set_item("trading_day", &self.config.trading_day)?;
        dict.set_item("action_day", &self.config.action_day)?;
        dict.set_item("seed", self.config.seed)?;
        dict.set_item("base_price", self.config.base_price)?;
        dict.set_item("price_step", self.config.price_step)?;
        dict.set_item("max_ticks", self.config.max_ticks)?;
        Ok(dict.into_py(py))
    }

    fn metrics(&self, py: Python<'_>) -> PyResult<PyObject> {
        let metrics = self.metrics.lock().unwrap().clone();
        metrics_to_pydict(py, &metrics)
    }

    fn _zippy_output_schema(&self, py: Python<'_>) -> PyResult<PyObject> {
        zippy_openctp_core::schema::tick_data_schema()
            .to_pyarrow(py)
            .map_err(|error| PyRuntimeError::new_err(error.to_string()))
    }

    fn _zippy_source_mode(&self) -> &str {
        "pipeline"
    }

    fn _zippy_source_name(&self) -> &str {
        "openctp-market-generator-source"
    }

    fn _zippy_source_type(&self) -> &str {
        "openctp.generator"
    }

    fn _zippy_start(
        &mut self,
        py: Python<'_>,
        sink: Py<PyAny>,
    ) -> PyResult<Py<OpenCtpRuntimeHandle>> {
        let source = self
            .source
            .take()
            .ok_or_else(|| PyRuntimeError::new_err("openctp generator source already started"))?;
        let handle = Box::new(source)
            .start(Arc::new(PyCallbackSink { sink }))
            .map_err(|error| PyRuntimeError::new_err(error.to_string()))?;
        Py::new(py, OpenCtpRuntimeHandle { handle })
    }

    fn _zippy_start_native(
        &mut self,
        py: Python<'_>,
        sink_capsule: &Bound<'_, PyAny>,
    ) -> PyResult<Py<OpenCtpRuntimeHandle>> {
        let source = self
            .source
            .take()
            .ok_or_else(|| PyRuntimeError::new_err("openctp generator source already started"))?;
        let sink = NativeCapsuleSink::from_capsule(sink_capsule)?;
        let handle = Box::new(source)
            .start(Arc::new(sink))
            .map_err(|error| PyRuntimeError::new_err(error.to_string()))?;
        Py::new(py, OpenCtpRuntimeHandle { handle })
    }
}

#[pymodule]
fn _internal(py: Python<'_>, module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add_function(wrap_pyfunction!(tick_data_schema_fields, module)?)?;
    module.add_function(wrap_pyfunction!(runtime_handle_releases_gil, module)?)?;
    module.add_class::<OpenCtpMarketDataSource>()?;
    module.add_class::<OpenCtpMarketGeneratorSource>()?;
    module.add_class::<OpenCtpSegmentReader>()?;
    module.add_class::<PyOpenCtpSegmentTestWriter>()?;
    module.add_class::<OpenCtpRuntimeHandle>()?;
    let _ = py;
    Ok(())
}
