use std::ffi::CString;
use std::io::Cursor;
use std::os::raw::{c_char, c_int, c_void};

use arrow::ipc::writer::StreamWriter;
use arrow::record_batch::RecordBatch;
use pyo3::exceptions::PyRuntimeError;
use pyo3::prelude::*;
use pyo3::types::{PyAnyMethods, PyCapsule, PyCapsuleMethods};
use zippy_core::{SourceEvent, SourceSink, StreamHello, ZippyError};

const NATIVE_SOURCE_SINK_CAPSULE_NAME: &str = "zippy.native_source_sink.v1";

#[repr(C)]
#[derive(Clone, Copy)]
pub struct NativeSourceSinkAbi {
    pub ctx: *mut c_void,
    pub emit_hello: unsafe extern "C" fn(*mut c_void, *const c_char, u16) -> c_int,
    pub emit_data_ipc: unsafe extern "C" fn(*mut c_void, *const u8, usize) -> c_int,
    pub emit_flush: unsafe extern "C" fn(*mut c_void) -> c_int,
    pub emit_stop: unsafe extern "C" fn(*mut c_void) -> c_int,
    pub emit_error: unsafe extern "C" fn(*mut c_void, *const c_char, usize) -> c_int,
}

unsafe impl Send for NativeSourceSinkAbi {}
unsafe impl Sync for NativeSourceSinkAbi {}

pub struct NativeCapsuleSink {
    _capsule_owner: Py<PyAny>,
    abi: NativeSourceSinkAbi,
}

unsafe impl Send for NativeCapsuleSink {}
unsafe impl Sync for NativeCapsuleSink {}

impl NativeCapsuleSink {
    pub fn from_capsule(capsule: &Bound<'_, PyAny>) -> PyResult<Self> {
        let capsule = capsule.downcast::<PyCapsule>()?;
        let capsule_name = capsule
            .name()?
            .and_then(|value| value.to_str().ok())
            .ok_or_else(|| {
                PyRuntimeError::new_err("native source sink capsule has no valid name")
            })?;
        if capsule_name != NATIVE_SOURCE_SINK_CAPSULE_NAME {
            return Err(PyRuntimeError::new_err(format!(
                "unexpected native source sink capsule name [{capsule_name}]"
            )));
        }
        let abi = unsafe { *capsule.reference::<NativeSourceSinkAbi>() };
        Ok(Self {
            _capsule_owner: capsule.clone().unbind().into_any(),
            abi,
        })
    }

    fn call_result(&self, kind: &str, code: c_int) -> zippy_core::Result<()> {
        if code == 0 {
            return Ok(());
        }

        Err(ZippyError::Io {
            reason: format!("native source sink call failed kind=[{kind}] code=[{code}]"),
        })
    }

    fn emit_hello(&self, hello: StreamHello) -> zippy_core::Result<()> {
        let stream_name = CString::new(hello.stream_name).map_err(|error| ZippyError::Io {
            reason: format!("native source sink stream name contains nul error=[{error}]"),
        })?;
        let code = unsafe {
            (self.abi.emit_hello)(self.abi.ctx, stream_name.as_ptr(), hello.protocol_version)
        };
        self.call_result("hello", code)
    }

    fn emit_data(&self, batch: RecordBatch) -> zippy_core::Result<()> {
        let bytes = serialize_batch_to_ipc(&batch)?;
        let code = unsafe { (self.abi.emit_data_ipc)(self.abi.ctx, bytes.as_ptr(), bytes.len()) };
        self.call_result("data_ipc", code)
    }

    fn emit_flush(&self) -> zippy_core::Result<()> {
        let code = unsafe { (self.abi.emit_flush)(self.abi.ctx) };
        self.call_result("flush", code)
    }

    fn emit_stop(&self) -> zippy_core::Result<()> {
        let code = unsafe { (self.abi.emit_stop)(self.abi.ctx) };
        self.call_result("stop", code)
    }

    fn emit_error(&self, reason: String) -> zippy_core::Result<()> {
        let reason_bytes = reason.into_bytes();
        let code = unsafe {
            (self.abi.emit_error)(
                self.abi.ctx,
                reason_bytes.as_ptr().cast::<c_char>(),
                reason_bytes.len(),
            )
        };
        self.call_result("error", code)
    }
}

impl SourceSink for NativeCapsuleSink {
    fn emit(&self, event: SourceEvent) -> zippy_core::Result<()> {
        match event {
            SourceEvent::Hello(hello) => self.emit_hello(hello),
            SourceEvent::Data(batch) => self.emit_data(batch.to_record_batch()?),
            SourceEvent::Flush => self.emit_flush(),
            SourceEvent::Stop => self.emit_stop(),
            SourceEvent::Error(reason) => self.emit_error(reason),
        }
    }
}

fn serialize_batch_to_ipc(batch: &RecordBatch) -> zippy_core::Result<Vec<u8>> {
    let mut bytes = Vec::new();
    let mut writer = StreamWriter::try_new(Cursor::new(&mut bytes), batch.schema().as_ref())
        .map_err(|error| ZippyError::Io {
            reason: format!("native source sink failed to open ipc writer error=[{error}]"),
        })?;
    writer.write(batch).map_err(|error| ZippyError::Io {
        reason: format!("native source sink failed to write ipc batch error=[{error}]"),
    })?;
    writer.finish().map_err(|error| ZippyError::Io {
        reason: format!("native source sink failed to finish ipc stream error=[{error}]"),
    })?;
    Ok(bytes)
}
