use pyo3::buffer::PyBuffer;
use pyo3::exceptions::PyMemoryError;
use pyo3::ffi::Py_ssize_t;
use pyo3::prelude::*;
use pyo3::types::PyBytes;
use pyo3::{ffi, AsPyPointer};
use std::ffi::{c_char, c_void};
use std::ptr::copy_nonoverlapping;
use std::sync::{Arc, Mutex};

#[pyclass]
pub struct ArcVec {
    // inspired by https://github.com/PyO3/pyo3/blob/
    //   3b3ba4e3abd57bc3b8f86444b3f61e6e2f4c5fc1/tests/test_buffer_protocol.rs#L16
    data: PyBuffer<u8>,
    obj: *mut ffi::PyObject,
}

unsafe impl Send for ArcVec {}

#[pymethods]
impl ArcVec {
    #[new]
    pub fn new(data: &PyAny) -> ArcVec {
        let ptr = data.as_ptr();
        let buf: PyBuffer<u8> = PyBuffer::get(data).unwrap();
        ArcVec { data: buf, obj: ptr }
    }

    pub fn memoryview<'py>(
        &mut self, py: Python<'py>,
    ) -> PyResult<&'py PyAny> {
        let l = self.data.len_bytes();
        let pt = self.data.buf_ptr();

        Ok(unsafe {
            let out = ffi::PyMemoryView_FromMemory(
                pt as *mut c_char,
                l as Py_ssize_t,
                ffi::PyBUF_READ,
            );
            ffi::Py_INCREF(self.obj);
            py.from_owned_ptr(out)
        })
    }
}

impl Drop for ArcVec {
    fn drop(&mut self) {
        unsafe { ffi::Py_DECREF(self.obj) };
    }
}

#[pyfunction]
pub fn pybuf_from_pybuf<'py>(
    py: Python<'py>, data: &PyAny,
) -> PyResult<&'py PyAny> {
    // unsafe: if original obj disappears, result of this fn is still accessible and
    // will cause segfault
    let buf: PyBuffer<u8> = PyBuffer::get(data).unwrap();
    let l = buf.len_bytes();
    let pt = buf.buf_ptr();
    Ok(unsafe {
        let out = ffi::PyMemoryView_FromMemory(
            pt as *mut c_char,
            l as Py_ssize_t,
            ffi::PyBUF_READ,
        );
        py.from_owned_ptr(out)
    })
}

#[pyfunction]
pub fn pybytes_from_pybytes<'py>(
    py: Python<'py>, data: &PyBytes,
) -> PyResult<&'py PyBytes> {
    let l = data.len().unwrap();
    let b = unsafe {
        ffi::PyBytes_FromStringAndSize(std::ptr::null(), l as Py_ssize_t)
            .as_mut()
    };
    match b {
        Some(mut_pyob) => {
            let dst = unsafe { ffi::PyBytes_AsString(mut_pyob) };
            let src = unsafe { ffi::PyBytes_AsString(data.into_ptr()) };
            unsafe { copy_nonoverlapping(src, dst, l) };
            Ok(unsafe { py.from_owned_ptr(mut_pyob) })
        }
        _ => {
            Err(PyMemoryError::new_err("Failed to allocate new bytes object"))
        }
    }
}
