use pyo3::exceptions::{PyBufferError, PyValueError};
use pyo3::ffi;
use pyo3::ffi::{Py_buffer, Py_ssize_t};
use pyo3::prelude::*;
use pyo3::types::PySlice;
use pyo3::{AsPyPointer, PyAny, PyResult, Python};
use std::ffi::c_long;
use std::ptr;
use std::sync::Arc;

/// Rust-side buffer which can be zero-copy viewed in python
///
/// Expected usage is with memoryview() to do further slicing, with [..] to get
/// direct slicing (which will yield bytes) and a simple file-like interface,
/// which also yields bytes.
///
/// This object can be shared between threads.
#[pyclass]
pub struct ArcVec {
    data: Arc<Vec<u8>>,
    loc: i64,
}

unsafe impl Send for ArcVec {}

#[pymethods]
impl ArcVec {
    #[new]
    pub fn new(n: usize) -> ArcVec {
        let buf: Vec<u8> = b"0".repeat(n).to_vec();
        ArcVec { data: Arc::new(buf), loc: 0 }
    }

    /// Only accepts python slices, not integers, lists or anything else. Slice
    /// must not have a step.
    pub fn __getitem__(&self, sl: &PySlice) -> PyResult<&[u8]> {
        let indices = sl.indices(self.data.len() as c_long)?;
        if indices.step > 1 {
            return Err(PyValueError::new_err("shouldn't step"));
        }
        // becomes a bytes by copy
        Ok(&self.data[indices.start as usize..indices.stop as usize])
    }

    /// n<0 or None implies read all
    pub fn read(&mut self, n: Option<i64>) -> &[u8] {
        let here = match self.loc {
            x if x < 0 => 0,
            x if x > self.data.len() as i64 => self.data.len(),
            x => x as usize,
        };
        self.loc = match n {
            Some(n) if n >= 0 => self.loc + n,
            Some(_) | None => self.data.len() as i64,
        };
        let there = match self.loc {
            x if x < 0 => 0,
            x if x > self.data.len() as i64 => self.data.len(),
            x => x as usize,
        };
        &self.data[here..there]
    }

    pub fn tell(&self) -> i64 {
        self.loc
    }

    pub fn seek(&mut self, n: i64, whence: Option<usize>) -> PyResult<i64> {
        match whence {
            None | Some(0) => self.loc = n,
            Some(1) => self.loc = self.loc + n,
            Some(2) => self.loc = (self.data.len() as i64 + n),
            _ => return Err(PyValueError::new_err("bad whence")),
        }
        Ok(self.loc)
    }

    pub unsafe fn __getbuffer__(
        self_: PyRefMut<'_, Self>, buf: *mut Py_buffer, flags: i32,
    ) -> PyResult<()> {
        let flags = flags as std::os::raw::c_int;
        if (flags & ffi::PyBUF_WRITABLE) == ffi::PyBUF_WRITABLE {
            return Err(PyBufferError::new_err("Object is not writable"));
        }
        (*buf).len = self_.data.len() as Py_ssize_t;
        (*buf).buf = self_.data.as_ptr() as *mut std::os::raw::c_void;
        (*buf).obj = self_.as_ptr() as *mut ffi::PyObject;
        ffi::Py_INCREF((*buf).obj);
        (*buf).readonly = 1;
        (*buf).itemsize = 1;
        (*buf).format = ptr::null_mut();
        if (flags & ffi::PyBUF_FORMAT) == ffi::PyBUF_FORMAT {
            // "format" field has been demanded
            let msg = std::ffi::CStr::from_bytes_with_nul(b"B\0").unwrap();
            (*buf).format = msg.as_ptr() as *mut _;
        };
        (*buf).ndim = 1;
        (*buf).shape = ptr::null_mut();
        (*buf).strides = ptr::null_mut();
        (*buf).suboffsets = ptr::null_mut();
        (*buf).internal = ptr::null_mut();
        if (flags & ffi::PyBUF_ND) == ffi::PyBUF_ND {
            // "nd" flag is set, so require shape as a one-element array
            (*buf).shape = (&((*buf).len)) as *const _ as *mut _;
        };
        if (flags & ffi::PyBUF_STRIDES) == ffi::PyBUF_STRIDES {
            // "strides" flag is set, so require strides as a one-element array
            (*buf).strides = &((*buf).itemsize) as *const _ as *mut _;
        };
        Ok(())
    }

    pub unsafe fn __releasebuffer__(&self, _buf: *mut Py_buffer) -> () {}
}

/*
impl Drop for ArcVec {
    fn drop(&mut self) {
        println!("drop");
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
*/
