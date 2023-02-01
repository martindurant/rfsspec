use pyo3::prelude::*;
use std::collections::HashMap;

use futures::future::join_all;
use lazy_static::lazy_static;
use reqwest::*;
use tokio::runtime::{Builder, Runtime};

lazy_static! {
    static ref RUNTIME: Runtime = Builder::new_current_thread().enable_all().build().unwrap();
}

async fn get_url(url: &str) -> Result<String> {
    Ok(reqwest::get(url).await?.text().await?)
}

#[pyfunction]
fn get_urls(urls: Vec<&str>) -> HashMap<&str, String> {
    let mut result: HashMap<&str, String> = HashMap::new();
    let coroutine = async {
        let out = urls.iter().map(|u| get_url(*u));

        let bits = join_all(out).await;
        urls.iter()
            .zip(bits.iter())
            .map(|(u, s)| result.insert(*u, s.as_ref().unwrap().clone()))
            .into_iter()
            .count();
    };
    RUNTIME.block_on(coroutine);
    result
}

/// A Python module implemented in Rust.
#[pymodule]
fn rfsspec(_py: Python, m: &PyModule) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(get_urls, m)?)?;
    Ok(())
}
