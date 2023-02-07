use pyo3::prelude::*;
use std::collections::HashMap;

use futures::future::join_all;
use lazy_static::lazy_static;
use reqwest;
use tokio::runtime::{Builder, Runtime};

lazy_static! {
    static ref RUNTIME: Runtime = Builder::new_current_thread().enable_all().build().unwrap();
}
lazy_static! {
    static ref CLIENT: reqwest::Client = reqwest::Client::new();
}

async fn get_url(
    url: &str,
    method: reqwest::Method,
    head: HashMap<&str, &str>,
    body: Option<&str>,
) -> reqwest::Result<String> {
    let mut req = CLIENT.request(method, url);
    for (key, value) in head.iter() {
        req = req.header(*key, *value);
    }
    req = match body {
        Some(text) => req.body(text.to_string()),
        None => req,
    };
    Ok(req.send().await?.text().await?)
}

async fn get_url_or(url: &str) -> String {
    let headers: HashMap<&str, &str> = HashMap::new();
    match get_url(url, reqwest::Method::GET, headers, None).await {
        Ok(text) => text,
        Err(e) => e.to_string(),
    }
}

#[pyfunction]
fn get_urls(urls: Vec<&str>) -> HashMap<&str, String> {
    let mut result: HashMap<&str, String> = HashMap::new();
    let coroutine = async {
        let out = urls.iter().map(|u| get_url_or(*u));

        let bits = join_all(out).await;
        urls.iter()
            .zip(bits.iter())
            .map(|(u, s)| result.insert(*u, s.clone()))
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
