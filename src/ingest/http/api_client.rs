use super::rate_limiter::RateLimiterRegistry;
use crate::error::{AppError, AppResult};
use crate::ingest::metrics::IngestMetrics;
use crate::ingest::spec::HttpRequestSpec;
use std::sync::Arc;

use reqwest::{Client, Response};
use std::time::Duration;

#[derive(Debug, Clone)]
pub struct ApiClient {
    pub name: &'static str,
    pub base_url: String,
    pub http: Client,
    pub limiter_registry: Option<Arc<RateLimiterRegistry>>,
    pub timeout: Duration,
    pub metrics: Option<Arc<IngestMetrics>>,
}

impl ApiClient {
    pub fn new(
        name: &'static str,
        base_url: impl Into<String>,
        limiter_registry: Option<Arc<RateLimiterRegistry>>,
        metrics: Option<Arc<IngestMetrics>>,
    ) -> Self {
        Self {
            name,
            base_url: base_url.into(),
            http: Client::new(),
            limiter_registry,
            timeout: Duration::from_secs(10),
            metrics,
        }
    }

    /// Execute a fully-resolved HTTP request spec.
    /// Applies limiter (if present) and syncs used-weight from headers.
    pub async fn execute(&self, spec: &HttpRequestSpec) -> AppResult<Response> {
        if let Some(l) = &self.limiter_registry {
            l.acquire(self.name, spec.weight).await;
        }

        let url = format!("{}{}", self.base_url, spec.path);

        let mut rb = self
            .http
            .request(spec.method.clone(), url)
            .timeout(self.timeout);

        if !spec.query.is_empty() {
            rb = rb.query(&spec.query);
        }

        for (k, v) in &spec.headers {
            rb = rb.header(k, v);
        }

        if let Some(body) = &spec.json_body {
            rb = rb.json(body);
        }

        let resp = rb.send().await.map_err(AppError::Reqwest)?;

        // Sync limiter from headers (Binance x-mbx-used-weight-1m etc.)
        if let Some(l) = &self.limiter_registry {
            l.set_used_weight(self.name, Some(resp.headers()), None)
                .await;
        }

        Ok(resp)
    }

    /// Execute and deserialize JSON, with consistent API error mapping.
    pub async fn execute_json<T: serde::de::DeserializeOwned>(
        &self,
        spec: &HttpRequestSpec,
    ) -> AppResult<T> {
        let resp = self.execute(spec).await?;

        if !resp.status().is_success() {
            println!("status = {}", resp.status());
            let status = resp.status();
            let body = resp.text().await.unwrap_or_default();

            if let Some(m) = &self.metrics {
                m.inc_error();
            }

            return Err(AppError::Api {
                service: self.name.to_string(),
                status,
                body,
            });
        }

        resp.json::<T>().await.map_err(|e| {
            if let Some(m) = &self.metrics {
                m.inc_error();
            }
            // This is a JSON decode/transport-level error from reqwest:
            AppError::Reqwest(e)
        })
    }
}

impl ApiClient {
    /// Poll a fixed, fully-resolved request spec forever.
    /// Best when the spec is stable (symbol/coin doesn't change).
    pub async fn poll_json_spec<T, F, Fut>(
        &self,
        spec: HttpRequestSpec,
        mut on_item: F,
    ) -> AppResult<()>
    where
        T: serde::de::DeserializeOwned,
        F: FnMut(T) -> Fut,
        Fut: std::future::Future<Output = AppResult<()>>,
    {
        let mut interval = tokio::time::interval(Duration::from_secs(spec.interval_seconds));

        loop {
            interval.tick().await;

            match self.execute_json::<T>(&spec).await {
                Ok(item) => {
                    if let Some(m) = &self.metrics {
                        m.inc_in();
                    }

                    match on_item(item).await {
                        Ok(()) => {
                            if let Some(m) = &self.metrics {
                                m.inc_processed();
                            }
                        }
                        Err(e) => {
                            if let Some(m) = &self.metrics {
                                m.inc_error();
                            }
                            return Err(e);
                        }
                    }
                }
                Err(e) => {
                    if let Some(m) = &self.metrics {
                        m.inc_error();
                    }
                    return Err(e);
                }
            }
        }
    }

    /// Poll a dynamically-built spec forever.
    /// Best when something must change each tick (rotating symbols, timestamps, signing, etc.).
    ///
    /// `build_spec` is called every tick to produce the current request spec.
    pub async fn poll_json_spec_dynamic<T, F, Fut, B>(
        &self,
        mut build_spec: B,
        mut on_item: F,
    ) -> AppResult<()>
    where
        T: serde::de::DeserializeOwned,
        F: FnMut(T) -> Fut,
        Fut: std::future::Future<Output = AppResult<()>>,
        B: FnMut() -> AppResult<HttpRequestSpec>,
    {
        // Build once to know the interval
        let first = build_spec()?;
        let mut interval = tokio::time::interval(Duration::from_secs(first.interval_seconds));

        loop {
            interval.tick().await;

            let spec = build_spec()?;

            match self.execute_json::<T>(&spec).await {
                Ok(item) => {
                    if let Some(m) = &self.metrics {
                        m.inc_in();
                    }

                    match on_item(item).await {
                        Ok(()) => {
                            if let Some(m) = &self.metrics {
                                m.inc_processed();
                            }
                        }
                        Err(e) => {
                            if let Some(m) = &self.metrics {
                                m.inc_error();
                            }
                            return Err(e);
                        }
                    }
                }
                Err(e) => {
                    if let Some(m) = &self.metrics {
                        m.inc_error();
                    }
                    return Err(e);
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::app::config::load_app_config;
    use crate::error::{AppError, AppResult};
    use crate::ingest::config::ExchangeConfigs;

    use crate::ingest::spec::resolve::resolve_http_request;
    use crate::ingest::spec::{Ctx, ParamPlacement};

    use reqwest::Response;
    use serde_json::Value as JsonValue;

    fn pretty_json(v: &JsonValue) -> String {
        serde_json::to_string_pretty(v).unwrap_or_else(|_| "<json pretty failed>".into())
    }

    async fn read_body(resp: Response) -> (reqwest::StatusCode, String) {
        let status = resp.status();
        let body = match resp.text().await {
            Ok(t) if !t.trim().is_empty() => t,
            Ok(_) => "<empty body>".to_string(),
            Err(e) => format!("<failed to read body: {e}>"),
        };
        (status, body)
    }

    fn print_http_result(label: &str, status: reqwest::StatusCode, body: &str) {
        println!("{label} status = {status}");
        if let Ok(v) = serde_json::from_str::<JsonValue>(body) {
            println!("{label} body (json) = {}", pretty_json(&v));
        } else {
            println!("{label} body (raw) = {body}");
        }
    }

    #[tokio::test]
    async fn test_api_calls_binance_and_hyperliquid_depth() -> AppResult<()> {
        // 1) Load configs
        let appconfig = load_app_config()?;
        let exchangeconfigs = ExchangeConfigs::new(&appconfig)?;

        let binance = exchangeconfigs.binance_linear.as_ref().ok_or_else(|| {
            AppError::InvalidConfig("binance_linear missing in ExchangeConfigs".into())
        })?;

        let hyper = exchangeconfigs.hyperliquid_perp.as_ref().ok_or_else(|| {
            AppError::InvalidConfig("hyperliquid_perp missing in ExchangeConfigs".into())
        })?;

        // 2) Build ctx
        let mut ctx: Ctx = Ctx::new();
        ctx.insert("symbol".into(), "btcusdt".into());
        ctx.insert("coin".into(), "btcperp".into());

        // 3) Resolve request specs
        let binance_depth_ep = binance
            .api
            .get("depth")
            .ok_or_else(|| AppError::InvalidConfig("binance api.depth missing".into()))?;
        let binance_spec = resolve_http_request(binance_depth_ep, &ctx, ParamPlacement::Query)?;

        let hyper_depth_ep = hyper
            .api
            .get("depth")
            .ok_or_else(|| AppError::InvalidConfig("hyperliquid api.depth missing".into()))?;
        let hyper_spec = resolve_http_request(hyper_depth_ep, &ctx, ParamPlacement::JsonBody)?;

        println!("--- resolved specs ---");
        println!(
            "binance: method={:?} base_url={} path={} query={:?} json_body={:?}",
            binance_spec.method,
            binance.api_base_url,
            binance_spec.path,
            binance_spec.query,
            binance_spec.json_body
        );
        println!(
            "hyper:   method={:?} base_url={} path={} query={:?} json_body={}",
            hyper_spec.method,
            hyper.api_base_url,
            hyper_spec.path,
            hyper_spec.query,
            hyper_spec
                .json_body
                .as_ref()
                .map(pretty_json)
                .unwrap_or_else(|| "<none>".into())
        );

        // 4) Build API clients (no limiter/metrics in this test)
        let binance_client =
            ApiClient::new("binance_test", binance.api_base_url.clone(), None, None);
        let hyper_client =
            ApiClient::new("hyperliquid_test", hyper.api_base_url.clone(), None, None);

        // 5) Execute Binance
        println!("\n--- CALL 1: Binance depth ---");
        let resp1 = binance_client.execute(&binance_spec).await?;
        let (status1, body1) = read_body(resp1).await;
        print_http_result("binance", status1, &body1);

        // 6) Execute Hyperliquid
        println!("\n--- CALL 2: Hyperliquid depth ---");
        let resp2 = hyper_client.execute(&hyper_spec).await?;
        let (status2, body2) = read_body(resp2).await;
        print_http_result("hyperliquid", status2, &body2);

        Ok(())
    }
}
