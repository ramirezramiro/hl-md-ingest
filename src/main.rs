
use std::{env, time::Duration};
use axum::{routing::get, Router};
use futures::{StreamExt};
use prometheus::{Encoder, IntCounter, IntCounterVec, Registry, TextEncoder};
use tokio::sync::mpsc;
use tokio_tungstenite::connect_async;
use tracing::{error, info, warn};
use tracing_subscriber::{EnvFilter, fmt};

static WS_DEFAULT: &str = "wss://stream.binance.com:9443/ws/btcusdt@trade";

#[derive(Clone)]
struct Metrics {
    msgs_total: IntCounter,
    reconnects_total: IntCounter,
    dropped_total: IntCounter,
    kind_total: IntCounterVec,
    registry: Registry,
}

impl Metrics {
    fn new() -> Self {
        let registry = Registry::new();
        let msgs_total = IntCounter::new("messages_total", "Total messages received").unwrap();
        let reconnects_total = IntCounter::new("reconnects_total", "Total reconnect attempts").unwrap();
        let dropped_total = IntCounter::new("dropped_total", "Total messages dropped due to back-pressure").unwrap();
        let kind_total = IntCounterVec::new(
            prometheus::Opts::new("events_total", "Event counts by type"),
            &["kind"],
        ).unwrap();

        registry.register(Box::new(msgs_total.clone())).unwrap();
        registry.register(Box::new(reconnects_total.clone())).unwrap();
        registry.register(Box::new(dropped_total.clone())).unwrap();
        registry.register(Box::new(kind_total.clone())).unwrap();

        Self { msgs_total, reconnects_total, dropped_total, kind_total, registry }
    }

    async fn serve(self, addr: &str) {
        let registry = self.registry.clone();
        let app = Router::new().route("/metrics", get(move || {
            let registry = registry.clone();
            async move {
                let encoder = TextEncoder::new();
                let metric_families = registry.gather();
                let mut buf = Vec::new();
                encoder.encode(&metric_families, &mut buf).unwrap();
                String::from_utf8(buf).unwrap()
            }
        }));

        info!("metrics server listening on {}", addr);
        axum::Server::bind(&addr.parse().unwrap())
            .serve(app.into_make_service())
            .await
            .unwrap();
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    fmt().with_env_filter(EnvFilter::from_default_env())
        .with_target(false)
        .compact()
        .init();

    let ws_url = env::var("WS_URL").unwrap_or_else(|_| WS_DEFAULT.to_string());
    let metrics_addr = env::var("METRICS_ADDR").unwrap_or_else(|_| "0.0.0.0:9000".to_string());
    let queue_cap: usize = env::var("QUEUE_CAP").ok().and_then(|s| s.parse().ok()).unwrap_or(1000);

    let metrics = Metrics::new();

    // metrics server
    let m_clone = metrics.clone();
    let server = tokio::spawn(async move {
        m_clone.serve(&metrics_addr).await;
    });

    let (tx, mut rx) = mpsc::channel::<String>(queue_cap);

    // processing task (counts, could be extended to parse, write parquet, etc.)
    let m_clone = metrics.clone();
    let proc = tokio::spawn(async move {
        while let Some(msg) = rx.recv().await {
            let _ = msg; // parse if needed: serde_json::from_str::<serde_json::Value>(&msg)
            m_clone.msgs_total.inc();
        }
    });

    // ingest with reconnect + backoff
    let m_clone = metrics.clone();
    let ingest = tokio::spawn(async move {
        let mut attempt: u64 = 0;
        loop {
            info!("connecting to {}", ws_url);
            match connect_async(&ws_url).await {
                Ok((ws_stream, _resp)) => {
                    info!("connected");
                    attempt = 0;
                    let (mut write, mut read) = ws_stream.split();
                    drop(write); // we only read in this sample

                    while let Some(msg) = read.next().await {
                        match msg {
                            Ok(m) => {
                                if m.is_text() || m.is_binary() {
                                    if let Err(_e) = tx.try_send(m.to_text().unwrap_or_default().to_string()) {
                                        m_clone.dropped_total.inc();
                                    }
                                } else if m.is_ping() {
                                    m_clone.kind_total.with_label_values(&["ping"]).inc();
                                } else if m.is_pong() {
                                    m_clone.kind_total.with_label_values(&["pong"]).inc();
                                } else if m.is_close() {
                                    warn!("server closed connection");
                                    break;
                                }
                            }
                            Err(e) => {
                                warn!("ws read error: {:?}, reconnecting", e);
                                break;
                            }
                        }
                    }
                }
                Err(e) => {
                    warn!("connect error: {:?}, backing off", e);
                }
            }

            attempt += 1;
            m_clone.reconnects_total.inc();
            let backoff = Duration::from_millis((200u64).saturating_mul((attempt.min(10)).pow(2)));
            tokio::time::sleep(backoff).await;
        }
    });

    tokio::try_join!(server, proc, ingest).unwrap();
    Ok(())
}
