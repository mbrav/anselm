use http_body_util::{combinators, BodyExt, Full};
use hyper::{
    body::{Bytes, Incoming},
    server::conn::http1,
    service::service_fn,
    Request, Response,
};
use hyper_util::rt::TokioIo;
use prometheus_client::{encoding::text::encode, metrics::counter::Counter, registry::Registry};
use std::{
    future::Future,
    io,
    net::{IpAddr, Ipv4Addr, SocketAddr},
    pin::Pin,
    sync::Arc,
};

use tokio::{
    net::TcpListener,
    pin,
    signal::unix::{signal, SignalKind},
    task::JoinHandle,
};

/// Create a new JoinHandle for Promethues server
pub async fn new_task(port: u16) -> JoinHandle<()> {
    // Initialize metrics
    let request_counter: Counter<u64> = Default::default();

    let mut registry = <Registry>::with_prefix("tokio_hyper_example");

    registry.register(
        "requests",
        "How many requests the application has received",
        request_counter.clone(),
    );

    // Return a handlert for serving OpenMetrics endpoint.
    let metrics_addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(0, 0, 0, 0)), port);
    tokio::spawn(async move {
        start_metrics_server(metrics_addr, registry).await.unwrap();
    })
}

/// Start a HTTP server to report metrics.
async fn start_metrics_server(
    metrics_addr: SocketAddr,
    registry: Registry,
) -> Result<(), Box<dyn std::error::Error>> {
    eprintln!("Starting metrics server on {metrics_addr}");

    let registry = Arc::new(registry);

    let tcp_listener = TcpListener::bind(metrics_addr).await.unwrap();
    let server = http1::Builder::new();
    while let Ok((stream, _)) = tcp_listener.accept().await {
        let mut shutdown_stream = signal(SignalKind::terminate()).unwrap();
        let io = TokioIo::new(stream);
        let server_clone = server.clone();
        let registry_clone = registry.clone();
        tokio::task::spawn(async move {
            let conn = server_clone.serve_connection(io, service_fn(make_handler(registry_clone)));
            pin!(conn);
            tokio::select! {
                _ = conn.as_mut() => {}
                _ = shutdown_stream.recv() => {
                    conn.as_mut().graceful_shutdown();
                }
            }
        });
    }
    Ok(())
}

/// Boxed HTTP body for responses
type BoxBody = combinators::BoxBody<Bytes, hyper::Error>;

/// This function returns a HTTP handler (i.e. another function)
fn make_handler(
    registry: Arc<Registry>,
) -> impl Fn(Request<Incoming>) -> Pin<Box<dyn Future<Output = io::Result<Response<BoxBody>>> + Send>>
{
    // This closure accepts a request and responds with the OpenMetrics encoding of our metrics.
    move |_req: Request<Incoming>| {
        let reg = registry.clone();

        Box::pin(async move {
            let mut buf = String::new();
            encode(&mut buf, &reg.clone())
                .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))
                .map(|_| {
                    let body = full(Bytes::from(buf));
                    Response::builder()
                        .header(
                            hyper::header::CONTENT_TYPE,
                            "application/openmetrics-text; version=1.0.0; charset=utf-8",
                        )
                        .body(body)
                        .unwrap()
                })
        })
    }
}

/// helper function to build a full boxed body
fn full(body: Bytes) -> BoxBody {
    Full::new(body).map_err(|never| match never {}).boxed()
}
