pub mod clients;
mod handlers;
mod metrics;

use std::{net::SocketAddr, process::exit, sync::Arc, time::Duration};

use axum::{
    Router,
    extract::DefaultBodyLimit,
    http::{HeaderValue, StatusCode, header},
    routing::{delete, get, put},
};
use clients::{RealMetadataClient, RealStorageNodeClientFactory};
use opentelemetry::{KeyValue, global};
use opentelemetry_otlp::WithExportConfig;
use opentelemetry_sdk::{Resource, propagation::TraceContextPropagator, trace::SdkTracerProvider};
use prometheus_client::registry::Registry;
use std::sync::Mutex;
use storage_proto_lib::storage_metadata;
use storage_proto_lib::{MetadataClient, StorageNodeClientFactory};
use tokio::{net::TcpListener, time};
use tower::ServiceBuilder;
use tower_http::{ServiceBuilderExt, timeout::TimeoutLayer, trace::TraceLayer};
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

/// Application state shared across all handlers.
///
/// Generic over client types to enable dependency injection for testing.
pub struct AppState<M, F>
where
    M: MetadataClient,
    F: StorageNodeClientFactory,
{
    pub metadata_client: M,
    pub storage_factory: F,
    pub metrics: Arc<metrics::Metrics>,
    pub registry: Arc<Mutex<Registry>>,
    /// Shared connection cache for storage node gRPC clients, persisted across requests.
    pub storage_connections: Arc<tokio::sync::RwLock<std::collections::HashMap<String, F::Client>>>,
}

/// Type alias for production use with real gRPC clients.
pub type ProductionAppState = AppState<RealMetadataClient, RealStorageNodeClientFactory>;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let tracer_provider = init_tracer();
    init_logs();

    let addr = SocketAddr::from(([0, 0, 0, 0], 8080));
    tracing::info!(message = format!("listening on {}", addr));
    axum::serve(
        TcpListener::bind(addr).await.expect("bind error"),
        app().await.into_make_service(),
    )
    .await
    .expect("server error");

    tracer_provider
        .shutdown()
        .expect("shutdown tracer provider failed");
    Ok(())
}

async fn app() -> Router {
    // Support METADATA_URLS (comma-separated) with fallback to METADATA_URL
    let metadata_urls: Vec<String> = std::env::var("METADATA_URLS")
        .map(|s| s.split(',').map(|u| u.trim().to_string()).filter(|u| !u.is_empty()).collect())
        .unwrap_or_else(|_| {
            vec![std::env::var("METADATA_URL").unwrap_or("http://127.0.0.1:3001".to_string())]
        });

    let mut retries = 5;
    let tonic_client = 'outer: loop {
        for url in &metadata_urls {
            match storage_metadata::storage_metadata_service_client::StorageMetadataServiceClient::connect(
                url.clone(),
            )
            .await {
                Ok(client) => {
                    tracing::info!("Connected to metadata server at {}", url);
                    break 'outer client;
                }
                Err(e) => {
                    tracing::warn!("Failed to connect to metadata server {}: {}", url, e);
                }
            }
        }
        retries -= 1;
        if retries < 0 {
            tracing::error!("Failed to connect to any metadata server after retries");
            exit(1);
        }
        tracing::warn!("Retrying metadata connection in 2s (retries left: {})", retries);
        time::sleep(Duration::from_secs(2)).await;
    };

    // Wrap the tonic client in our trait wrapper
    let metadata_client = RealMetadataClient::new(tonic_client);
    let storage_factory = RealStorageNodeClientFactory;

    let sensitive_headers: Arc<[_]> = vec![header::AUTHORIZATION, header::COOKIE].into();

    let middleware = ServiceBuilder::new()
        .sensitive_request_headers(sensitive_headers.clone())
        .sensitive_response_headers(sensitive_headers)
        .layer(TimeoutLayer::with_status_code(
            StatusCode::REQUEST_TIMEOUT,
            Duration::from_secs(120),
        ))
        .compression()
        .insert_request_header_if_not_present(
            header::CONTENT_TYPE,
            HeaderValue::from_static("application/octet-stream"),
        );

    // Initialize metrics
    let mut registry = Registry::default();
    let app_metrics = Arc::new(metrics::Metrics::new(&mut registry));
    let registry = Arc::new(Mutex::new(registry));

    let state: Arc<ProductionAppState> = Arc::new(AppState {
        metadata_client,
        storage_factory,
        metrics: app_metrics,
        registry,
        storage_connections: Arc::new(tokio::sync::RwLock::new(std::collections::HashMap::new())),
    });

    Router::new()
        .route("/health", get(|| async { "OK" }))
        .route("/metrics", get(handlers::handle_metrics::<RealMetadataClient, RealStorageNodeClientFactory>))
        .route("/{objectName}", put(handlers::handle_put_dispatch::<RealMetadataClient, RealStorageNodeClientFactory>))
        .route("/{objectName}", get(handlers::handle_get_dispatch::<RealMetadataClient, RealStorageNodeClientFactory>))
        .route("/", get(handlers::handle_list_objects::<RealMetadataClient, RealStorageNodeClientFactory>))
        .route("/{objectName}", delete(handlers::handle_delete_object::<RealMetadataClient, RealStorageNodeClientFactory>))
        .layer(DefaultBodyLimit::disable()) // No global limit; multipart streaming + JSON extractor handle limits per-route
        .with_state(state)
        .layer(middleware)
        .layer(TraceLayer::new_for_http())
}

fn init_tracer() -> SdkTracerProvider {
    global::set_text_map_propagator(TraceContextPropagator::new());

    let otlp_endpoint =
        std::env::var("OTLP_ENDPOINT").unwrap_or_else(|_| "http://localhost:4317".to_string());

    let exporter = opentelemetry_otlp::SpanExporter::builder()
        .with_tonic()
        .with_endpoint(&otlp_endpoint)
        .build()
        .expect("failed to create OTLP exporter");

    let provider = SdkTracerProvider::builder()
        .with_batch_exporter(exporter)
        .with_resource(
            Resource::builder()
                .with_attributes([
                    KeyValue::new("service.name", env!("CARGO_PKG_NAME")),
                    KeyValue::new("service.version", env!("CARGO_PKG_VERSION")),
                ])
                .build(),
        )
        .build();

    global::set_tracer_provider(provider.clone());
    provider
}

fn init_logs() {
    let fmt_layer = tracing_subscriber::fmt::layer()
        .json()
        .with_target(true)
        .with_current_span(true)
        .with_span_list(false)
        .with_span_events(
            tracing_subscriber::fmt::format::FmtSpan::NEW
                | tracing_subscriber::fmt::format::FmtSpan::CLOSE,
        );
    tracing_subscriber::registry()
        .with(fmt_layer)
        .with(tracing_subscriber::filter::LevelFilter::INFO)
        .init();
}
