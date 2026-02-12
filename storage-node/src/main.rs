use local_ip_address::local_ip;
use std::process::exit;
use std::time::Duration;
use std::env;

use opentelemetry::{global, KeyValue};
use opentelemetry_sdk::{propagation::TraceContextPropagator, trace::SdkTracerProvider, Resource};
use opentelemetry_otlp::WithExportConfig;
use storage_proto_lib::storage_metadata::{PutStorageNodeRequest, storage_metadata_service_client};
use tokio::time;
use tonic::Code;
use tonic::{Request, Response, Status, transport::Server};
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};
use prometheus_client::registry::Registry;
use std::sync::{Arc, Mutex};
use axum::{Router, routing::get, http::header};
use tokio::net::TcpListener;

use storage_proto_lib::storage_node::storage_node_service_server::{
    StorageNodeService, StorageNodeServiceServer,
};
use storage_proto_lib::storage_node::{
    CheckHealthRequest, CheckHealthResponse, DeleteChunkRequest, DeleteChunkResponse,
    GetChunkSizeRequest, GetChunkSizeResponse, GetFreeChunksRequest, GetFreeChunksResponse,
    ReadChunkRequest, ReadChunkResponse, WriteChunkRequest, WriteChunkResponse,
};

use crate::chunk_store::ChunkStore;

mod chunk_store;
mod config;
mod grpc_utils;
mod metrics;

const MAX_GRPC_MESSAGE_SIZE: usize = 16 * 1024 * 1024; // 16 MiB for 8 MiB chunks + overhead

pub struct StorageNodeImpl {
    store: std::sync::Arc<ChunkStore>,
    metrics: Arc<metrics::Metrics>,
}

impl StorageNodeImpl {
    pub fn construct(
        metrics: Arc<metrics::Metrics>,
        chunk_store_path: &str,
        allocated_slots: Option<usize>,
        options: chunk_store::ChunkStoreOptions,
    ) -> std::io::Result<Self> {
        let recover = std::fs::exists(chunk_store_path).unwrap_or(false);
        let store = ChunkStore::open(chunk_store_path, allocated_slots, options.clone())?;
        if recover {
            store.recover_index(options.fast_recover)?;
        }
        let store = std::sync::Arc::new(store);
        Ok(StorageNodeImpl { store, metrics })
    }
}

#[tonic::async_trait]
impl StorageNodeService for StorageNodeImpl {
    #[tracing::instrument(skip(self, request), fields(chunk_id = request.get_ref().chunk_id, chunk_bytes_len = request.get_ref().chunk_bytes.len(), error))]
    async fn write_chunk(
        &self,
        request: Request<WriteChunkRequest>,
    ) -> Result<Response<WriteChunkResponse>, Status> {
        let chunk_id = request.get_ref().chunk_id;
        let chunk_bytes = request.get_ref().chunk_bytes.clone();
        let span = tracing::Span::current();
        let store = self.store.clone();

        // Offload blocking I/O to the blocking thread pool
        let res = tokio::task::spawn_blocking(move || store.put_chunk(chunk_id, &chunk_bytes))
        .await
        .map_err(|e| Status::new(Code::Internal, format!("task join error: {}", e)))?;

        if let Err(res_err) = res {
            let return_status = match res_err.kind() {
                std::io::ErrorKind::AlreadyExists => {
                    span.record("error", "chunk id already exists");
                    Status::new(Code::AlreadyExists, "chunk id already exists")
                }
                std::io::ErrorKind::StorageFull => {
                    span.record("error", "chunk store is full");
                    Status::new(Code::ResourceExhausted, "chunk store is full")
                }
                _ => {
                    span.record("error", "failed to write chunk");
                    Status::new(Code::Internal, "failed to write chunk")
                }
            };
            return Err(return_status);
        }

        let bytes_len = request.get_ref().chunk_bytes.len();
        self.metrics.chunks_written_total.inc();
        self.metrics.bytes_written_total.inc_by(bytes_len as u64);
        self.metrics.grpc_requests_total.get_or_create(&metrics::RpcLabels {
            method: "write_chunk".to_string(),
            status: "ok".to_string(),
        }).inc();
        // Update free slots gauge
        let free = self.store.free_slots();
        self.metrics.free_slots.set(free as i64);

        let resp = WriteChunkResponse { chunk_id };
        Ok(Response::new(resp))
    }

    #[tracing::instrument(skip(self, request), fields(chunk_id = request.get_ref().chunk_id, chunk_bytes_len, checksum, error))]
    async fn read_chunk(
        &self,
        request: Request<ReadChunkRequest>,
    ) -> Result<Response<ReadChunkResponse>, Status> {
        let chunk_id = request.get_ref().chunk_id;
        let span = tracing::Span::current();
        let store = self.store.clone();

        // Offload blocking I/O to the blocking thread pool
        let result = tokio::task::spawn_blocking(move || store.get_chunk(chunk_id))
            .await
            .map_err(|e| Status::new(Code::Internal, format!("task join error: {}", e)))?;

        let (chunk_bytes, checksum) = match result {
            Ok((chunk_bytes, checksum)) => {
                span.record("chunk_bytes_len", chunk_bytes.len());
                span.record("checksum", checksum);
                (chunk_bytes, checksum)
            }
            Err(e) => {
                let return_status = match e.kind() {
                    std::io::ErrorKind::NotFound => {
                        span.record("error", "chunk not found");
                        Status::new(Code::NotFound, "chunk not found")
                    }
                    _ => {
                        span.record("error", "failed to read chunk");
                        Status::new(Code::Internal, "failed to read chunk")
                    }
                };
                return Err(return_status);
            }
        };

        self.metrics.chunks_read_total.inc();
        self.metrics.bytes_read_total.inc_by(chunk_bytes.len() as u64);
        self.metrics.grpc_requests_total.get_or_create(&metrics::RpcLabels {
            method: "read_chunk".to_string(),
            status: "ok".to_string(),
        }).inc();

        let resp = ReadChunkResponse {
            chunk_id,
            chunk_bytes,
            checksum,
        };
        Ok(Response::new(resp))
    }

    #[tracing::instrument(skip(self, request), fields(chunk_id = request.get_ref().chunk_id, error))]
    async fn delete_chunk(
        &self,
        request: Request<DeleteChunkRequest>,
    ) -> Result<Response<DeleteChunkResponse>, Status> {
        let chunk_id = request.get_ref().chunk_id;
        let span = tracing::Span::current();
        let store = self.store.clone();

        // Offload blocking I/O to the blocking thread pool
        let delete_resp =
            tokio::task::spawn_blocking(move || store.delete_chunk(chunk_id))
                .await
                .map_err(|e| Status::new(Code::Internal, format!("task join error: {}", e)))?;

        if delete_resp.is_err() {
            span.record("error", "failed to delete chunk");
            return Err(Status::new(Code::Internal, "failed to delete chunk"));
        }

        self.metrics.chunks_deleted_total.inc();
        self.metrics.grpc_requests_total.get_or_create(&metrics::RpcLabels {
            method: "delete_chunk".to_string(),
            status: "ok".to_string(),
        }).inc();
        // Update free slots gauge
        let free = self.store.free_slots();
        self.metrics.free_slots.set(free as i64);

        let resp = DeleteChunkResponse { chunk_id };
        Ok(Response::new(resp))
    }

    #[tracing::instrument(skip(self, _request), fields(num_free))]
    async fn get_free_chunks(
        &self,
        _request: Request<GetFreeChunksRequest>,
    ) -> Result<Response<GetFreeChunksResponse>, Status> {
        let span = tracing::Span::current();
        // This is a quick in-memory operation, no I/O involved
        let num_free = self.store.free_slots();
        span.record("num_free", num_free);

        let resp = GetFreeChunksResponse { num_free };
        Ok(Response::new(resp))
    }

    #[tracing::instrument(skip(self, _request), fields(chunk_size))]
    async fn get_chunk_size(
        &self,
        _request: Request<GetChunkSizeRequest>,
    ) -> Result<Response<GetChunkSizeResponse>, Status> {
        let span = tracing::Span::current();
        // This is a quick in-memory operation, no I/O involved
        let chunk_size = self.store.get_chunk_size();
        span.record("chunk_size", chunk_size);

        let resp = GetChunkSizeResponse { chunk_size };
        Ok(Response::new(resp))
    }

    #[tracing::instrument(skip(self, _request), fields(healthy))]
    async fn check_health(
        &self,
        _request: Request<CheckHealthRequest>,
    ) -> Result<Response<CheckHealthResponse>, Status> {
        let span = tracing::Span::current();
        span.record("healthy", true);
        Ok(Response::new(CheckHealthResponse { healthy: true }))
    }
}

fn init_tracer() -> SdkTracerProvider {
    global::set_text_map_propagator(TraceContextPropagator::new());

    let otlp_endpoint = std::env::var("OTLP_ENDPOINT")
        .unwrap_or_else(|_| "http://localhost:4317".to_string());

    let exporter = opentelemetry_otlp::SpanExporter::builder()
        .with_tonic()
        .with_endpoint(&otlp_endpoint)
        .build()
        .expect("failed to create OTLP exporter");

    let provider = SdkTracerProvider::builder()
        .with_batch_exporter(exporter)
        .with_resource(Resource::builder()
            .with_attributes([
                KeyValue::new("service.name", env!("CARGO_PKG_NAME")),
                KeyValue::new("service.version", env!("CARGO_PKG_VERSION")),
            ])
            .build())
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

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let tracer_provider = init_tracer();
    init_logs();

    // Initialize metrics
    let mut registry = Registry::default();
    let app_metrics = Arc::new(metrics::Metrics::new(&mut registry));
    let registry = Arc::new(Mutex::new(registry));

    // Load configuration
    let config_path = env::var("CONFIG_PATH").unwrap_or("config.yaml".into());
    let config = match config::StorageNodeConfig::load(&config_path) {
        Ok(cfg) => {
            tracing::info!("loaded configuration from {}", config_path);
            tracing::info!("metadata addresses: {:?}", cfg.metadata_addresses);
            tracing::info!("grpc port: {}", cfg.port);
            tracing::info!("metrics port: {}", cfg.metrics_port);
            cfg
        }
        Err(e) => {
            tracing::error!("failed to load configuration from '{}': {}", config_path, e);
            exit(1);
        }
    };

    // Start metrics HTTP server with configured port
    let metrics_registry = registry.clone();
    let metrics_port = config.metrics_port;
    tokio::spawn(async move {
        let metrics_router = Router::new().route(
            "/metrics",
            get(move || {
                let reg = metrics_registry.clone();
                async move {
                    let registry = reg.lock().unwrap();
                    let body = metrics::encode_metrics(&registry);
                    (
                        [(header::CONTENT_TYPE, "text/plain; version=0.0.4; charset=utf-8")],
                        body,
                    )
                }
            }),
        );
        let metrics_addr = format!("0.0.0.0:{}", metrics_port);
        let listener = TcpListener::bind(&metrics_addr).await.unwrap();
        tracing::info!("metrics server listening on {}", metrics_addr);
        axum::serve(listener, metrics_router).await.unwrap();
    });

    // Connect to metadata service with multi-address support and retry logic
    let mut attempts = 0;
    let max_attempts = 5;
    let connect_timeout = Duration::from_secs(10);

    let metadata_client = 'outer: loop {
        let mut last_error = None;
        for metadata_address in &config.metadata_addresses {
            // Add timeout to connection attempt
            match time::timeout(
                connect_timeout,
                storage_metadata_service_client::StorageMetadataServiceClient::connect(
                    metadata_address.clone(),
                ),
            )
            .await
            {
                Ok(Ok(client)) => {
                    tracing::info!("connected to metadata server at {}", metadata_address);
                    break 'outer client;
                }
                Ok(Err(e)) => {
                    tracing::warn!(
                        "failed to connect to metadata server {}: {}",
                        metadata_address,
                        e
                    );
                    last_error = Some(format!("connection error: {}", e));
                }
                Err(_) => {
                    tracing::warn!(
                        "connection to metadata server {} timed out after {:?}",
                        metadata_address,
                        connect_timeout
                    );
                    last_error = Some(format!("connection timeout after {:?}", connect_timeout));
                }
            }
        }

        if attempts >= max_attempts {
            tracing::error!(
                "could not connect to any metadata service in {} attempts. Last error: {:?}",
                max_attempts,
                last_error
            );
            exit(1);
        }

        attempts += 1;
        tracing::warn!("retrying metadata connection in 2s (attempt {}/{})", attempts, max_attempts);
        time::sleep(Duration::from_secs(2)).await;
    };

    let local_addr = match local_ip() {
        Ok(local_addr) => local_addr,
        Err(e) => {
            tracing::error!("could not get local address. {}", e);
            exit(1);
        }
    };
    tracing::info!("local address: {:?}", local_addr);
    let register_addr = format!("http://{}:{}", local_addr, config.port);

    // Register with metadata service using retry logic
    let retry_config = grpc_utils::RetryConfig {
        max_attempts: 3,
        timeout_per_attempt: Duration::from_secs(5),
        delay_between_attempts: Duration::from_secs(2),
    };

    let register_result = grpc_utils::call_with_retry(
        "register storage node",
        || {
            let mut client = metadata_client.clone();
            let addr = register_addr.clone();
            async move {
                client
                    .put_storage_node(PutStorageNodeRequest { address: addr })
                    .await
            }
        },
        &retry_config,
    )
    .await;

    if let Err(e) = register_result {
        tracing::error!("could not register to metadata service: {}", e);
        exit(1);
    }

    tracing::info!("successfully registered with metadata service");

    let addr: std::net::SocketAddr = format!("0.0.0.0:{}", config.port)
        .parse()
        .expect("valid socket address");
    let storage_node = StorageNodeImpl::construct(
        app_metrics.clone(),
        &config.chunk_store_path,
        config.allocated_slots,
        config.chunk_store_options(),
    )?;
    let storage_node_svc = StorageNodeServiceServer::new(storage_node)
        .max_decoding_message_size(MAX_GRPC_MESSAGE_SIZE)
        .max_encoding_message_size(MAX_GRPC_MESSAGE_SIZE);

    tracing::info!(message = format!("listening on {}", addr));

    let server_result = Server::builder()
        .accept_http1(true)
        .add_service(tonic_web::enable(storage_node_svc))
        .serve(addr)
        .await;

    tracer_provider
        .shutdown()
        .expect("shutdown tracer provider failed");

    server_result?;
    Ok(())
}
