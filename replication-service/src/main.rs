mod clients;
mod metrics;

use opentelemetry::{global, KeyValue};
use opentelemetry_otlp::WithExportConfig;
use opentelemetry_sdk::{propagation::TraceContextPropagator, trace::SdkTracerProvider, Resource};
use prometheus_client::registry::Registry;
use rand::seq::IndexedRandom;
use std::sync::{Arc, Mutex};
use std::time::Instant;
use tokio::net::TcpListener;
use tokio::time::{self, Duration};
use tracing;
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

use axum::{http::header, routing::get, Router};
use storage_proto_lib::storage_metadata::{
    storage_metadata_service_client::StorageMetadataServiceClient,
    AddChunkReplicaRequest, GetStorageNodeRequest, ListStorageNodesRequest,
    ListUnderReplicatedChunksRequest,
};
use storage_proto_lib::storage_node::{
    DeleteChunkRequest as StorageDeleteChunkRequest,
    GetFreeChunksRequest, ReadChunkRequest, WriteChunkRequest,
};
use storage_proto_lib::{MetadataClient, StorageNodeClient, StorageNodeClientFactory};

const DEFAULT_REPLICATION_INTERVAL_SECS: u64 = 60;
const DEFAULT_MAX_CHUNKS_PER_CYCLE: usize = 100;
const MAX_REPLICAS: usize = 3;

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

#[tracing::instrument(skip(metadata_client, storage_factory, app_metrics), fields(under_replicated_count, replicated_count, skipped_count))]
async fn run_replication_cycle<M, F>(
    metadata_client: &mut M,
    storage_factory: &F,
    app_metrics: &Arc<metrics::Metrics>,
    max_chunks_per_cycle: usize,
)
where
    M: MetadataClient,
    F: StorageNodeClientFactory,
{
    let start = Instant::now();
    let span = tracing::Span::current();

    app_metrics.replication_runs_total.inc();

    // 1. Get under-replicated chunks (< 3 replicas)
    let chunks = match metadata_client
        .list_under_replicated_chunks(ListUnderReplicatedChunksRequest {
            max_replicas: MAX_REPLICAS as u32,
        })
        .await
    {
        Ok(resp) => resp.into_inner().chunks,
        Err(e) => {
            tracing::error!("Failed to list under-replicated chunks: {}", e);
            app_metrics.inc_failure("list_chunks");
            return;
        }
    };

    let total_under_replicated = chunks.len();
    app_metrics
        .under_replicated_chunks
        .set(total_under_replicated as i64);
    span.record("under_replicated_count", total_under_replicated);

    if chunks.is_empty() {
        tracing::info!("No under-replicated chunks found");
        app_metrics
            .replication_duration_seconds
            .observe(start.elapsed().as_secs_f64());
        return;
    }

    // 2. Get available storage nodes
    let nodes = match metadata_client
        .list_storage_nodes(ListStorageNodesRequest {})
        .await
    {
        Ok(resp) => resp.into_inner().storage_nodes,
        Err(e) => {
            tracing::error!("Failed to list storage nodes: {}", e);
            app_metrics.inc_failure("list_nodes");
            return;
        }
    };

    if nodes.is_empty() {
        tracing::warn!("No storage nodes available for replication");
        return;
    }

    // 3. Apply rate limit (chunks are already sorted by replica count from metadata service)
    let chunks_to_process: Vec<_> = chunks.into_iter().take(max_chunks_per_cycle).collect();
    let skipped = total_under_replicated.saturating_sub(max_chunks_per_cycle);
    if skipped > 0 {
        app_metrics.chunks_skipped_rate_limit.inc_by(skipped as u64);
    }
    span.record("skipped_count", skipped);

    let mut replicated_count = 0usize;

    for chunk in chunks_to_process {
        // 4. Find eligible nodes (not already hosting this chunk)
        let current_node_ids: Vec<u64> = chunk.current_storage_node_ids.clone();
        let eligible_nodes: Vec<_> = nodes
            .iter()
            .filter(|n| !current_node_ids.contains(&n.id))
            .collect();

        if eligible_nodes.is_empty() {
            tracing::debug!(
                "No eligible nodes for chunk {} (already on {} nodes)",
                chunk.chunk_id,
                current_node_ids.len()
            );
            continue;
        }

        // 5. Pick a target node (random selection)
        let mut rng = rand::rng();
        let target_node = match eligible_nodes.choose(&mut rng) {
            Some(node) => *node,
            None => continue,
        };

        // Check if target node has free space
        let target_address = target_node.address.clone();
        let has_space = match storage_factory.connect(&target_address).await {
            Ok(mut client) => {
                match client.get_free_chunks(GetFreeChunksRequest {}).await {
                    Ok(resp) => resp.into_inner().num_free > 0,
                    Err(_) => false,
                }
            }
            Err(_) => false,
        };

        if !has_space {
            tracing::debug!(
                "Target node {} has no free space, skipping",
                target_node.id
            );
            continue;
        }

        // 6. Read chunk from existing replica
        let source_node_id = match current_node_ids.first() {
            Some(id) => *id,
            None => {
                tracing::warn!("Chunk {} has no source nodes", chunk.chunk_id);
                continue;
            }
        };

        // Get source node address
        let source_address = match metadata_client
            .get_storage_node(GetStorageNodeRequest { id: source_node_id })
            .await
        {
            Ok(resp) => resp.into_inner().address,
            Err(e) => {
                tracing::warn!(
                    "Failed to get source node {} address: {}",
                    source_node_id,
                    e
                );
                app_metrics.inc_failure("connect_source");
                continue;
            }
        };

        let chunk_data = match storage_factory.connect(&source_address).await {
            Ok(mut client) => {
                match client
                    .read_chunk(ReadChunkRequest {
                        chunk_id: chunk.chunk_id,
                    })
                    .await
                {
                    Ok(resp) => resp.into_inner().chunk_bytes,
                    Err(e) => {
                        tracing::warn!(
                            "Failed to read chunk {} from node {}: {}",
                            chunk.chunk_id,
                            source_node_id,
                            e
                        );
                        app_metrics.inc_failure("read_chunk");
                        continue;
                    }
                }
            }
            Err(e) => {
                tracing::warn!("Failed to connect to source node {}: {}", source_address, e);
                app_metrics.inc_failure("connect_source");
                continue;
            }
        };

        // 6b. Verify checksum of received data
        let computed_checksum = {
            let mut hasher = crc32fast::Hasher::new();
            hasher.update(&chunk_data);
            hasher.finalize()
        };
        if computed_checksum != chunk.checksum {
            tracing::error!(
                chunk_id = chunk.chunk_id,
                source_node_id = source_node_id,
                expected_checksum = chunk.checksum,
                computed_checksum = computed_checksum,
                "Checksum mismatch during replication, skipping chunk"
            );
            app_metrics.inc_failure("checksum_mismatch");
            continue;
        }

        // 7. Write to target node
        match storage_factory.connect(&target_address).await {
            Ok(mut client) => {
                if let Err(e) = client
                    .write_chunk(WriteChunkRequest {
                        chunk_id: chunk.chunk_id,
                        chunk_bytes: chunk_data,
                    })
                    .await
                {
                    tracing::warn!(
                        "Failed to write chunk {} to node {}: {}",
                        chunk.chunk_id,
                        target_node.id,
                        e
                    );
                    app_metrics.inc_failure("write_chunk");
                    continue;
                }
            }
            Err(e) => {
                tracing::warn!(
                    "Failed to connect to target node {}: {}",
                    target_address,
                    e
                );
                app_metrics.inc_failure("connect_target");
                continue;
            }
        }

        // 8. Update metadata
        if let Err(e) = metadata_client
            .add_chunk_replica(AddChunkReplicaRequest {
                chunk_id: chunk.chunk_id,
                storage_node_id: target_node.id,
            })
            .await
        {
            tracing::warn!(
                "Failed to update metadata for chunk {} replica on node {}: {}",
                chunk.chunk_id,
                target_node.id,
                e
            );
            // Attempt cleanup of orphaned chunk on target node
            if let Ok(mut cleanup_client) = storage_factory.connect(&target_address).await {
                let _ = cleanup_client
                    .delete_chunk(StorageDeleteChunkRequest {
                        chunk_id: chunk.chunk_id,
                    })
                    .await;
            }
            app_metrics.inc_failure("update_metadata");
            continue;
        }

        tracing::info!(
            "Replicated chunk {} to node {} (now has {} replicas)",
            chunk.chunk_id,
            target_node.id,
            current_node_ids.len() + 1
        );
        replicated_count += 1;
        app_metrics.chunks_replicated_total.inc();
    }

    span.record("replicated_count", replicated_count);
    app_metrics
        .replication_duration_seconds
        .observe(start.elapsed().as_secs_f64());

    tracing::info!(
        "Replication cycle complete: {} chunks replicated, {} under-replicated remaining",
        replicated_count,
        total_under_replicated.saturating_sub(replicated_count)
    );
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let _tracer_provider = init_tracer();
    init_logs();

    // Parse configuration from environment
    let replication_interval_secs: u64 = std::env::var("REPLICATION_INTERVAL_SECS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(DEFAULT_REPLICATION_INTERVAL_SECS);

    let max_chunks_per_cycle: usize = std::env::var("REPLICATION_MAX_CHUNKS_PER_CYCLE")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(DEFAULT_MAX_CHUNKS_PER_CYCLE);

    // Support METADATA_URLS (comma-separated) with fallback to METADATA_URL
    let metadata_urls: Vec<String> = std::env::var("METADATA_URLS")
        .map(|s| s.split(',').map(|u| u.trim().to_string()).filter(|u| !u.is_empty()).collect())
        .unwrap_or_else(|_| {
            vec![std::env::var("METADATA_URL").unwrap_or_else(|_| "http://localhost:3001".to_string())]
        });

    tracing::info!(
        "Starting replication service with interval={}s, max_chunks_per_cycle={}, metadata_urls={:?}",
        replication_interval_secs,
        max_chunks_per_cycle,
        metadata_urls
    );

    // Initialize metrics
    let mut registry = Registry::default();
    let app_metrics = Arc::new(metrics::Metrics::new(&mut registry));
    let registry = Arc::new(Mutex::new(registry));

    // Start metrics HTTP server on port 9093
    let metrics_registry = registry.clone();
    tokio::spawn(async move {
        let metrics_router = Router::new().route(
            "/metrics",
            get(move || {
                let reg = metrics_registry.clone();
                async move {
                    let registry = reg.lock().unwrap();
                    let body = metrics::encode_metrics(&registry);
                    (
                        [(
                            header::CONTENT_TYPE,
                            "text/plain; version=0.0.4; charset=utf-8",
                        )],
                        body,
                    )
                }
            }),
        );
        let listener = TcpListener::bind("0.0.0.0:9093").await.unwrap();
        tracing::info!("Metrics server listening on 0.0.0.0:9093");
        axum::serve(listener, metrics_router).await.unwrap();
    });

    // Connect to metadata service with retry and failover
    let mut metadata_client = {
        let mut attempts = 0;
        'connect: loop {
            for url in &metadata_urls {
                match StorageMetadataServiceClient::connect(url.clone()).await {
                    Ok(client) => {
                        tracing::info!("Connected to metadata service at {}", url);
                        break 'connect clients::RealMetadataClient::new(client);
                    }
                    Err(e) => {
                        tracing::warn!(
                            "Failed to connect to metadata service at {} (attempt {}): {}",
                            url,
                            attempts + 1,
                            e
                        );
                    }
                }
            }
            attempts += 1;
            if attempts >= 5 {
                tracing::error!(
                    "Failed to connect to any metadata service after {} attempts",
                    attempts,
                );
                return Err("failed to connect to metadata service".into());
            }
            time::sleep(Duration::from_secs(2)).await;
        }
    };

    let storage_factory = clients::RealStorageNodeClientFactory;

    // Main replication loop
    let mut interval = time::interval(Duration::from_secs(replication_interval_secs));
    let mut consecutive_failures: u32 = 0;
    const MAX_CONSECUTIVE_FAILURES: u32 = 3;

    loop {
        interval.tick().await;

        // If we've had too many consecutive failures, try to reconnect
        if consecutive_failures >= MAX_CONSECUTIVE_FAILURES {
            tracing::warn!(
                "Reconnecting to metadata service after {} consecutive failures",
                consecutive_failures
            );
            let mut reconnected = false;
            for url in &metadata_urls {
                match StorageMetadataServiceClient::connect(url.clone()).await {
                    Ok(new_client) => {
                        metadata_client = clients::RealMetadataClient::new(new_client);
                        consecutive_failures = 0;
                        tracing::info!("Reconnected to metadata service at {}", url);
                        reconnected = true;
                        break;
                    }
                    Err(e) => {
                        tracing::warn!("Failed to reconnect to metadata service at {}: {}", url, e);
                    }
                }
            }
            if !reconnected {
                tracing::error!("Failed to reconnect to any metadata service");
                app_metrics.inc_failure("connect_metadata");
                continue;
            }
        }

        tracing::info!("Starting replication cycle");

        // Probe the connection before running a full cycle
        let probe = metadata_client
            .list_storage_nodes(ListStorageNodesRequest {})
            .await;
        if probe.is_err() {
            consecutive_failures += 1;
            tracing::warn!(
                "Metadata service probe failed (consecutive failures: {}): {}",
                consecutive_failures,
                probe.unwrap_err()
            );
            app_metrics.inc_failure("connect_metadata");
            continue;
        }
        consecutive_failures = 0;

        run_replication_cycle(&mut metadata_client, &storage_factory, &app_metrics, max_chunks_per_cycle).await;
    }

}

#[cfg(test)]
mod tests {
    use super::*;
    use async_trait::async_trait;
    use prometheus_client::registry::Registry;
    use std::collections::HashMap;
    use std::sync::Mutex as StdMutex;
    use storage_proto_lib::storage_metadata::{
        AllocateChunkRequest, AllocateChunkResponse,
        ConfirmChunkRequest, ConfirmChunkResponse,
        DeleteChunkRequest as MetadataDeleteChunkRequest,
        DeleteChunkResponse as MetadataDeleteChunkResponse,
        DeleteObjectRequest, DeleteObjectResponse,
        DeleteStorageNodeRequest, DeleteStorageNodeResponse,
        GetChunkRequest, GetChunkResponse,
        GetObjectRequest, GetObjectResponse,
        GetStorageNodeRequest, GetStorageNodeResponse,
        ListObjectsRequest, ListObjectsResponse,
        ListStorageNodesResponse,
        ListUnderReplicatedChunksResponse,
        MetaStorageNode, UnderReplicatedChunk,
        PutChunkRequest, PutChunkResponse,
        PutObjectRequest, PutObjectResponse,
        PutStorageNodeRequest, PutStorageNodeResponse,
    };
    use storage_proto_lib::storage_node::{
        CheckHealthRequest, CheckHealthResponse,
        DeleteChunkRequest as StorageDeleteChunkRequest,
        DeleteChunkResponse as StorageDeleteChunkResponse,
        GetChunkSizeRequest, GetChunkSizeResponse,
        GetFreeChunksResponse,
        ReadChunkResponse,
        WriteChunkResponse,
    };
    use tonic::{Response, Status};

    #[derive(Clone)]
    struct MockMetadataClient {
        storage_nodes: Vec<MetaStorageNode>,
        under_replicated_chunks: Vec<UnderReplicatedChunk>,
        node_addresses: HashMap<u64, String>,
        replicas_added: Arc<StdMutex<Vec<(u64, u64)>>>,
    }

    impl Default for MockMetadataClient {
        fn default() -> Self {
            Self {
                storage_nodes: vec![
                    MetaStorageNode { id: 1, address: "http://node1:3000".to_string() },
                    MetaStorageNode { id: 2, address: "http://node2:3000".to_string() },
                ],
                under_replicated_chunks: Vec::new(),
                node_addresses: HashMap::from([
                    (1, "http://node1:3000".to_string()),
                    (2, "http://node2:3000".to_string()),
                ]),
                replicas_added: Arc::new(StdMutex::new(Vec::new())),
            }
        }
    }

    #[async_trait]
    impl MetadataClient for MockMetadataClient {
        async fn list_storage_nodes(&mut self, _request: ListStorageNodesRequest) -> Result<Response<ListStorageNodesResponse>, Status> {
            Ok(Response::new(ListStorageNodesResponse {
                storage_nodes: self.storage_nodes.clone(),
            }))
        }
        async fn list_under_replicated_chunks(&mut self, _request: ListUnderReplicatedChunksRequest) -> Result<Response<ListUnderReplicatedChunksResponse>, Status> {
            Ok(Response::new(ListUnderReplicatedChunksResponse {
                chunks: self.under_replicated_chunks.clone(),
            }))
        }
        async fn get_storage_node(&mut self, request: GetStorageNodeRequest) -> Result<Response<GetStorageNodeResponse>, Status> {
            match self.node_addresses.get(&request.id) {
                Some(address) => Ok(Response::new(GetStorageNodeResponse { address: address.clone() })),
                None => Err(Status::not_found("node not found")),
            }
        }
        async fn add_chunk_replica(&mut self, request: AddChunkReplicaRequest) -> Result<Response<storage_proto_lib::storage_metadata::AddChunkReplicaResponse>, Status> {
            self.replicas_added.lock().unwrap().push((request.chunk_id, request.storage_node_id));
            Ok(Response::new(storage_proto_lib::storage_metadata::AddChunkReplicaResponse {}))
        }
        // Unused methods - required by trait
        async fn put_object(&mut self, _: PutObjectRequest) -> Result<Response<PutObjectResponse>, Status> { unimplemented!() }
        async fn get_object(&mut self, _: GetObjectRequest) -> Result<Response<GetObjectResponse>, Status> { unimplemented!() }
        async fn delete_object(&mut self, _: DeleteObjectRequest) -> Result<Response<DeleteObjectResponse>, Status> { unimplemented!() }
        async fn allocate_chunk(&mut self, _: AllocateChunkRequest) -> Result<Response<AllocateChunkResponse>, Status> { unimplemented!() }
        async fn confirm_chunk(&mut self, _: ConfirmChunkRequest) -> Result<Response<ConfirmChunkResponse>, Status> { unimplemented!() }
        async fn list_objects(&mut self, _: ListObjectsRequest) -> Result<Response<ListObjectsResponse>, Status> { unimplemented!() }
        async fn delete_chunk(&mut self, _: MetadataDeleteChunkRequest) -> Result<Response<MetadataDeleteChunkResponse>, Status> { unimplemented!() }
        async fn put_chunk(&mut self, _: PutChunkRequest) -> Result<Response<PutChunkResponse>, Status> { unimplemented!() }
        async fn put_storage_node(&mut self, _: PutStorageNodeRequest) -> Result<Response<PutStorageNodeResponse>, Status> { unimplemented!() }
        async fn get_chunk(&mut self, _: GetChunkRequest) -> Result<Response<GetChunkResponse>, Status> { unimplemented!() }
        async fn delete_storage_node(&mut self, _: DeleteStorageNodeRequest) -> Result<Response<DeleteStorageNodeResponse>, Status> { unimplemented!() }
        async fn update_object_checksum(&mut self, _: storage_proto_lib::storage_metadata::UpdateObjectChecksumRequest) -> Result<Response<storage_proto_lib::storage_metadata::UpdateObjectChecksumResponse>, Status> { unimplemented!() }
    }

    #[derive(Clone)]
    struct MockStorageNodeClient {
        chunks: Arc<StdMutex<HashMap<u64, Vec<u8>>>>,
        free_slots: u64,
    }

    #[async_trait]
    impl StorageNodeClient for MockStorageNodeClient {
        async fn get_free_chunks(&mut self, _request: GetFreeChunksRequest) -> Result<Response<GetFreeChunksResponse>, Status> {
            Ok(Response::new(GetFreeChunksResponse { num_free: self.free_slots }))
        }
        async fn read_chunk(&mut self, request: ReadChunkRequest) -> Result<Response<ReadChunkResponse>, Status> {
            let chunks = self.chunks.lock().unwrap();
            match chunks.get(&request.chunk_id) {
                Some(data) => Ok(Response::new(ReadChunkResponse {
                    chunk_id: request.chunk_id,
                    chunk_bytes: data.clone(),
                    checksum: 0,
                })),
                None => Err(Status::not_found("chunk not found")),
            }
        }
        async fn write_chunk(&mut self, request: WriteChunkRequest) -> Result<Response<WriteChunkResponse>, Status> {
            self.chunks.lock().unwrap().insert(request.chunk_id, request.chunk_bytes);
            Ok(Response::new(WriteChunkResponse { chunk_id: request.chunk_id }))
        }
        async fn delete_chunk(&mut self, request: StorageDeleteChunkRequest) -> Result<Response<StorageDeleteChunkResponse>, Status> {
            self.chunks.lock().unwrap().remove(&request.chunk_id);
            Ok(Response::new(StorageDeleteChunkResponse { chunk_id: request.chunk_id }))
        }
        async fn get_chunk_size(&mut self, _: GetChunkSizeRequest) -> Result<Response<GetChunkSizeResponse>, Status> { unimplemented!() }
        async fn check_health(&mut self, _: CheckHealthRequest) -> Result<Response<CheckHealthResponse>, Status> { unimplemented!() }
    }

    #[derive(Clone)]
    struct MockStorageNodeClientFactory {
        /// Shared chunk store across all "nodes"
        chunks: Arc<StdMutex<HashMap<u64, Vec<u8>>>>,
        free_slots: u64,
    }

    impl Default for MockStorageNodeClientFactory {
        fn default() -> Self {
            Self {
                chunks: Arc::new(StdMutex::new(HashMap::new())),
                free_slots: 10,
            }
        }
    }

    #[async_trait]
    impl StorageNodeClientFactory for MockStorageNodeClientFactory {
        type Client = MockStorageNodeClient;

        async fn connect(&self, _address: &str) -> Result<Self::Client, tonic::transport::Error> {
            Ok(MockStorageNodeClient {
                chunks: self.chunks.clone(),
                free_slots: self.free_slots,
            })
        }
    }

    fn create_test_metrics() -> Arc<metrics::Metrics> {
        let mut registry = Registry::default();
        Arc::new(metrics::Metrics::new(&mut registry))
    }

    #[tokio::test]
    async fn test_no_under_replicated_chunks() {
        let mut metadata = MockMetadataClient::default();
        let factory = MockStorageNodeClientFactory::default();
        let app_metrics = create_test_metrics();

        run_replication_cycle(&mut metadata, &factory, &app_metrics, 100).await;

        assert_eq!(app_metrics.replication_runs_total.get(), 1);
        assert_eq!(app_metrics.chunks_replicated_total.get(), 0);
    }

    #[tokio::test]
    async fn test_replicates_under_replicated_chunk() {
        let chunks = Arc::new(StdMutex::new(HashMap::from([
            (42, b"hello world".to_vec()),
        ])));

        let mut metadata = MockMetadataClient {
            under_replicated_chunks: vec![UnderReplicatedChunk {
                chunk_id: 42,
                checksum: 222957957, // CRC32 of "hello world"
                current_storage_node_ids: vec![1],
            }],
            ..Default::default()
        };

        let factory = MockStorageNodeClientFactory {
            chunks: chunks.clone(),
            free_slots: 10,
        };
        let app_metrics = create_test_metrics();

        run_replication_cycle(&mut metadata, &factory, &app_metrics, 100).await;

        assert_eq!(app_metrics.chunks_replicated_total.get(), 1);
        let replicas = metadata.replicas_added.lock().unwrap();
        assert_eq!(replicas.len(), 1);
        assert_eq!(replicas[0].0, 42); // chunk_id
        assert_eq!(replicas[0].1, 2);  // target node (only node 2 is eligible)
    }

    #[tokio::test]
    async fn test_skips_chunk_when_no_eligible_nodes() {
        let mut metadata = MockMetadataClient {
            storage_nodes: vec![
                MetaStorageNode { id: 1, address: "http://node1:3000".to_string() },
            ],
            under_replicated_chunks: vec![UnderReplicatedChunk {
                chunk_id: 42,
                checksum: 12345,
                current_storage_node_ids: vec![1], // already on the only node
            }],
            ..Default::default()
        };

        let factory = MockStorageNodeClientFactory::default();
        let app_metrics = create_test_metrics();

        run_replication_cycle(&mut metadata, &factory, &app_metrics, 100).await;

        assert_eq!(app_metrics.chunks_replicated_total.get(), 0);
    }

    #[tokio::test]
    async fn test_skips_chunk_when_no_free_space() {
        let chunks = Arc::new(StdMutex::new(HashMap::from([
            (42, b"data".to_vec()),
        ])));

        let mut metadata = MockMetadataClient {
            under_replicated_chunks: vec![UnderReplicatedChunk {
                chunk_id: 42,
                checksum: 2918445923, // CRC32 of "data"
                current_storage_node_ids: vec![1],
            }],
            ..Default::default()
        };

        let factory = MockStorageNodeClientFactory {
            chunks,
            free_slots: 0, // no free space
        };
        let app_metrics = create_test_metrics();

        run_replication_cycle(&mut metadata, &factory, &app_metrics, 100).await;

        assert_eq!(app_metrics.chunks_replicated_total.get(), 0);
    }

    #[tokio::test]
    async fn test_rate_limiting_respects_max_chunks_per_cycle() {
        let chunks = Arc::new(StdMutex::new(HashMap::from([
            (1, b"data1".to_vec()),
            (2, b"data2".to_vec()),
            (3, b"data3".to_vec()),
        ])));

        let mut metadata = MockMetadataClient {
            under_replicated_chunks: vec![
                UnderReplicatedChunk { chunk_id: 1, checksum: 1472867494, current_storage_node_ids: vec![1] }, // CRC32 of "data1"
                UnderReplicatedChunk { chunk_id: 2, checksum: 3468918044, current_storage_node_ids: vec![1] }, // CRC32 of "data2"
                UnderReplicatedChunk { chunk_id: 3, checksum: 3116649866, current_storage_node_ids: vec![1] }, // CRC32 of "data3"
            ],
            ..Default::default()
        };

        let factory = MockStorageNodeClientFactory {
            chunks,
            free_slots: 10,
        };
        let app_metrics = create_test_metrics();

        // Only allow 1 chunk per cycle
        run_replication_cycle(&mut metadata, &factory, &app_metrics, 1).await;

        assert_eq!(app_metrics.chunks_replicated_total.get(), 1);
        // 2 chunks should have been skipped due to rate limit
        assert_eq!(app_metrics.chunks_skipped_rate_limit.get(), 2);
    }

    #[tokio::test]
    async fn test_no_storage_nodes_available() {
        let mut metadata = MockMetadataClient {
            storage_nodes: vec![], // no nodes
            under_replicated_chunks: vec![UnderReplicatedChunk {
                chunk_id: 42,
                checksum: 12345,
                current_storage_node_ids: vec![1],
            }],
            ..Default::default()
        };

        let factory = MockStorageNodeClientFactory::default();
        let app_metrics = create_test_metrics();

        run_replication_cycle(&mut metadata, &factory, &app_metrics, 100).await;

        assert_eq!(app_metrics.chunks_replicated_total.get(), 0);
    }

    #[tokio::test]
    async fn test_chunk_with_no_source_nodes_skipped() {
        let mut metadata = MockMetadataClient {
            under_replicated_chunks: vec![UnderReplicatedChunk {
                chunk_id: 42,
                checksum: 12345,
                current_storage_node_ids: vec![], // no source nodes
            }],
            ..Default::default()
        };

        let factory = MockStorageNodeClientFactory::default();
        let app_metrics = create_test_metrics();

        run_replication_cycle(&mut metadata, &factory, &app_metrics, 100).await;

        assert_eq!(app_metrics.chunks_replicated_total.get(), 0);
    }

    #[tokio::test]
    async fn test_metrics_updated_correctly() {
        let app_metrics = create_test_metrics();
        let mut metadata = MockMetadataClient::default();
        let factory = MockStorageNodeClientFactory::default();

        // Run with no work to do
        run_replication_cycle(&mut metadata, &factory, &app_metrics, 100).await;

        assert_eq!(app_metrics.replication_runs_total.get(), 1);
        assert_eq!(app_metrics.under_replicated_chunks.get(), 0);
        // No failures should have been recorded for any error type
    }
}
