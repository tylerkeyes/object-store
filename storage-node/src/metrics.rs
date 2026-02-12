use prometheus_client::encoding::{EncodeLabelSet, text::encode};
use prometheus_client::metrics::counter::Counter;
use prometheus_client::metrics::family::Family;
use prometheus_client::metrics::gauge::Gauge;
use prometheus_client::metrics::histogram::Histogram;
use prometheus_client::registry::Registry;

#[derive(Clone, Debug, Hash, PartialEq, Eq, EncodeLabelSet)]
pub struct RpcLabels {
    pub method: String,
    pub status: String,
}

#[derive(Clone, Debug, Hash, PartialEq, Eq, EncodeLabelSet)]
pub struct OperationLabels {
    pub operation: String,
}

pub struct Metrics {
    pub grpc_requests_total: Family<RpcLabels, Counter>,
    pub chunks_written_total: Counter,
    pub chunks_read_total: Counter,
    pub chunks_deleted_total: Counter,
    pub bytes_written_total: Counter,
    pub bytes_read_total: Counter,
    pub free_slots: Gauge,
    pub operation_duration_seconds: Family<OperationLabels, Histogram>,
}

impl Metrics {
    pub fn new(registry: &mut Registry) -> Self {
        let grpc_requests_total = Family::<RpcLabels, Counter>::default();
        let chunks_written_total = Counter::default();
        let chunks_read_total = Counter::default();
        let chunks_deleted_total = Counter::default();
        let bytes_written_total = Counter::default();
        let bytes_read_total = Counter::default();
        let free_slots = Gauge::default();
        let operation_duration_seconds =
            Family::<OperationLabels, Histogram>::new_with_constructor(|| {
                Histogram::new([0.0001, 0.0005, 0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0].into_iter())
            });

        registry.register(
            "storage_node_grpc_requests",
            "Total gRPC requests by method and status",
            grpc_requests_total.clone(),
        );
        registry.register(
            "storage_node_chunks_written",
            "Total chunks written",
            chunks_written_total.clone(),
        );
        registry.register(
            "storage_node_chunks_read",
            "Total chunks read",
            chunks_read_total.clone(),
        );
        registry.register(
            "storage_node_chunks_deleted",
            "Total chunks deleted",
            chunks_deleted_total.clone(),
        );
        registry.register(
            "storage_node_bytes_written",
            "Total bytes written",
            bytes_written_total.clone(),
        );
        registry.register(
            "storage_node_bytes_read",
            "Total bytes read",
            bytes_read_total.clone(),
        );
        registry.register(
            "storage_node_free_slots",
            "Number of free chunk slots",
            free_slots.clone(),
        );
        registry.register(
            "storage_node_operation_duration_seconds",
            "Operation duration in seconds",
            operation_duration_seconds.clone(),
        );

        Metrics {
            grpc_requests_total,
            chunks_written_total,
            chunks_read_total,
            chunks_deleted_total,
            bytes_written_total,
            bytes_read_total,
            free_slots,
            operation_duration_seconds,
        }
    }
}

pub fn encode_metrics(registry: &Registry) -> String {
    let mut buffer = String::new();
    encode(&mut buffer, registry).expect("failed to encode metrics");
    buffer
}
