use prometheus_client::encoding::{EncodeLabelSet, text::encode};
use prometheus_client::metrics::counter::Counter;
use prometheus_client::metrics::family::Family;
use prometheus_client::metrics::gauge::Gauge;
use prometheus_client::registry::Registry;

#[derive(Clone, Debug, Hash, PartialEq, Eq, EncodeLabelSet)]
pub struct RpcLabels {
    pub method: String,
    pub status: String,
}

pub struct Metrics {
    pub grpc_requests_total: Family<RpcLabels, Counter>,
    pub objects_total: Gauge,
    pub chunks_total: Gauge,
    pub storage_nodes_total: Gauge,
    pub health_checks_total: Counter,
    pub health_checks_failed_total: Counter,
}

impl Metrics {
    pub fn new(registry: &mut Registry) -> Self {
        let grpc_requests_total = Family::<RpcLabels, Counter>::default();
        let objects_total = Gauge::default();
        let chunks_total = Gauge::default();
        let storage_nodes_total = Gauge::default();
        let health_checks_total = Counter::default();
        let health_checks_failed_total = Counter::default();

        registry.register(
            "metadata_grpc_requests",
            "Total gRPC requests by method and status",
            grpc_requests_total.clone(),
        );
        registry.register(
            "metadata_objects",
            "Current number of objects stored",
            objects_total.clone(),
        );
        registry.register(
            "metadata_chunks",
            "Current number of chunks stored",
            chunks_total.clone(),
        );
        registry.register(
            "metadata_storage_nodes",
            "Current number of registered storage nodes",
            storage_nodes_total.clone(),
        );
        registry.register(
            "metadata_health_checks",
            "Total health checks performed",
            health_checks_total.clone(),
        );
        registry.register(
            "metadata_health_checks_failed",
            "Total failed health checks",
            health_checks_failed_total.clone(),
        );

        Metrics {
            grpc_requests_total,
            objects_total,
            chunks_total,
            storage_nodes_total,
            health_checks_total,
            health_checks_failed_total,
        }
    }
}

pub fn encode_metrics(registry: &Registry) -> String {
    let mut buffer = String::new();
    encode(&mut buffer, registry).expect("failed to encode metrics");
    buffer
}
