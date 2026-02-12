use prometheus_client::encoding::{EncodeLabelSet, text::encode};
use prometheus_client::metrics::counter::Counter;
use prometheus_client::metrics::family::Family;
use prometheus_client::metrics::gauge::Gauge;
use prometheus_client::metrics::histogram::Histogram;
use prometheus_client::registry::Registry;

#[derive(Clone, Debug, Hash, PartialEq, Eq, EncodeLabelSet)]
pub struct FailureLabels {
    pub error_type: String,
}

pub struct Metrics {
    pub replication_runs_total: Counter,
    pub chunks_replicated_total: Counter,
    pub replication_failures: Family<FailureLabels, Counter>,
    pub replication_duration_seconds: Histogram,
    pub under_replicated_chunks: Gauge,
    pub chunks_skipped_rate_limit: Counter,
}

impl Metrics {
    pub fn new(registry: &mut Registry) -> Self {
        let replication_runs_total = Counter::default();
        let chunks_replicated_total = Counter::default();
        let replication_failures = Family::<FailureLabels, Counter>::default();
        let replication_duration_seconds = Histogram::new(
            [0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1.0, 5.0, 10.0, 30.0, 60.0].into_iter(),
        );
        let under_replicated_chunks = Gauge::default();
        let chunks_skipped_rate_limit = Counter::default();

        registry.register(
            "replication_runs",
            "Total replication cycles run",
            replication_runs_total.clone(),
        );
        registry.register(
            "replication_chunks_replicated",
            "Total chunks successfully replicated",
            chunks_replicated_total.clone(),
        );
        registry.register(
            "replication_failures",
            "Total replication failures by error type",
            replication_failures.clone(),
        );
        registry.register(
            "replication_duration_seconds",
            "Duration of replication cycles in seconds",
            replication_duration_seconds.clone(),
        );
        registry.register(
            "replication_under_replicated_chunks",
            "Current count of under-replicated chunks",
            under_replicated_chunks.clone(),
        );
        registry.register(
            "replication_chunks_skipped_rate_limit",
            "Chunks skipped due to rate limit",
            chunks_skipped_rate_limit.clone(),
        );

        Metrics {
            replication_runs_total,
            chunks_replicated_total,
            replication_failures,
            replication_duration_seconds,
            under_replicated_chunks,
            chunks_skipped_rate_limit,
        }
    }

    /// Increment failure counter for a specific error type.
    pub fn inc_failure(&self, error_type: &str) {
        self.replication_failures
            .get_or_create(&FailureLabels {
                error_type: error_type.to_string(),
            })
            .inc();
    }
}

pub fn encode_metrics(registry: &Registry) -> String {
    let mut buffer = String::new();
    encode(&mut buffer, registry).expect("failed to encode metrics");
    buffer
}
