use prometheus_client::encoding::{EncodeLabelSet, text::encode};
use prometheus_client::metrics::counter::Counter;
use prometheus_client::metrics::family::Family;
use prometheus_client::metrics::histogram::Histogram;
use prometheus_client::registry::Registry;

#[derive(Clone, Debug, Hash, PartialEq, Eq, EncodeLabelSet)]
pub struct RequestLabels {
    pub method: String,
    pub endpoint: String,
    pub status: String,
}

#[derive(Clone, Debug, Hash, PartialEq, Eq, EncodeLabelSet)]
pub struct OperationLabels {
    pub operation: String,
}

pub struct Metrics {
    pub http_requests_total: Family<RequestLabels, Counter>,
    pub http_request_duration_seconds: Family<OperationLabels, Histogram>,
    pub objects_uploaded_total: Counter,
    pub objects_downloaded_total: Counter,
    pub objects_deleted_total: Counter,
    pub bytes_uploaded_total: Counter,
    pub bytes_downloaded_total: Counter,
}

impl Metrics {
    pub fn new(registry: &mut Registry) -> Self {
        let http_requests_total = Family::<RequestLabels, Counter>::default();
        let http_request_duration_seconds =
            Family::<OperationLabels, Histogram>::new_with_constructor(|| {
                Histogram::new([
                    0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0,
                ])
            });
        let objects_uploaded_total = Counter::default();
        let objects_downloaded_total = Counter::default();
        let objects_deleted_total = Counter::default();
        let bytes_uploaded_total = Counter::default();
        let bytes_downloaded_total = Counter::default();

        registry.register(
            "api_gateway_http_requests_total",
            "Total HTTP requests by method, endpoint, and status",
            http_requests_total.clone(),
        );
        registry.register(
            "api_gateway_http_request_duration_seconds",
            "HTTP request duration in seconds",
            http_request_duration_seconds.clone(),
        );
        registry.register(
            "api_gateway_objects_uploaded",
            "Total number of objects uploaded",
            objects_uploaded_total.clone(),
        );
        registry.register(
            "api_gateway_objects_downloaded",
            "Total number of objects downloaded",
            objects_downloaded_total.clone(),
        );
        registry.register(
            "api_gateway_objects_deleted",
            "Total number of objects deleted",
            objects_deleted_total.clone(),
        );
        registry.register(
            "api_gateway_bytes_uploaded",
            "Total bytes uploaded",
            bytes_uploaded_total.clone(),
        );
        registry.register(
            "api_gateway_bytes_downloaded",
            "Total bytes downloaded",
            bytes_downloaded_total.clone(),
        );

        Metrics {
            http_requests_total,
            http_request_duration_seconds,
            objects_uploaded_total,
            objects_downloaded_total,
            objects_deleted_total,
            bytes_uploaded_total,
            bytes_downloaded_total,
        }
    }
}

pub fn encode_metrics(registry: &Registry) -> String {
    let mut buffer = String::new();
    encode(&mut buffer, registry).expect("failed to encode metrics");
    buffer
}
