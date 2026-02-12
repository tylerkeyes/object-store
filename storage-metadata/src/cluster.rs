use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use chitchat::transport::UdpTransport;
use chitchat::{ChitchatConfig, ChitchatHandle, ChitchatId, FailureDetectorConfig};
use tokio::sync::RwLock;
use tonic::transport::Channel;

use crate::hash_ring::HashRing;
use storage_proto_lib::storage_metadata::metadata_cluster_service_client::MetadataClusterServiceClient;

const GRPC_ADDR_KEY: &str = "grpc_addr";

/// Manages cluster membership via gossip and maintains the consistent hash ring.
pub struct ClusterManager {
    handle: ChitchatHandle,
    ring: Arc<RwLock<HashRing>>,
    node_id: String,
    grpc_addr: String,
    /// Peer gRPC connections: node_id → client
    peers: Arc<RwLock<HashMap<String, MetadataClusterServiceClient<Channel>>>>,
    /// node_id → grpc_addr mapping from gossip state
    peer_addrs: Arc<RwLock<HashMap<String, String>>>,
}

impl ClusterManager {
    /// Initialize the cluster manager and start gossip.
    pub async fn new(
        node_id: String,
        grpc_port: u16,
        gossip_port: u16,
        seed_nodes: Vec<String>,
    ) -> Result<Self, Box<dyn std::error::Error>> {
        let gossip_addr: SocketAddr = format!("0.0.0.0:{}", gossip_port).parse()?;
        let generation_id = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();

        let chitchat_id = ChitchatId::new(node_id.clone(), generation_id, gossip_addr);

        let config = ChitchatConfig {
            chitchat_id,
            cluster_id: "object-store-metadata".to_string(),
            gossip_interval: Duration::from_secs(1),
            listen_addr: gossip_addr,
            seed_nodes,
            failure_detector_config: FailureDetectorConfig::default(),
            marked_for_deletion_grace_period: Duration::from_secs(60),
            catchup_callback: None,
            extra_liveness_predicate: None,
        };

        let grpc_addr = format!("0.0.0.0:{}", grpc_port);
        let initial_kv = vec![(GRPC_ADDR_KEY.to_string(), grpc_addr.clone())];

        let transport = UdpTransport;
        let handle = chitchat::spawn_chitchat(config, initial_kv, &transport).await?;

        let ring = Arc::new(RwLock::new(HashRing::new()));
        let peers = Arc::new(RwLock::new(HashMap::new()));
        let peer_addrs = Arc::new(RwLock::new(HashMap::new()));

        // Add ourselves to the ring
        {
            let mut r = ring.write().await;
            r.add_node(&node_id);
        }

        let manager = Self {
            handle,
            ring,
            node_id,
            grpc_addr,
            peers,
            peer_addrs,
        };

        Ok(manager)
    }

    /// Start a background task that watches for membership changes
    /// and updates the ring + peer connections.
    pub fn spawn_membership_watcher(&self) {
        let handle_chitchat = self.handle.chitchat();
        let ring = self.ring.clone();
        let peers = self.peers.clone();
        let peer_addrs = self.peer_addrs.clone();
        let self_node_id = self.node_id.clone();

        tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_secs(2));
            loop {
                interval.tick().await;

                let (live_node_ids, live_grpc_addrs) = {
                    let chitchat = handle_chitchat.lock().await;
                    let mut ids = Vec::new();
                    let mut addrs = HashMap::new();

                    for node in chitchat.live_nodes() {
                        let nid = node.node_id.clone();
                        if let Some(state) = chitchat.node_state(&node) {
                            if let Some(addr) = state.get(GRPC_ADDR_KEY) {
                                addrs.insert(nid.clone(), addr.to_string());
                            }
                        }
                        ids.push(nid);
                    }
                    (ids, addrs)
                };

                // Update ring: add new nodes, remove departed ones
                {
                    let mut r = ring.write().await;
                    let current_nodes = r.nodes();

                    // Add new nodes (skip self, already added)
                    for nid in &live_node_ids {
                        if nid != &self_node_id && !current_nodes.contains(nid) {
                            tracing::info!("Adding node {} to hash ring", nid);
                            r.add_node(nid);
                        }
                    }

                    // Remove departed nodes
                    for nid in &current_nodes {
                        if nid != &self_node_id && !live_node_ids.contains(nid) {
                            tracing::info!("Removing node {} from hash ring", nid);
                            r.remove_node(nid);
                        }
                    }
                }

                // Update peer addresses
                {
                    let mut pa = peer_addrs.write().await;
                    *pa = live_grpc_addrs.clone();
                }

                // Clean up stale peer connections
                {
                    let mut p = peers.write().await;
                    p.retain(|nid, _| live_node_ids.contains(nid));
                }
            }
        });
    }

    /// Check if the given key is owned by this node.
    pub async fn is_local(&self, key: &str) -> bool {
        let ring = self.ring.read().await;
        match ring.get_owner(key) {
            Some(owner) => owner == self.node_id,
            None => true, // No nodes? Handle locally as fallback
        }
    }

    /// Get the gRPC address of the node that owns the given key.
    /// Returns None if this node owns the key.
    pub async fn get_owner_addr(&self, key: &str) -> Option<String> {
        let ring = self.ring.read().await;
        match ring.get_owner(key) {
            Some(owner) if owner == self.node_id => None,
            Some(owner) => {
                let addrs = self.peer_addrs.read().await;
                addrs.get(owner).cloned()
            }
            None => None,
        }
    }

    /// Get gRPC addresses of all peer nodes (excluding self).
    pub async fn get_all_peer_addrs(&self) -> Vec<String> {
        let addrs = self.peer_addrs.read().await;
        addrs
            .iter()
            .filter(|(nid, _)| *nid != &self.node_id)
            .map(|(_, addr)| addr.clone())
            .collect()
    }

    /// Get or create a gRPC client connection to a peer.
    pub async fn get_peer_client(
        &self,
        addr: &str,
    ) -> Result<MetadataClusterServiceClient<Channel>, tonic::transport::Error> {
        // Check if we already have a connection for this address
        {
            let peers = self.peers.read().await;
            // Find by address
            let pa = self.peer_addrs.read().await;
            for (nid, a) in pa.iter() {
                if a == addr {
                    if let Some(client) = peers.get(nid) {
                        return Ok(client.clone());
                    }
                }
            }
        }

        // Create new connection
        let endpoint = if addr.starts_with("http://") || addr.starts_with("https://") {
            addr.to_string()
        } else {
            format!("http://{}", addr)
        };

        let client = MetadataClusterServiceClient::connect(endpoint).await?;

        // Cache it
        {
            let pa = self.peer_addrs.read().await;
            let mut peers = self.peers.write().await;
            for (nid, a) in pa.iter() {
                if a == addr {
                    peers.insert(nid.clone(), client.clone());
                    break;
                }
            }
        }

        Ok(client)
    }

    /// Get the hash ring (read access).
    pub fn ring(&self) -> &Arc<RwLock<HashRing>> {
        &self.ring
    }

    /// Get this node's ID.
    pub fn node_id(&self) -> &str {
        &self.node_id
    }

    /// Get this node's gRPC address.
    pub fn grpc_addr(&self) -> &str {
        &self.grpc_addr
    }

    /// Get the node_id that owns a key (for logging/debugging).
    pub async fn get_owner_id(&self, key: &str) -> Option<String> {
        let ring = self.ring.read().await;
        ring.get_owner(key).map(|s| s.to_string())
    }

    /// Check if the cluster has more than one node.
    pub async fn is_clustered(&self) -> bool {
        let ring = self.ring.read().await;
        ring.node_count() > 1
    }

    /// Initiate graceful shutdown of gossip.
    pub async fn shutdown(self) -> Result<(), Box<dyn std::error::Error>> {
        self.handle.shutdown().await?;
        Ok(())
    }
}
