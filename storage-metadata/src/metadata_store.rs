use serde::{Deserialize, Serialize};
use std::{
    collections::HashMap,
    fs::{self, File, OpenOptions},
    io::{Read as _, Write},
    path::PathBuf,
    str::FromStr,
    time::Duration,
};

use http::Uri;
use rand::{Rng, seq::IteratorRandom, thread_rng};
use storage_proto_lib::storage_node::{
    CheckHealthRequest, storage_node_service_client::StorageNodeServiceClient,
};
use tokio::time;
use tonic::Request;

#[derive(Clone, Serialize, Deserialize)]
pub struct MetaStorageNode {
    pub id: u64,
    pub address: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum ChunkStatus {
    Pending,
    Confirmed,
}

impl Default for ChunkStatus {
    fn default() -> Self {
        ChunkStatus::Confirmed
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct MetaChunk {
    pub id: u64,
    pub object_name: String,
    pub checksum: u32,
    /// List of storage_node ids
    pub storage_nodes: Vec<u64>,
    #[serde(default)]
    pub status: ChunkStatus,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct MetaObject {
    pub name: String,
    pub checksum: u32,
    /// Maps chunk_id => list of chunk_ids
    pub chunks: Vec<u64>,
    #[serde(default)]
    pub total_size: u64,
}

const PERSISTENT_STATE_VERSION: u32 = 1;

#[derive(Serialize, Deserialize)]
struct PersistentState {
    #[serde(default)]
    version: u32,
    objects: HashMap<String, MetaObject>,
    chunks: HashMap<u64, MetaChunk>,
}

pub struct MetadataStore {
    path: PathBuf,
    /// Map object_id => MetaObject
    pub objects: HashMap<String, MetaObject>,
    /// Set of chunk_ids that are currently in use, mapped to their owning object
    pub chunks: HashMap<u64, MetaChunk>,
    pub storage_nodes: HashMap<u64, MetaStorageNode>,
}

impl MetaStorageNode {
    pub fn new(id: u64, address: String) -> std::io::Result<Self> {
        match Uri::from_str(address.as_str()) {
            Ok(_) => Ok(Self { id, address }),
            Err(_) => Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "invalid address",
            )),
        }
    }

    pub async fn check_health(&self) -> Result<bool, Box<dyn std::error::Error + Send>> {
        // Health checks should be fast - 5 seconds total is generous
        let timeout_duration = Duration::from_secs(5);

        // Timeout on connect
        let mut client = time::timeout(
            timeout_duration,
            StorageNodeServiceClient::connect(self.address.clone()),
        )
        .await
        .map_err(|_| -> Box<dyn std::error::Error + Send> {
            Box::new(std::io::Error::new(
                std::io::ErrorKind::TimedOut,
                format!("connection timeout to {} after {:?}", self.address, timeout_duration),
            ))
        })?
        .map_err(|e| -> Box<dyn std::error::Error + Send> { Box::new(e) })?;

        // Timeout on RPC call
        let response = time::timeout(
            timeout_duration,
            client.check_health(Request::new(CheckHealthRequest {})),
        )
        .await
        .map_err(|_| -> Box<dyn std::error::Error + Send> {
            Box::new(std::io::Error::new(
                std::io::ErrorKind::TimedOut,
                format!("health check RPC timeout to {} after {:?}", self.address, timeout_duration),
            ))
        })?
        .map_err(|e| -> Box<dyn std::error::Error + Send> { Box::new(e) })?
        .into_inner();

        Ok(response.healthy)
    }
}

impl MetaChunk {
    pub fn new(id: u64, checksum: u32, object_name: String) -> Self {
        Self {
            id,
            object_name,
            checksum,
            storage_nodes: Vec::new(),
            status: ChunkStatus::Confirmed,
        }
    }

    pub fn new_pending(id: u64, object_name: String) -> Self {
        Self {
            id,
            object_name,
            checksum: 0,
            storage_nodes: Vec::new(),
            status: ChunkStatus::Pending,
        }
    }

    pub fn add_storage_node(&mut self, storage_node_id: u64) {
        self.storage_nodes.push(storage_node_id);
    }
}

impl MetaObject {
    pub fn new(name: String, checksum: u32) -> Self {
        Self {
            name,
            checksum,
            chunks: Vec::new(),
            total_size: 0,
        }
    }

    pub fn add_chunk(&mut self, chunk_id: u64) {
        self.chunks.push(chunk_id);
    }
}

impl MetadataStore {
    /// Create a new MetadataStore with a node-specific persistence path.
    /// If data_dir and node_id are provided, persists to `{data_dir}/metadata-{node_id}.dat`.
    pub fn new_with_node(data_dir: &str, node_id: &str) -> std::io::Result<Self> {
        let path = format!("{}/metadata-{}.dat", data_dir, node_id);
        Self::new(&path)
    }

    pub fn new(path: &str) -> std::io::Result<Self> {
        let path_buf = PathBuf::from(path);

        // Try to load existing state from the file
        let (objects, chunks) = if path_buf.exists() {
            let mut file = File::open(&path_buf)?;
            let metadata = file.metadata()?;
            if metadata.len() > 0 {
                let mut contents = String::new();
                file.read_to_string(&mut contents)?;
                match serde_json::from_str::<PersistentState>(&contents) {
                    Ok(state) => {
                        tracing::info!(
                            "Loaded metadata from {} (version {}): {} objects, {} chunks",
                            path,
                            state.version,
                            state.objects.len(),
                            state.chunks.len()
                        );
                        (state.objects, state.chunks)
                    }
                    Err(e) => {
                        tracing::warn!("Failed to parse metadata file {}: {}, starting fresh", path, e);
                        (HashMap::new(), HashMap::new())
                    }
                }
            } else {
                (HashMap::new(), HashMap::new())
            }
        } else {
            (HashMap::new(), HashMap::new())
        };

        Ok(Self {
            path: path_buf,
            objects,
            chunks,
            // Storage nodes are not persisted; they re-register on startup
            storage_nodes: HashMap::new(),
        })
    }

    /// Persists the current objects and chunks state to the file.
    /// Uses atomic write-to-temp-then-rename to avoid data loss on crash.
    /// Storage nodes are not persisted as they re-register on startup.
    pub fn save(&self) -> std::io::Result<()> {
        let state = PersistentState {
            version: PERSISTENT_STATE_VERSION,
            objects: self.objects.clone(),
            chunks: self.chunks.clone(),
        };
        let json = serde_json::to_string(&state).map_err(|e| {
            std::io::Error::new(std::io::ErrorKind::Other, format!("serialization error: {}", e))
        })?;

        // Write to a temporary file, sync, then atomically rename
        let tmp_path = self.path.with_extension("tmp");
        let mut tmp_file = OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(&tmp_path)?;
        tmp_file.write_all(json.as_bytes())?;
        tmp_file.sync_data()?;
        drop(tmp_file);

        fs::rename(&tmp_path, &self.path)?;
        Ok(())
    }

    pub fn construct_storage_node(&mut self, address: String) -> std::io::Result<u64> {
        let id = self.generate_unique_id(&self.storage_nodes);
        let storage_node = MetaStorageNode::new(id, address)?;
        self.storage_nodes.insert(id, storage_node);
        Ok(id)
    }

    fn choose_storage_node(&self) -> Option<&MetaStorageNode> {
        if self.storage_nodes.is_empty() {
            return None;
        }

        let mut rng = thread_rng();
        self.storage_nodes.values().choose(&mut rng)
    }

    /// Generates a unique id, using the given map as a reference.
    /// TODO: this should be thread safe, and 'hold' the generated id so that other threads don't use it.
    fn generate_unique_id<T>(&self, id_map: &HashMap<u64, T>) -> u64 {
        let mut rng = rand::thread_rng();
        let mut id = rng.r#gen::<u64>();
        while id_map.contains_key(&id) {
            id = rng.r#gen::<u64>();
        }
        id
    }

    /// Stores an object, returns the object_id.
    /// Takes in the checksum of the entire object.
    pub fn put_object(&mut self, checksum: u32, name: String) -> std::io::Result<()> {
        if self.objects.contains_key(&name) {
            return Err(std::io::Error::new(
                std::io::ErrorKind::AlreadyExists,
                format!("object name {} already exists", name),
            ));
        }
        let object = MetaObject::new(name.clone(), checksum);
        self.objects.insert(name, object);
        self.save()?;
        Ok(())
    }

    pub fn get_object(&self, object_name: String) -> std::io::Result<&MetaObject> {
        self.objects
            .get(&object_name)
            .ok_or_else(|| std::io::Error::new(std::io::ErrorKind::NotFound, "object not found"))
    }

    pub fn get_object_mut(&mut self, object_name: String) -> std::io::Result<&mut MetaObject> {
        self.objects
            .get_mut(&object_name)
            .ok_or_else(|| std::io::Error::new(std::io::ErrorKind::NotFound, "object not found"))
    }

    pub fn delete_object(&mut self, object_name: String) -> std::io::Result<()> {
        let object = self
            .objects
            .remove(&object_name)
            .ok_or_else(|| std::io::Error::new(std::io::ErrorKind::NotFound, "object not found"))?;
        for chunk_id in object.chunks {
            // Chunk may have already been deleted via DeleteChunkFromNodes, so ignore missing
            let _ = self.chunks.remove(&chunk_id);
        }
        self.save()?;
        Ok(())
    }

    pub fn put_chunk(
        &mut self,
        object_name: String,
        checksum: u32,
    ) -> std::io::Result<(u64, String)> {
        let span = tracing::Span::current();
        let chunk_id = self.generate_unique_id(&self.chunks);
        let mut chunk = MetaChunk::new(chunk_id, checksum, object_name.clone());
        span.record("chunk.created_for_object", &chunk.object_name);
        let storage_node = match self.choose_storage_node() {
            Some(storage_node_id) => storage_node_id.clone(),
            None => {
                span.record("error", "no available storage node");
                return Err(std::io::Error::new(
                    std::io::ErrorKind::HostUnreachable,
                    "no available storage node",
                ));
            }
        };

        span.record("storage_node.found", &storage_node.address);
        span.record("storage_node.id", storage_node.id);
        chunk.add_storage_node(storage_node.id);
        self.chunks.insert(chunk_id, chunk);

        let object = self
            .objects
            .get_mut(&object_name)
            .ok_or_else(|| std::io::Error::new(std::io::ErrorKind::NotFound, "object not found"))?;
        object.add_chunk(chunk_id);
        let address = storage_node.address.clone();
        self.save()?;
        Ok((chunk_id, address))
    }

    /// Allocates a chunk and reserves the ID with a Pending placeholder (Query phase).
    /// Returns chunk_id, storage_node_id, and storage_node_address.
    /// The caller must call confirm_chunk after successfully writing to the storage node.
    pub fn allocate_chunk(&mut self, object_name: &str) -> std::io::Result<(u64, u64, String)> {
        let chunk_id = self.generate_unique_id(&self.chunks);
        let storage_node = match self.choose_storage_node() {
            Some(node) => node.clone(),
            None => {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::HostUnreachable,
                    "no available storage node",
                ));
            }
        };
        // Insert a pending placeholder to reserve the ID
        let pending_chunk = MetaChunk::new_pending(chunk_id, object_name.to_string());
        self.chunks.insert(chunk_id, pending_chunk);
        Ok((chunk_id, storage_node.id, storage_node.address.clone()))
    }

    /// Confirms a chunk after successful write to storage node (Persist phase).
    /// Updates the pending chunk placeholder with real data and adds it to the object.
    pub fn confirm_chunk(
        &mut self,
        chunk_id: u64,
        object_name: String,
        checksum: u32,
        storage_node_id: u64,
    ) -> std::io::Result<()> {
        // Validate storage node exists
        if !self.storage_nodes.contains_key(&storage_node_id) {
            return Err(std::io::Error::new(
                std::io::ErrorKind::NotFound,
                format!("storage node {} not found", storage_node_id),
            ));
        }

        // Check if chunk exists as a pending reservation or already confirmed
        match self.chunks.get(&chunk_id) {
            Some(existing) if existing.status == ChunkStatus::Confirmed => {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::AlreadyExists,
                    format!("chunk {} already confirmed", chunk_id),
                ));
            }
            Some(_) => {
                // Pending placeholder exists, will be updated below
            }
            None => {
                // No placeholder - chunk was allocated without reserving ID (legacy path)
                // Allow it for backwards compatibility
            }
        }

        // Validate object exists
        let object = self
            .objects
            .get_mut(&object_name)
            .ok_or_else(|| std::io::Error::new(std::io::ErrorKind::NotFound, "object not found"))?;

        // Create/update the chunk with confirmed status
        let mut chunk = MetaChunk::new(chunk_id, checksum, object_name);
        chunk.add_storage_node(storage_node_id);
        self.chunks.insert(chunk_id, chunk);
        object.add_chunk(chunk_id);
        self.save()?;

        Ok(())
    }

    pub fn get_chunk(&self, chunk_id: u64) -> std::io::Result<(String, &MetaChunk)> {
        let chunk = self
            .chunks
            .get(&chunk_id)
            .ok_or_else(|| std::io::Error::new(std::io::ErrorKind::NotFound, "chunk not found"))?;

        let object_name = chunk.object_name.clone();

        Ok((object_name, chunk))
    }

    pub fn delete_chunk(&mut self, chunk_id: u64) -> std::io::Result<()> {
        let chunk = self.chunks.remove(&chunk_id).ok_or_else(|| {
            std::io::Error::new(std::io::ErrorKind::NotFound, "could not find mapped object")
        })?;

        let object = match self.get_object_mut(chunk.object_name) {
            Ok(object) => object,
            Err(e) => return Err(std::io::Error::new(std::io::ErrorKind::NotFound, e)),
        };
        object.chunks.retain(|c| *c != chunk_id);
        self.save()?;

        Ok(())
    }

    pub fn list_objects(&self) -> Vec<MetaObject> {
        self.objects.values().cloned().collect()
    }

    pub fn get_storage_node(&self, storage_node_id: u64) -> std::io::Result<String> {
        match self.storage_nodes.get(&storage_node_id) {
            Some(storage_node) => Ok(storage_node.address.clone()),
            None => Err(std::io::Error::new(
                std::io::ErrorKind::NotFound,
                format!("could not find storage_node_id {}", storage_node_id),
            )),
        }
    }

    /// Updates an existing object's checksum and total_size (for deferred checksum after streaming upload).
    pub fn update_object_checksum(&mut self, object_name: &str, checksum: u32, total_size: u64) -> std::io::Result<()> {
        let object = self.objects.get_mut(object_name).ok_or_else(|| {
            std::io::Error::new(std::io::ErrorKind::NotFound, "object not found")
        })?;
        object.checksum = checksum;
        object.total_size = total_size;
        self.save()?;
        Ok(())
    }

    pub fn list_storage_nodes(&self) -> Vec<MetaStorageNode> {
        self.storage_nodes.clone().into_values().collect()
    }

    pub fn delete_storage_node(&mut self, storage_node_id: u64) -> std::io::Result<()> {
        match self.storage_nodes.remove(&storage_node_id) {
            Some(_) => Ok(()),
            None => Err(std::io::Error::new(
                std::io::ErrorKind::NotFound,
                format!("could not find storage node {}", storage_node_id),
            )),
        }
    }

    /// Returns chunks with fewer than `max_replicas` storage nodes.
    /// Chunks are sorted by replica count (fewest first) for replication priority.
    pub fn list_under_replicated_chunks(&self, max_replicas: u32) -> Vec<&MetaChunk> {
        let mut chunks: Vec<_> = self
            .chunks
            .values()
            .filter(|chunk| chunk.status == ChunkStatus::Confirmed && chunk.storage_nodes.len() < max_replicas as usize)
            .collect();
        // Sort by replica count (fewest first) for priority
        chunks.sort_by_key(|c| c.storage_nodes.len());
        chunks
    }

    /// Adds a storage node replica to a chunk.
    /// Returns an error if:
    /// - The chunk doesn't exist
    /// - The storage node doesn't exist
    /// - The chunk is already on this storage node
    /// - The chunk already has 3 replicas
    pub fn add_chunk_replica(
        &mut self,
        chunk_id: u64,
        storage_node_id: u64,
    ) -> std::io::Result<()> {
        // Validate storage node exists
        if !self.storage_nodes.contains_key(&storage_node_id) {
            return Err(std::io::Error::new(
                std::io::ErrorKind::NotFound,
                format!("storage node {} not found", storage_node_id),
            ));
        }

        // Get chunk and validate
        let chunk = self.chunks.get_mut(&chunk_id).ok_or_else(|| {
            std::io::Error::new(std::io::ErrorKind::NotFound, "chunk not found")
        })?;

        // Check chunk not already on this node
        if chunk.storage_nodes.contains(&storage_node_id) {
            return Err(std::io::Error::new(
                std::io::ErrorKind::AlreadyExists,
                format!(
                    "chunk {} already exists on storage node {}",
                    chunk_id, storage_node_id
                ),
            ));
        }

        // Check chunk doesn't already have max replicas (3)
        const MAX_REPLICAS: usize = 3;
        if chunk.storage_nodes.len() >= MAX_REPLICAS {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                format!(
                    "chunk {} already has {} replicas (max: {})",
                    chunk_id,
                    chunk.storage_nodes.len(),
                    MAX_REPLICAS
                ),
            ));
        }

        // Add the replica
        chunk.storage_nodes.push(storage_node_id);
        self.save()?;
        Ok(())
    }

    /// Gets a mutable reference to a chunk.
    #[allow(dead_code)]
    pub fn get_chunk_mut(&mut self, chunk_id: u64) -> std::io::Result<&mut MetaChunk> {
        self.chunks.get_mut(&chunk_id).ok_or_else(|| {
            std::io::Error::new(std::io::ErrorKind::NotFound, "chunk not found")
        })
    }

    // ===== Migration helpers =====

    /// Returns objects (with their chunks) whose names hash into any of the given ranges.
    /// Used by TransferPartition to stream data to a new node.
    pub fn objects_in_hash_ranges(
        &self,
        ranges: &[(u64, u64)],
    ) -> Vec<(MetaObject, Vec<MetaChunk>)> {
        use crate::hash_ring::HashRing;

        let mut result = Vec::new();
        for obj in self.objects.values() {
            let hash = HashRing::hash_key(&obj.name);
            if HashRing::hash_in_ranges(hash, ranges) {
                let chunks: Vec<MetaChunk> = obj
                    .chunks
                    .iter()
                    .filter_map(|id| self.chunks.get(id).cloned())
                    .collect();
                result.push((obj.clone(), chunks));
            }
        }
        result
    }

    /// Ingest migrated objects and chunks from another node.
    pub fn ingest_partition(
        &mut self,
        objects: Vec<MetaObject>,
        chunks: Vec<MetaChunk>,
    ) -> std::io::Result<()> {
        for obj in objects {
            self.objects.insert(obj.name.clone(), obj);
        }
        for chunk in chunks {
            self.chunks.insert(chunk.id, chunk);
        }
        self.save()?;
        Ok(())
    }

    /// Remove objects by name (and their associated chunks) after migration transfer.
    #[allow(dead_code)]
    pub fn remove_objects(&mut self, names: &[String]) -> std::io::Result<()> {
        for name in names {
            if let Some(obj) = self.objects.remove(name) {
                for chunk_id in &obj.chunks {
                    self.chunks.remove(chunk_id);
                }
            }
        }
        self.save()?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::NamedTempFile;

    fn create_test_store() -> (MetadataStore, tempfile::NamedTempFile) {
        let file = NamedTempFile::new().unwrap();
        let path = file.path().to_str().unwrap();
        let store = MetadataStore::new(path).unwrap();
        (store, file)
    }

    fn add_storage_node(store: &mut MetadataStore, id: u64, port: u16) {
        store.storage_nodes.insert(
            id,
            MetaStorageNode {
                id,
                address: format!("127.0.0.1:{}", port),
            },
        );
    }

    // ===================
    // Storage Node Tests
    // ===================

    #[test]
    fn test_storage_node_address_validation() {
        // Valid URI formats (http::Uri is permissive)
        assert!(MetaStorageNode::new(0, "http://10.10.10.10".to_string()).is_ok());
        assert!(MetaStorageNode::new(0, "http://10.10.10.10:80".to_string()).is_ok());
        assert!(MetaStorageNode::new(0, "127.0.0.1:8080".to_string()).is_ok());
        assert!(MetaStorageNode::new(0, "localhost:3000".to_string()).is_ok());
        assert!(MetaStorageNode::new(0, "10.10.10.10".to_string()).is_ok());

        // Empty string is invalid
        assert!(MetaStorageNode::new(0, "".to_string()).is_err());
    }

    #[test]
    fn test_construct_storage_node_generates_unique_ids() {
        let (mut store, _file) = create_test_store();

        let id1 = store.construct_storage_node("127.0.0.1:8080".to_string()).unwrap();
        let id2 = store.construct_storage_node("127.0.0.1:8081".to_string()).unwrap();
        let id3 = store.construct_storage_node("127.0.0.1:8082".to_string()).unwrap();

        assert_ne!(id1, id2);
        assert_ne!(id2, id3);
        assert_ne!(id1, id3);
        assert_eq!(store.storage_nodes.len(), 3);
    }

    #[test]
    fn test_construct_storage_node_rejects_invalid_address() {
        let (mut store, _file) = create_test_store();

        // Empty string is an invalid URI
        let result = store.construct_storage_node("".to_string());
        assert!(result.is_err());
        assert_eq!(result.unwrap_err().kind(), std::io::ErrorKind::InvalidInput);
    }

    #[test]
    fn test_get_storage_node_returns_address() {
        let (mut store, _file) = create_test_store();
        add_storage_node(&mut store, 42, 3000);

        let address = store.get_storage_node(42).unwrap();
        assert_eq!(address, "127.0.0.1:3000");
    }

    #[test]
    fn test_get_storage_node_not_found() {
        let (store, _file) = create_test_store();

        let result = store.get_storage_node(99999);
        assert!(result.is_err());
        assert_eq!(result.unwrap_err().kind(), std::io::ErrorKind::NotFound);
    }

    #[test]
    fn test_delete_storage_node() {
        let (mut store, _file) = create_test_store();
        add_storage_node(&mut store, 42, 3000);

        store.delete_storage_node(42).unwrap();
        assert!(store.get_storage_node(42).is_err());
    }

    #[test]
    fn test_delete_storage_node_not_found() {
        let (mut store, _file) = create_test_store();

        let result = store.delete_storage_node(99999);
        assert!(result.is_err());
        assert_eq!(result.unwrap_err().kind(), std::io::ErrorKind::NotFound);
    }

    #[test]
    fn test_list_storage_nodes() {
        let (mut store, _file) = create_test_store();
        add_storage_node(&mut store, 1, 3000);
        add_storage_node(&mut store, 2, 3001);

        let nodes = store.list_storage_nodes();
        assert_eq!(nodes.len(), 2);

        let ids: Vec<u64> = nodes.iter().map(|n| n.id).collect();
        assert!(ids.contains(&1));
        assert!(ids.contains(&2));
    }

    // ===================
    // Object Tests
    // ===================

    #[test]
    fn test_put_and_get_object() {
        let (mut store, _file) = create_test_store();

        store.put_object(12345, "myobject".to_string()).unwrap();

        let object = store.get_object("myobject".to_string()).unwrap();
        assert_eq!(object.name, "myobject");
        assert_eq!(object.checksum, 12345);
        assert!(object.chunks.is_empty());
    }

    #[test]
    fn test_put_object_duplicate_name_fails() {
        let (mut store, _file) = create_test_store();

        store.put_object(12345, "myobject".to_string()).unwrap();
        let result = store.put_object(67890, "myobject".to_string());

        assert!(result.is_err());
        assert_eq!(result.unwrap_err().kind(), std::io::ErrorKind::AlreadyExists);
    }

    #[test]
    fn test_get_object_not_found() {
        let (store, _file) = create_test_store();

        let result = store.get_object("nonexistent".to_string());
        assert!(result.is_err());
        assert_eq!(result.unwrap_err().kind(), std::io::ErrorKind::NotFound);
    }

    #[test]
    fn test_delete_object() {
        let (mut store, _file) = create_test_store();

        store.put_object(12345, "myobject".to_string()).unwrap();
        store.delete_object("myobject".to_string()).unwrap();

        assert!(store.get_object("myobject".to_string()).is_err());
    }

    #[test]
    fn test_delete_object_not_found() {
        let (mut store, _file) = create_test_store();

        let result = store.delete_object("nonexistent".to_string());
        assert!(result.is_err());
        assert_eq!(result.unwrap_err().kind(), std::io::ErrorKind::NotFound);
    }

    #[test]
    fn test_delete_object_also_removes_chunks() {
        let (mut store, _file) = create_test_store();
        add_storage_node(&mut store, 1, 3000);

        store.put_object(12345, "myobject".to_string()).unwrap();
        let (chunk_id, _) = store.put_chunk("myobject".to_string(), 111).unwrap();

        // Verify chunk exists
        assert!(store.get_chunk(chunk_id).is_ok());

        // Delete object
        store.delete_object("myobject".to_string()).unwrap();

        // Chunk should be removed from chunks index
        assert!(store.get_chunk(chunk_id).is_err());
    }

    #[test]
    fn test_list_objects() {
        let (mut store, _file) = create_test_store();

        store.put_object(111, "obj1".to_string()).unwrap();
        store.put_object(222, "obj2".to_string()).unwrap();
        store.put_object(333, "obj3".to_string()).unwrap();

        let objects = store.list_objects();
        assert_eq!(objects.len(), 3);

        let names: Vec<&str> = objects.iter().map(|o| o.name.as_str()).collect();
        assert!(names.contains(&"obj1"));
        assert!(names.contains(&"obj2"));
        assert!(names.contains(&"obj3"));
    }

    #[test]
    fn test_list_objects_empty() {
        let (store, _file) = create_test_store();

        let objects = store.list_objects();
        assert!(objects.is_empty());
    }

    // ===================
    // Chunk Tests - put_chunk (legacy)
    // ===================

    #[test]
    fn test_put_chunk_assigns_storage_node() {
        let (mut store, _file) = create_test_store();
        add_storage_node(&mut store, 1, 3000);

        store.put_object(12345, "myobject".to_string()).unwrap();
        let (chunk_id, address) = store.put_chunk("myobject".to_string(), 67890).unwrap();

        assert_eq!(address, "127.0.0.1:3000");

        let (_, chunk) = store.get_chunk(chunk_id).unwrap();
        assert_eq!(chunk.checksum, 67890);
        assert_eq!(chunk.storage_nodes, vec![1]);
    }

    #[test]
    fn test_put_chunk_adds_to_object() {
        let (mut store, _file) = create_test_store();
        add_storage_node(&mut store, 1, 3000);

        store.put_object(12345, "myobject".to_string()).unwrap();
        let (chunk_id, _) = store.put_chunk("myobject".to_string(), 111).unwrap();

        let object = store.get_object("myobject".to_string()).unwrap();
        assert_eq!(object.chunks, vec![chunk_id]);
    }

    #[test]
    fn test_put_chunk_no_storage_nodes_fails() {
        let (mut store, _file) = create_test_store();

        store.put_object(12345, "myobject".to_string()).unwrap();
        let result = store.put_chunk("myobject".to_string(), 67890);

        assert!(result.is_err());
        assert_eq!(result.unwrap_err().kind(), std::io::ErrorKind::HostUnreachable);
    }

    #[test]
    fn test_put_chunk_object_not_found_fails() {
        let (mut store, _file) = create_test_store();
        add_storage_node(&mut store, 1, 3000);

        let result = store.put_chunk("nonexistent".to_string(), 67890);
        assert!(result.is_err());
        assert_eq!(result.unwrap_err().kind(), std::io::ErrorKind::NotFound);
    }

    // ===================
    // Chunk Tests - allocate/confirm (two-phase)
    // ===================

    #[test]
    fn test_allocate_chunk_returns_node_info() {
        let (mut store, _file) = create_test_store();
        add_storage_node(&mut store, 42, 3000);

        let (chunk_id, node_id, address) = store.allocate_chunk("myobject").unwrap();

        assert!(chunk_id > 0 || chunk_id == 0); // Just verify it returns something
        assert_eq!(node_id, 42);
        assert_eq!(address, "127.0.0.1:3000");
    }

    #[test]
    fn test_allocate_chunk_no_storage_nodes_fails() {
        let (mut store, _file) = create_test_store();

        let result = store.allocate_chunk("myobject");
        assert!(result.is_err());
        assert_eq!(result.unwrap_err().kind(), std::io::ErrorKind::HostUnreachable);
    }

    #[test]
    fn test_allocate_chunk_reserves_id_as_pending() {
        let (mut store, _file) = create_test_store();
        add_storage_node(&mut store, 1, 3000);

        let (chunk_id, _, _) = store.allocate_chunk("myobject").unwrap();

        // Chunk should be in the store as a pending placeholder
        assert!(!store.chunks.is_empty());
        let chunk = store.chunks.get(&chunk_id).unwrap();
        assert_eq!(chunk.status, ChunkStatus::Pending);
    }

    #[test]
    fn test_confirm_chunk_persists_metadata() {
        let (mut store, _file) = create_test_store();
        add_storage_node(&mut store, 1, 3000);
        store.put_object(12345, "myobject".to_string()).unwrap();

        let (chunk_id, node_id, _) = store.allocate_chunk("myobject").unwrap();

        store.confirm_chunk(chunk_id, "myobject".to_string(), 67890, node_id).unwrap();

        // Chunk should now exist
        let (obj_name, chunk) = store.get_chunk(chunk_id).unwrap();
        assert_eq!(obj_name, "myobject");
        assert_eq!(chunk.checksum, 67890);
        assert_eq!(chunk.storage_nodes, vec![node_id]);

        // Object should reference the chunk
        let object = store.get_object("myobject".to_string()).unwrap();
        assert!(object.chunks.contains(&chunk_id));
    }

    #[test]
    fn test_confirm_chunk_storage_node_not_found_fails() {
        let (mut store, _file) = create_test_store();
        add_storage_node(&mut store, 1, 3000);
        store.put_object(12345, "myobject".to_string()).unwrap();

        let result = store.confirm_chunk(100, "myobject".to_string(), 67890, 99999);

        assert!(result.is_err());
        assert_eq!(result.unwrap_err().kind(), std::io::ErrorKind::NotFound);
    }

    #[test]
    fn test_confirm_chunk_duplicate_chunk_id_fails() {
        let (mut store, _file) = create_test_store();
        add_storage_node(&mut store, 1, 3000);
        store.put_object(12345, "myobject".to_string()).unwrap();

        let (chunk_id, node_id, _) = store.allocate_chunk("myobject").unwrap();
        store.confirm_chunk(chunk_id, "myobject".to_string(), 111, node_id).unwrap();

        // Try to confirm same chunk_id again
        let result = store.confirm_chunk(chunk_id, "myobject".to_string(), 222, node_id);

        assert!(result.is_err());
        assert_eq!(result.unwrap_err().kind(), std::io::ErrorKind::AlreadyExists);
    }

    #[test]
    fn test_confirm_chunk_object_not_found_fails() {
        let (mut store, _file) = create_test_store();
        add_storage_node(&mut store, 1, 3000);

        let (chunk_id, node_id, _) = store.allocate_chunk("myobject").unwrap();
        let result = store.confirm_chunk(chunk_id, "nonexistent".to_string(), 67890, node_id);

        assert!(result.is_err());
        assert_eq!(result.unwrap_err().kind(), std::io::ErrorKind::NotFound);
    }

    #[test]
    fn test_full_allocate_confirm_flow_multiple_chunks() {
        let (mut store, _file) = create_test_store();
        add_storage_node(&mut store, 1, 3000);
        add_storage_node(&mut store, 2, 3001);
        store.put_object(12345, "myobject".to_string()).unwrap();

        // Simulate uploading 3 chunks
        let mut chunk_ids = Vec::new();
        for i in 0..3 {
            let (chunk_id, node_id, _) = store.allocate_chunk("myobject").unwrap();
            store.confirm_chunk(chunk_id, "myobject".to_string(), 100 + i, node_id).unwrap();
            chunk_ids.push(chunk_id);
        }

        // Verify all chunks are attached to object
        let object = store.get_object("myobject".to_string()).unwrap();
        assert_eq!(object.chunks.len(), 3);
        for id in &chunk_ids {
            assert!(object.chunks.contains(id));
        }
    }

    // ===================
    // Chunk Tests - get/delete
    // ===================

    #[test]
    fn test_get_chunk_returns_object_name_and_metadata() {
        let (mut store, _file) = create_test_store();
        add_storage_node(&mut store, 1, 3000);
        store.put_object(12345, "myobject".to_string()).unwrap();

        let (chunk_id, _) = store.put_chunk("myobject".to_string(), 67890).unwrap();

        let (obj_name, chunk) = store.get_chunk(chunk_id).unwrap();
        assert_eq!(obj_name, "myobject");
        assert_eq!(chunk.id, chunk_id);
        assert_eq!(chunk.checksum, 67890);
        assert_eq!(chunk.object_name, "myobject");
    }

    #[test]
    fn test_get_chunk_not_found() {
        let (store, _file) = create_test_store();

        let result = store.get_chunk(99999);
        assert!(result.is_err());
        assert_eq!(result.unwrap_err().kind(), std::io::ErrorKind::NotFound);
    }

    #[test]
    fn test_delete_chunk_removes_from_object() {
        let (mut store, _file) = create_test_store();
        add_storage_node(&mut store, 1, 3000);
        store.put_object(12345, "myobject".to_string()).unwrap();

        let (chunk_id, _) = store.put_chunk("myobject".to_string(), 67890).unwrap();

        // Verify chunk in object
        let object = store.get_object("myobject".to_string()).unwrap();
        assert_eq!(object.chunks, vec![chunk_id]);

        // Delete chunk
        store.delete_chunk(chunk_id).unwrap();

        // Verify chunk is gone from index and object
        assert!(store.get_chunk(chunk_id).is_err());
        let object = store.get_object("myobject".to_string()).unwrap();
        assert!(object.chunks.is_empty());
    }

    #[test]
    fn test_delete_chunk_not_found() {
        let (mut store, _file) = create_test_store();

        let result = store.delete_chunk(99999);
        assert!(result.is_err());
        assert_eq!(result.unwrap_err().kind(), std::io::ErrorKind::NotFound);
    }

    // ===================
    // Replication Tests
    // ===================

    #[test]
    fn test_list_under_replicated_chunks_sorted_by_replica_count() {
        let (mut store, _file) = create_test_store();
        add_storage_node(&mut store, 1, 3000);
        add_storage_node(&mut store, 2, 3001);
        add_storage_node(&mut store, 3, 3002);

        store.put_object(12345, "myobject".to_string()).unwrap();

        // Create 3 chunks with 1, 2, and 1 replicas respectively
        let (_chunk1_id, _) = store.put_chunk("myobject".to_string(), 111).unwrap();
        let (chunk2_id, _) = store.put_chunk("myobject".to_string(), 222).unwrap();
        let (_chunk3_id, _) = store.put_chunk("myobject".to_string(), 333).unwrap();

        // Add extra replica to chunk2 (using a different node)
        let (_, chunk2) = store.get_chunk(chunk2_id).unwrap();
        let chunk2_node = chunk2.storage_nodes[0];
        let other_node = if chunk2_node == 1 { 2 } else { 1 };
        store.add_chunk_replica(chunk2_id, other_node).unwrap();

        // Under-replicated with max=3 should return all 3
        let under_rep = store.list_under_replicated_chunks(3);
        assert_eq!(under_rep.len(), 3);
        // Should be sorted: 1-replica chunks first, then 2-replica
        assert!(under_rep[0].storage_nodes.len() <= under_rep[2].storage_nodes.len());

        // Under-replicated with max=2 should return only 1-replica chunks
        let under_rep = store.list_under_replicated_chunks(2);
        assert_eq!(under_rep.len(), 2);
        for chunk in under_rep {
            assert_eq!(chunk.storage_nodes.len(), 1);
        }

        // Under-replicated with max=1 should return none
        let under_rep = store.list_under_replicated_chunks(1);
        assert!(under_rep.is_empty());
    }

    #[test]
    fn test_add_chunk_replica_success() {
        let (mut store, _file) = create_test_store();
        add_storage_node(&mut store, 1, 3000);
        add_storage_node(&mut store, 2, 3001);

        store.put_object(12345, "myobject".to_string()).unwrap();
        let (chunk_id, _) = store.put_chunk("myobject".to_string(), 111).unwrap();

        // Get which node the chunk is on
        let (_, chunk) = store.get_chunk(chunk_id).unwrap();
        let first_node = chunk.storage_nodes[0];
        let second_node = if first_node == 1 { 2 } else { 1 };

        store.add_chunk_replica(chunk_id, second_node).unwrap();

        let (_, chunk) = store.get_chunk(chunk_id).unwrap();
        assert_eq!(chunk.storage_nodes.len(), 2);
        assert!(chunk.storage_nodes.contains(&first_node));
        assert!(chunk.storage_nodes.contains(&second_node));
    }

    #[test]
    fn test_add_chunk_replica_max_replicas_exceeded() {
        let (mut store, _file) = create_test_store();
        add_storage_node(&mut store, 1, 3000);
        add_storage_node(&mut store, 2, 3001);
        add_storage_node(&mut store, 3, 3002);
        add_storage_node(&mut store, 4, 3003);

        store.put_object(12345, "myobject".to_string()).unwrap();
        let (chunk_id, _) = store.put_chunk("myobject".to_string(), 111).unwrap();

        let (_, chunk) = store.get_chunk(chunk_id).unwrap();
        let first_node = chunk.storage_nodes[0];
        let other_nodes: Vec<u64> = vec![1, 2, 3, 4].into_iter().filter(|&n| n != first_node).collect();

        // Add 2 more replicas (total 3)
        store.add_chunk_replica(chunk_id, other_nodes[0]).unwrap();
        store.add_chunk_replica(chunk_id, other_nodes[1]).unwrap();

        // Fourth replica should fail
        let result = store.add_chunk_replica(chunk_id, other_nodes[2]);
        assert!(result.is_err());
        assert_eq!(result.unwrap_err().kind(), std::io::ErrorKind::InvalidInput);
    }

    #[test]
    fn test_add_chunk_replica_duplicate_fails() {
        let (mut store, _file) = create_test_store();
        add_storage_node(&mut store, 1, 3000);

        store.put_object(12345, "myobject".to_string()).unwrap();
        let (chunk_id, _) = store.put_chunk("myobject".to_string(), 111).unwrap();

        let (_, chunk) = store.get_chunk(chunk_id).unwrap();
        let existing_node = chunk.storage_nodes[0];

        let result = store.add_chunk_replica(chunk_id, existing_node);
        assert!(result.is_err());
        assert_eq!(result.unwrap_err().kind(), std::io::ErrorKind::AlreadyExists);
    }

    #[test]
    fn test_add_chunk_replica_chunk_not_found() {
        let (mut store, _file) = create_test_store();
        add_storage_node(&mut store, 1, 3000);

        let result = store.add_chunk_replica(99999, 1);
        assert!(result.is_err());
        assert_eq!(result.unwrap_err().kind(), std::io::ErrorKind::NotFound);
    }

    #[test]
    fn test_add_chunk_replica_storage_node_not_found() {
        let (mut store, _file) = create_test_store();
        add_storage_node(&mut store, 1, 3000);
        store.put_object(12345, "myobject".to_string()).unwrap();
        let (chunk_id, _) = store.put_chunk("myobject".to_string(), 111).unwrap();

        let result = store.add_chunk_replica(chunk_id, 99999);
        assert!(result.is_err());
        assert_eq!(result.unwrap_err().kind(), std::io::ErrorKind::NotFound);
    }

    // ===================
    // get_chunk_mut Tests
    // ===================

    #[test]
    fn test_get_chunk_mut_allows_modification() {
        let (mut store, _file) = create_test_store();
        add_storage_node(&mut store, 1, 3000);
        store.put_object(12345, "myobject".to_string()).unwrap();
        let (chunk_id, _) = store.put_chunk("myobject".to_string(), 111).unwrap();

        // Modify the chunk
        {
            let chunk = store.get_chunk_mut(chunk_id).unwrap();
            chunk.checksum = 999;
        }

        // Verify modification persisted
        let (_, chunk) = store.get_chunk(chunk_id).unwrap();
        assert_eq!(chunk.checksum, 999);
    }

    #[test]
    fn test_get_chunk_mut_not_found() {
        let (mut store, _file) = create_test_store();

        let result = store.get_chunk_mut(99999);
        assert!(result.is_err());
        assert_eq!(result.unwrap_err().kind(), std::io::ErrorKind::NotFound);
    }

    // ===================
    // Random Selection Tests
    // ===================

    #[test]
    fn test_choose_storage_node_distributes_across_nodes() {
        let (mut store, _file) = create_test_store();
        add_storage_node(&mut store, 1, 3000);
        add_storage_node(&mut store, 2, 3001);
        add_storage_node(&mut store, 3, 3002);

        // Call many times and verify we get different nodes
        let mut seen_nodes = std::collections::HashSet::new();
        for _ in 0..100 {
            if let Some(node) = store.choose_storage_node() {
                seen_nodes.insert(node.id);
            }
        }

        // With 100 tries and 3 nodes, we should see all 3 (statistically very likely)
        assert!(seen_nodes.len() >= 2, "Expected random distribution across nodes");
    }

    #[test]
    fn test_choose_storage_node_empty_returns_none() {
        let (store, _file) = create_test_store();

        assert!(store.choose_storage_node().is_none());
    }

    // ===================
    // Persistence Tests
    // ===================

    #[test]
    fn test_persistence_objects_survive_reload() {
        let file = NamedTempFile::new().unwrap();
        let path = file.path().to_str().unwrap().to_string();

        // Create store, add data
        {
            let mut store = MetadataStore::new(&path).unwrap();
            add_storage_node(&mut store, 1, 3000);
            store.put_object(12345, "obj1".to_string()).unwrap();
            store.put_object(67890, "obj2".to_string()).unwrap();
        }

        // Reload and verify
        let store = MetadataStore::new(&path).unwrap();
        assert_eq!(store.objects.len(), 2);
        let obj1 = store.get_object("obj1".to_string()).unwrap();
        assert_eq!(obj1.checksum, 12345);
        let obj2 = store.get_object("obj2".to_string()).unwrap();
        assert_eq!(obj2.checksum, 67890);
        // Storage nodes should not persist
        assert!(store.storage_nodes.is_empty());
    }

    #[test]
    fn test_persistence_chunks_survive_reload() {
        let file = NamedTempFile::new().unwrap();
        let path = file.path().to_str().unwrap().to_string();

        let chunk_id;

        {
            let mut store = MetadataStore::new(&path).unwrap();
            add_storage_node(&mut store, 1, 3000);
            store.put_object(12345, "myobject".to_string()).unwrap();
            let (cid, _) = store.put_chunk("myobject".to_string(), 67890).unwrap();
            chunk_id = cid;
        }

        let store = MetadataStore::new(&path).unwrap();
        let (obj_name, chunk) = store.get_chunk(chunk_id).unwrap();
        assert_eq!(obj_name, "myobject");
        assert_eq!(chunk.checksum, 67890);

        let object = store.get_object("myobject".to_string()).unwrap();
        assert!(object.chunks.contains(&chunk_id));
    }

    #[test]
    fn test_persistence_delete_reflected_on_reload() {
        let file = NamedTempFile::new().unwrap();
        let path = file.path().to_str().unwrap().to_string();

        {
            let mut store = MetadataStore::new(&path).unwrap();
            add_storage_node(&mut store, 1, 3000);
            store.put_object(111, "keep".to_string()).unwrap();
            store.put_object(222, "delete-me".to_string()).unwrap();
            store.delete_object("delete-me".to_string()).unwrap();
        }

        let store = MetadataStore::new(&path).unwrap();
        assert_eq!(store.objects.len(), 1);
        assert!(store.get_object("keep".to_string()).is_ok());
        assert!(store.get_object("delete-me".to_string()).is_err());
    }

    #[test]
    fn test_persistence_empty_file_starts_fresh() {
        let file = NamedTempFile::new().unwrap();
        let path = file.path().to_str().unwrap().to_string();

        let store = MetadataStore::new(&path).unwrap();
        assert!(store.objects.is_empty());
        assert!(store.chunks.is_empty());
    }

    // ===================
    // Migration Tests
    // ===================

    #[test]
    fn test_objects_in_hash_ranges_filters_correctly() {
        let (mut store, _file) = create_test_store();
        add_storage_node(&mut store, 1, 3000);

        // Create several objects
        for i in 0..20 {
            let name = format!("migration-obj-{}", i);
            store.put_object(i as u32, name.clone()).unwrap();
            let (chunk_id, _) = store.put_chunk(name.clone(), 100 + i as u32).unwrap();
            let _ = chunk_id;
        }

        // Use the hash ring to determine which objects belong to "node-a"
        use crate::hash_ring::HashRing;
        let mut ring = HashRing::new();
        ring.add_node("node-a");
        ring.add_node("node-b");

        let ranges_a = ring.get_owned_ranges("node-a");
        let ranges_b = ring.get_owned_ranges("node-b");

        let objects_a = store.objects_in_hash_ranges(&ranges_a);
        let objects_b = store.objects_in_hash_ranges(&ranges_b);

        // Every object should be in exactly one set
        assert_eq!(objects_a.len() + objects_b.len(), 20);

        // Both should have some objects (statistical check)
        assert!(!objects_a.is_empty(), "node-a should own some objects");
        assert!(!objects_b.is_empty(), "node-b should own some objects");

        // Verify chunks are included with their objects
        for (obj, chunks) in &objects_a {
            assert_eq!(chunks.len(), obj.chunks.len());
            for chunk in chunks {
                assert_eq!(chunk.object_name, obj.name);
            }
        }
    }

    #[test]
    fn test_ingest_partition_merges_data() {
        let (mut store, _file) = create_test_store();

        // Store starts with one object
        store.put_object(111, "local-obj".to_string()).unwrap();

        // Ingest migrated data
        let migrated_objects = vec![
            MetaObject {
                name: "migrated-1".to_string(),
                checksum: 222,
                chunks: vec![100, 101],
                total_size: 0,
            },
            MetaObject {
                name: "migrated-2".to_string(),
                checksum: 333,
                chunks: vec![102],
                total_size: 0,
            },
        ];
        let migrated_chunks = vec![
            MetaChunk { id: 100, object_name: "migrated-1".to_string(), checksum: 1000, storage_nodes: vec![1], status: ChunkStatus::Confirmed },
            MetaChunk { id: 101, object_name: "migrated-1".to_string(), checksum: 1001, storage_nodes: vec![1, 2], status: ChunkStatus::Confirmed },
            MetaChunk { id: 102, object_name: "migrated-2".to_string(), checksum: 1002, storage_nodes: vec![1], status: ChunkStatus::Confirmed },
        ];

        store.ingest_partition(migrated_objects, migrated_chunks).unwrap();

        // Local + migrated objects should all be present
        assert_eq!(store.objects.len(), 3);
        assert!(store.get_object("local-obj".to_string()).is_ok());
        assert!(store.get_object("migrated-1".to_string()).is_ok());
        assert!(store.get_object("migrated-2".to_string()).is_ok());

        // Chunks should be present
        assert_eq!(store.chunks.len(), 3);
        let (obj_name, chunk) = store.get_chunk(100).unwrap();
        assert_eq!(obj_name, "migrated-1");
        assert_eq!(chunk.checksum, 1000);
    }

    #[test]
    fn test_remove_objects_cleans_up_chunks() {
        let (mut store, _file) = create_test_store();
        add_storage_node(&mut store, 1, 3000);

        store.put_object(111, "keep-me".to_string()).unwrap();
        store.put_object(222, "remove-me".to_string()).unwrap();
        let (keep_chunk, _) = store.put_chunk("keep-me".to_string(), 1000).unwrap();
        let (remove_chunk, _) = store.put_chunk("remove-me".to_string(), 2000).unwrap();

        store.remove_objects(&["remove-me".to_string()]).unwrap();

        // "keep-me" and its chunk should survive
        assert!(store.get_object("keep-me".to_string()).is_ok());
        assert!(store.get_chunk(keep_chunk).is_ok());

        // "remove-me" and its chunk should be gone
        assert!(store.get_object("remove-me".to_string()).is_err());
        assert!(store.get_chunk(remove_chunk).is_err());
    }

    #[test]
    fn test_new_with_node_uses_node_specific_path() {
        let dir = tempfile::tempdir().unwrap();
        let dir_path = dir.path().to_str().unwrap();

        let mut store = MetadataStore::new_with_node(dir_path, "meta-1").unwrap();
        store.put_object(111, "test-obj".to_string()).unwrap();
        drop(store);

        // Verify file exists at expected path
        let expected_path = format!("{}/metadata-meta-1.dat", dir_path);
        assert!(std::path::Path::new(&expected_path).exists());

        // Reload from same path
        let store2 = MetadataStore::new_with_node(dir_path, "meta-1").unwrap();
        assert!(store2.get_object("test-obj".to_string()).is_ok());

        // Different node_id should have separate empty store
        let store3 = MetadataStore::new_with_node(dir_path, "meta-2").unwrap();
        assert!(store3.objects.is_empty());
    }
}
