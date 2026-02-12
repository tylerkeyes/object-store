use serde::Deserialize;
use std::env;
use std::path::Path;

#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct StorageNodeConfig {
    #[serde(rename = "metadata-addresses")]
    pub metadata_addresses: Vec<String>,

    pub port: u16,

    #[serde(rename = "metrics-port")]
    pub metrics_port: u16,

    #[serde(rename = "chunk-store-path")]
    pub chunk_store_path: String,

    #[serde(rename = "allocated-slots")]
    pub allocated_slots: Option<usize>,

    #[serde(rename = "fsync-every-n-writes")]
    pub fsync_every_n_writes: Option<u64>,

    #[serde(rename = "fsync-on-delete")]
    pub fsync_on_delete: bool,

    #[serde(rename = "fast-recover")]
    pub fast_recover: bool,
}

impl Default for StorageNodeConfig {
    fn default() -> Self {
        Self {
            metadata_addresses: vec!["http://localhost:3001".to_string()],
            port: 3000,
            metrics_port: 9092,
            chunk_store_path: "chunkstore.dat".to_string(),
            allocated_slots: None,
            fsync_every_n_writes: Some(crate::chunk_store::DEFAULT_FSYNC_EVERY_N_WRITES),
            fsync_on_delete: true,
            fast_recover: false,
        }
    }
}

impl StorageNodeConfig {
    /// Load configuration from a YAML file with validation and environment variable overrides
    pub fn load<P: AsRef<Path>>(path: P) -> Result<Self, Box<dyn std::error::Error>> {
        let contents = std::fs::read_to_string(path.as_ref())?;
        let mut config: StorageNodeConfig = serde_yaml::from_str(&contents)?;

        // Normalize addresses to include http:// prefix
        config.metadata_addresses = config
            .metadata_addresses
            .into_iter()
            .map(|addr| {
                if addr.starts_with("http://") || addr.starts_with("https://") {
                    addr
                } else {
                    format!("http://{}", addr)
                }
            })
            .collect();

        // Apply environment variable overrides
        config.apply_env_overrides();

        // Validate the final configuration
        config.validate()?;

        Ok(config)
    }

    /// Validate configuration values
    fn validate(&self) -> Result<(), Box<dyn std::error::Error>> {
        if self.metadata_addresses.is_empty() {
            return Err("metadata-addresses cannot be empty".into());
        }

        for addr in &self.metadata_addresses {
            if addr.trim().is_empty() {
                return Err("metadata-addresses cannot contain empty strings".into());
            }
        }

        if self.port == 0 {
            return Err("port cannot be 0".into());
        }

        if self.metrics_port == 0 {
            return Err("metrics-port cannot be 0".into());
        }

        Ok(())
    }

    /// Apply environment variable overrides
    fn apply_env_overrides(&mut self) {
        // Override metadata addresses from METADATA_URLS (comma-separated)
        if let Ok(urls) = env::var("METADATA_URLS") {
            self.metadata_addresses = urls
                .split(',')
                .map(|s| {
                    let addr = s.trim().to_string();
                    if addr.starts_with("http://") || addr.starts_with("https://") {
                        addr
                    } else {
                        format!("http://{}", addr)
                    }
                })
                .collect();
        }

        // Override gRPC port
        if let Ok(port_str) = env::var("GRPC_PORT") {
            if let Ok(port) = port_str.parse::<u16>() {
                self.port = port;
            }
        }

        // Override metrics port
        if let Ok(port_str) = env::var("METRICS_PORT") {
            if let Ok(port) = port_str.parse::<u16>() {
                self.metrics_port = port;
            }
        }

        // Override allocated slots
        if let Ok(slots_str) = env::var("ALLOCATED_SLOTS") {
            if let Ok(slots) = slots_str.parse::<usize>() {
                self.allocated_slots = Some(slots);
            }
        }

        // Override fsync every N writes (0 disables)
        if let Ok(val) = env::var("FSYNC_EVERY_N_WRITES") {
            if let Ok(n) = val.parse::<u64>() {
                self.fsync_every_n_writes = Some(n);
            }
        }

        // Override fsync on delete
        if let Ok(val) = env::var("FSYNC_ON_DELETE") {
            if let Ok(enabled) = val.parse::<bool>() {
                self.fsync_on_delete = enabled;
            }
        }

        // Override fast recover
        if let Ok(val) = env::var("FAST_RECOVER") {
            if let Ok(enabled) = val.parse::<bool>() {
                self.fast_recover = enabled;
            }
        }
    }

    pub fn chunk_store_options(&self) -> crate::chunk_store::ChunkStoreOptions {
        crate::chunk_store::ChunkStoreOptions {
            fsync_every_n_writes: self.fsync_every_n_writes,
            fsync_on_delete: self.fsync_on_delete,
            fast_recover: self.fast_recover,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serial_test::serial;
    use std::io::Write;

    fn create_test_config(content: &str) -> tempfile::NamedTempFile {
        let mut file = tempfile::NamedTempFile::new().unwrap();
        file.write_all(content.as_bytes()).unwrap();
        file.flush().unwrap();
        file
    }

    #[test]
    #[serial]
    fn test_load_valid_config() {
        // Clean up any environment variables from other tests
        unsafe {
            env::remove_var("METADATA_URLS");
            env::remove_var("GRPC_PORT");
            env::remove_var("METRICS_PORT");
        }

        let config_content = r#"
metadata-addresses:
  - http://localhost:3001
port: 3000
metrics-port: 9092
chunk-store-path: "chunkstore.dat"
"#;
        let file = create_test_config(config_content);
        let config = StorageNodeConfig::load(file.path()).unwrap();

        assert_eq!(config.metadata_addresses, vec!["http://localhost:3001"]);
        assert_eq!(config.port, 3000);
        assert_eq!(config.metrics_port, 9092);
        assert_eq!(config.chunk_store_path, "chunkstore.dat");
    }

    #[test]
    fn test_default_values() {
        let config = StorageNodeConfig::default();

        assert_eq!(config.metadata_addresses, vec!["http://localhost:3001"]);
        assert_eq!(config.port, 3000);
        assert_eq!(config.metrics_port, 9092);
        assert_eq!(config.chunk_store_path, "chunkstore.dat");
    }

    #[test]
    fn test_validation_empty_addresses() {
        let mut config = StorageNodeConfig::default();
        config.metadata_addresses = vec![];

        let result = config.validate();
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("cannot be empty"));
    }

    #[test]
    fn test_validation_zero_port() {
        let mut config = StorageNodeConfig::default();
        config.port = 0;

        let result = config.validate();
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("port cannot be 0"));
    }

    #[test]
    fn test_validation_zero_metrics_port() {
        let mut config = StorageNodeConfig::default();
        config.metrics_port = 0;

        let result = config.validate();
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("metrics-port cannot be 0"));
    }

    #[test]
    #[serial]
    fn test_address_normalization() {
        // Clean up any environment variables from other tests
        unsafe {
            env::remove_var("METADATA_URLS");
            env::remove_var("GRPC_PORT");
            env::remove_var("METRICS_PORT");
        }

        let config_content = r#"
metadata-addresses:
  - localhost:3001
  - http://metadata:3001
port: 3000
metrics-port: 9092
chunk-store-path: "chunkstore.dat"
"#;
        let file = create_test_config(config_content);
        let config = StorageNodeConfig::load(file.path()).unwrap();

        assert_eq!(
            config.metadata_addresses,
            vec!["http://localhost:3001", "http://metadata:3001"]
        );
    }

    #[test]
    #[serial]
    fn test_env_override_metadata_urls() {
        // Clean up before and after
        unsafe {
            env::remove_var("METADATA_URLS");
            env::remove_var("GRPC_PORT");
            env::remove_var("METRICS_PORT");
        }

        let config_content = r#"
metadata-addresses:
  - http://localhost:3001
port: 3000
metrics-port: 9092
chunk-store-path: "chunkstore.dat"
"#;
        let file = create_test_config(config_content);

        unsafe {
            env::set_var("METADATA_URLS", "http://server1:3001,http://server2:3001");
        }
        let config = StorageNodeConfig::load(file.path()).unwrap();
        unsafe {
            env::remove_var("METADATA_URLS");
            env::remove_var("GRPC_PORT");
            env::remove_var("METRICS_PORT");
        }

        assert_eq!(
            config.metadata_addresses,
            vec!["http://server1:3001", "http://server2:3001"]
        );
    }

    #[test]
    #[serial]
    fn test_env_override_ports() {
        // Clean up before and after
        unsafe {
            env::remove_var("METADATA_URLS");
            env::remove_var("GRPC_PORT");
            env::remove_var("METRICS_PORT");
        }

        let config_content = r#"
metadata-addresses:
  - http://localhost:3001
port: 3000
metrics-port: 9092
chunk-store-path: "chunkstore.dat"
"#;
        let file = create_test_config(config_content);

        unsafe {
            env::set_var("GRPC_PORT", "4000");
            env::set_var("METRICS_PORT", "9999");
        }
        let config = StorageNodeConfig::load(file.path()).unwrap();
        unsafe {
            env::remove_var("METADATA_URLS");
            env::remove_var("GRPC_PORT");
            env::remove_var("METRICS_PORT");
        }

        assert_eq!(config.port, 4000);
        assert_eq!(config.metrics_port, 9999);
    }
}
