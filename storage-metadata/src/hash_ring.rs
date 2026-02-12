use std::collections::BTreeMap;
use xxhash_rust::xxh3::xxh3_64;

const VNODES_PER_NODE: u32 = 150;

/// Consistent hashing ring that maps keys to node IDs.
///
/// Uses a BTreeMap of virtual nodes (vnodes) at xxh3 hash positions.
/// Each physical node gets `VNODES_PER_NODE` positions on the ring.
pub struct HashRing {
    ring: BTreeMap<u64, String>,
}

impl HashRing {
    pub fn new() -> Self {
        Self {
            ring: BTreeMap::new(),
        }
    }

    /// Add a node to the ring with VNODES_PER_NODE virtual nodes.
    pub fn add_node(&mut self, node_id: &str) {
        for i in 0..VNODES_PER_NODE {
            let vnode_key = format!("{}-vnode-{}", node_id, i);
            let hash = xxh3_64(vnode_key.as_bytes());
            self.ring.insert(hash, node_id.to_string());
        }
    }

    /// Remove all virtual nodes for the given node.
    pub fn remove_node(&mut self, node_id: &str) {
        self.ring.retain(|_, v| v != node_id);
    }

    /// Get the node that owns the given key.
    /// Walks clockwise from the key's hash position to find the next vnode.
    pub fn get_owner(&self, key: &str) -> Option<&str> {
        if self.ring.is_empty() {
            return None;
        }
        let hash = xxh3_64(key.as_bytes());
        // Find the first vnode at or after the hash position
        if let Some((_, node_id)) = self.ring.range(hash..).next() {
            return Some(node_id.as_str());
        }
        // Wrap around to the beginning of the ring
        self.ring.values().next().map(|s| s.as_str())
    }

    /// Hash a key to its ring position.
    pub fn hash_key(key: &str) -> u64 {
        xxh3_64(key.as_bytes())
    }

    /// Get the hash ranges owned by a given node.
    /// Returns a list of (start, end) half-open ranges where `start` is inclusive
    /// and `end` is exclusive. These are the ranges where keys hash to positions
    /// whose next clockwise vnode belongs to this node.
    pub fn get_owned_ranges(&self, node_id: &str) -> Vec<(u64, u64)> {
        if self.ring.is_empty() {
            return Vec::new();
        }

        let positions: Vec<(&u64, &String)> = self.ring.iter().collect();
        let len = positions.len();
        let mut ranges = Vec::new();

        for (i, &(pos, ref owner)) in positions.iter().enumerate() {
            if owner.as_str() != node_id {
                continue;
            }
            // This vnode owns the range from the previous vnode's position (exclusive)
            // to this position (inclusive).
            let prev_pos = if i == 0 {
                *positions[len - 1].0
            } else {
                *positions[i - 1].0
            };

            if prev_pos < *pos {
                // Normal range: (prev_pos, pos]
                ranges.push((prev_pos.wrapping_add(1), pos.wrapping_add(1)));
            } else {
                // Wrap-around: (prev_pos, u64::MAX] and [0, pos]
                ranges.push((prev_pos.wrapping_add(1), u64::MAX));
                ranges.push((0, pos.wrapping_add(1)));
            }
        }

        ranges
    }

    /// Check if a hash position falls within any of the given ranges.
    pub fn hash_in_ranges(hash: u64, ranges: &[(u64, u64)]) -> bool {
        for &(start, end) in ranges {
            if start <= end {
                if hash >= start && hash < end {
                    return true;
                }
            } else {
                // Wrap-around range
                if hash >= start || hash < end {
                    return true;
                }
            }
        }
        false
    }

    /// Returns the number of nodes on the ring.
    pub fn node_count(&self) -> usize {
        let mut nodes: Vec<&str> = self.ring.values().map(|s| s.as_str()).collect();
        nodes.sort();
        nodes.dedup();
        nodes.len()
    }

    /// Returns true if the ring is empty.
    pub fn is_empty(&self) -> bool {
        self.ring.is_empty()
    }

    /// Returns all unique node IDs on the ring.
    pub fn nodes(&self) -> Vec<String> {
        let mut nodes: Vec<String> = self.ring.values().cloned().collect();
        nodes.sort();
        nodes.dedup();
        nodes
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    #[test]
    fn test_empty_ring_returns_none() {
        let ring = HashRing::new();
        assert!(ring.get_owner("any_key").is_none());
        assert!(ring.is_empty());
    }

    #[test]
    fn test_single_node_owns_everything() {
        let mut ring = HashRing::new();
        ring.add_node("node-1");

        assert_eq!(ring.get_owner("key-a"), Some("node-1"));
        assert_eq!(ring.get_owner("key-z"), Some("node-1"));
        assert_eq!(ring.get_owner("any-random-key"), Some("node-1"));
        assert_eq!(ring.node_count(), 1);
    }

    #[test]
    fn test_consistent_hashing_deterministic() {
        let mut ring = HashRing::new();
        ring.add_node("node-1");
        ring.add_node("node-2");
        ring.add_node("node-3");

        let owner1 = ring.get_owner("test-key");
        let owner2 = ring.get_owner("test-key");
        assert_eq!(owner1, owner2);
    }

    #[test]
    fn test_distribution_across_nodes() {
        let mut ring = HashRing::new();
        ring.add_node("node-1");
        ring.add_node("node-2");
        ring.add_node("node-3");

        let mut counts: HashMap<&str, usize> = HashMap::new();
        for i in 0..1000 {
            let key = format!("object-{}", i);
            let owner = ring.get_owner(&key).unwrap();
            *counts.entry(owner).or_insert(0) += 1;
        }

        // Each node should own a reasonable portion (at least 15% of 1000 = 150)
        assert_eq!(counts.len(), 3);
        for (_, count) in &counts {
            assert!(*count > 150, "Expected better distribution, got {:?}", counts);
        }
    }

    #[test]
    fn test_add_node_minimal_redistribution() {
        let mut ring = HashRing::new();
        ring.add_node("node-1");
        ring.add_node("node-2");

        // Record owners for many keys
        let keys: Vec<String> = (0..1000).map(|i| format!("key-{}", i)).collect();
        let original_owners: Vec<Option<String>> = keys
            .iter()
            .map(|k| ring.get_owner(k).map(|s| s.to_string()))
            .collect();

        // Add a third node
        ring.add_node("node-3");
        let new_owners: Vec<Option<String>> = keys
            .iter()
            .map(|k| ring.get_owner(k).map(|s| s.to_string()))
            .collect();

        // Count how many keys changed owner
        let changed = original_owners
            .iter()
            .zip(new_owners.iter())
            .filter(|(a, b)| a != b)
            .count();

        // With consistent hashing, roughly 1/3 of keys should move
        // Allow generous range: 10-60%
        let pct = (changed as f64 / keys.len() as f64) * 100.0;
        assert!(
            pct > 10.0 && pct < 60.0,
            "Expected ~33% redistribution, got {:.1}%",
            pct
        );
    }

    #[test]
    fn test_remove_node() {
        let mut ring = HashRing::new();
        ring.add_node("node-1");
        ring.add_node("node-2");
        ring.add_node("node-3");

        assert_eq!(ring.node_count(), 3);

        ring.remove_node("node-2");
        assert_eq!(ring.node_count(), 2);

        // All keys should still resolve
        for i in 0..100 {
            let key = format!("key-{}", i);
            let owner = ring.get_owner(&key).unwrap();
            assert!(owner == "node-1" || owner == "node-3");
        }
    }

    #[test]
    fn test_get_owned_ranges_covers_ring() {
        let mut ring = HashRing::new();
        ring.add_node("node-1");
        ring.add_node("node-2");

        let ranges1 = ring.get_owned_ranges("node-1");
        let ranges2 = ring.get_owned_ranges("node-2");

        // Both nodes should have ranges
        assert!(!ranges1.is_empty());
        assert!(!ranges2.is_empty());
    }

    #[test]
    fn test_nodes_returns_all_unique_ids() {
        let mut ring = HashRing::new();
        ring.add_node("node-a");
        ring.add_node("node-b");
        ring.add_node("node-c");

        let mut nodes = ring.nodes();
        nodes.sort();
        assert_eq!(nodes, vec!["node-a", "node-b", "node-c"]);
    }

    #[test]
    fn test_hash_key_deterministic() {
        let h1 = HashRing::hash_key("test");
        let h2 = HashRing::hash_key("test");
        assert_eq!(h1, h2);

        let h3 = HashRing::hash_key("different");
        assert_ne!(h1, h3);
    }

    #[test]
    fn test_hash_in_ranges_normal_range() {
        // Normal range: [10, 20)
        assert!(HashRing::hash_in_ranges(10, &[(10, 20)]));
        assert!(HashRing::hash_in_ranges(15, &[(10, 20)]));
        assert!(!HashRing::hash_in_ranges(20, &[(10, 20)]));
        assert!(!HashRing::hash_in_ranges(5, &[(10, 20)]));
    }

    #[test]
    fn test_hash_in_ranges_multiple() {
        let ranges = vec![(10, 20), (50, 60)];
        assert!(HashRing::hash_in_ranges(15, &ranges));
        assert!(HashRing::hash_in_ranges(55, &ranges));
        assert!(!HashRing::hash_in_ranges(30, &ranges));
    }

    /// Simulate a 3-node cluster where a new node joins and needs to pull data.
    /// Verifies that the ring correctly partitions keys, and that adding a node
    /// only moves the expected fraction of keys.
    #[test]
    fn test_three_node_cluster_simulation() {
        let mut ring = HashRing::new();
        ring.add_node("meta-1");
        ring.add_node("meta-2");
        ring.add_node("meta-3");

        // Simulate 100 objects assigned to owners
        let mut ownership: HashMap<String, Vec<String>> = HashMap::new();
        for i in 0..100 {
            let obj_name = format!("object-{}", i);
            let owner = ring.get_owner(&obj_name).unwrap().to_string();
            ownership.entry(owner).or_default().push(obj_name);
        }

        // All 3 nodes should own something
        assert_eq!(ownership.len(), 3);
        for (_node, objects) in &ownership {
            assert!(!objects.is_empty());
        }

        // Now add a 4th node — simulating a new node joining
        ring.add_node("meta-4");

        let mut new_ownership: HashMap<String, Vec<String>> = HashMap::new();
        for i in 0..100 {
            let obj_name = format!("object-{}", i);
            let owner = ring.get_owner(&obj_name).unwrap().to_string();
            new_ownership.entry(owner).or_default().push(obj_name);
        }

        // meta-4 should now own some objects
        assert!(new_ownership.contains_key("meta-4"));
        assert!(new_ownership["meta-4"].len() > 5); // should be roughly 25

        // Objects that stayed with their original owner should still be there
        let total: usize = new_ownership.values().map(|v| v.len()).sum();
        assert_eq!(total, 100);
    }

    /// Verifies that get_owned_ranges + hash_in_ranges correctly identifies
    /// which node owns each key. Every key should match exactly one node's ranges.
    #[test]
    fn test_owned_ranges_cover_all_keys() {
        let mut ring = HashRing::new();
        ring.add_node("node-1");
        ring.add_node("node-2");
        ring.add_node("node-3");

        let ranges_1 = ring.get_owned_ranges("node-1");
        let ranges_2 = ring.get_owned_ranges("node-2");
        let ranges_3 = ring.get_owned_ranges("node-3");

        for i in 0..200 {
            let key = format!("test-key-{}", i);
            let hash = HashRing::hash_key(&key);
            let owner = ring.get_owner(&key).unwrap();

            let in_1 = HashRing::hash_in_ranges(hash, &ranges_1);
            let in_2 = HashRing::hash_in_ranges(hash, &ranges_2);
            let in_3 = HashRing::hash_in_ranges(hash, &ranges_3);

            // Key should be in exactly one node's ranges
            let matches: Vec<(&str, bool)> = vec![
                ("node-1", in_1),
                ("node-2", in_2),
                ("node-3", in_3),
            ];
            let matched: Vec<&str> = matches.iter()
                .filter(|(_, b)| *b)
                .map(|(n, _)| *n)
                .collect();

            assert_eq!(
                matched.len(), 1,
                "Key '{}' (hash {}) matched {} nodes: {:?}, expected owner: {}",
                key, hash, matched.len(), matched, owner
            );
            assert_eq!(
                matched[0], owner,
                "Key '{}' range owner {:?} doesn't match ring owner {}",
                key, matched[0], owner
            );
        }
    }

    /// Test the migration scenario: when a new node joins, we can identify
    /// which ranges it needs to pull from existing nodes.
    #[test]
    fn test_migration_range_computation() {
        // Start with 2-node ring
        let mut ring_before = HashRing::new();
        ring_before.add_node("existing-1");
        ring_before.add_node("existing-2");

        // Add new node
        let mut ring_after = HashRing::new();
        ring_after.add_node("existing-1");
        ring_after.add_node("existing-2");
        ring_after.add_node("new-node");

        // Get ranges the new node owns in the new ring
        let new_ranges = ring_after.get_owned_ranges("new-node");
        assert!(!new_ranges.is_empty());

        // For some test keys that the new node now owns, verify they were
        // previously owned by one of the existing nodes
        let mut moved_from_1 = 0;
        let mut moved_from_2 = 0;
        for i in 0..500 {
            let key = format!("obj-{}", i);
            let new_owner = ring_after.get_owner(&key).unwrap();
            if new_owner == "new-node" {
                let old_owner = ring_before.get_owner(&key).unwrap();
                match old_owner {
                    "existing-1" => moved_from_1 += 1,
                    "existing-2" => moved_from_2 += 1,
                    _ => panic!("unexpected old owner"),
                }
            }
        }

        // Both existing nodes should have given up some keys to the new node
        assert!(moved_from_1 > 0, "expected keys to move from existing-1");
        assert!(moved_from_2 > 0, "expected keys to move from existing-2");
    }
}
