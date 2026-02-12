#!/usr/bin/env python3
"""
E2E Test Script for Distributed Object Storage System

Starts all services (Cargo preferred, Bazel fallback), runs tests, and cleans up.

Usage:
    python e2e_test.py [--project-root /path/to/object-store] [--timeout 60] [--clean] [--use-bazel]
"""

import argparse
import atexit
import base64
import glob
import os
import shutil
import signal
import socket
import subprocess
import sys
import time
from typing import Optional

import requests


# =============================================================================
# Service Manager
# =============================================================================

class ServiceManager:
    """Manages starting and stopping services using Bazel or Cargo."""

    def __init__(self, project_root: str, prefer_bazel: bool = False):
        self.project_root = os.path.abspath(project_root)
        self.processes: dict[str, subprocess.Popen] = {}
        self.use_bazel = prefer_bazel and self._check_bazel_available()
        self._cleanup_registered = False

    def _check_bazel_available(self) -> bool:
        """Check if Bazel is available and project has MODULE.bazel."""
        if shutil.which("bazel") is None:
            return False
        module_file = os.path.join(self.project_root, "MODULE.bazel")
        return os.path.exists(module_file)

    def _register_cleanup(self):
        """Register cleanup handlers once."""
        if self._cleanup_registered:
            return
        self._cleanup_registered = True
        atexit.register(self.stop_all)
        signal.signal(signal.SIGTERM, lambda *_: self._signal_cleanup())
        signal.signal(signal.SIGINT, lambda *_: self._signal_cleanup())

    def _signal_cleanup(self):
        """Handle signal-based cleanup."""
        self.stop_all()
        sys.exit(1)

    def start_service(
        self,
        name: str,
        bazel_target: str,
        cargo_package: str,
        env_vars: Optional[dict] = None,
    ) -> subprocess.Popen:
        """Start a service using Bazel or Cargo."""
        self._register_cleanup()

        if self.use_bazel:
            cmd = ["bazel", "run", bazel_target]
            mode = "bazel"
        else:
            cmd = ["cargo", "run", "-p", cargo_package, "--release"]
            mode = "cargo"

        env = os.environ.copy()
        if env_vars:
            env.update(env_vars)

        proc = subprocess.Popen(
            cmd,
            env=env,
            cwd=self.project_root,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            preexec_fn=os.setsid if os.name != "nt" else None,
        )
        self.processes[name] = proc
        print(f"    Started {name} via {mode} (pid={proc.pid})")
        return proc

    def stop_service(self, name: str):
        """Stop a specific service."""
        if name not in self.processes:
            return
        proc = self.processes[name]
        if proc.poll() is None:  # Still running
            try:
                if os.name != "nt":
                    os.killpg(os.getpgid(proc.pid), signal.SIGTERM)
                else:
                    proc.terminate()
                proc.wait(timeout=5)
            except (ProcessLookupError, subprocess.TimeoutExpired):
                try:
                    if os.name != "nt":
                        os.killpg(os.getpgid(proc.pid), signal.SIGKILL)
                    else:
                        proc.kill()
                except ProcessLookupError:
                    pass
        del self.processes[name]

    def stop_all(self):
        """Stop all running services."""
        for name in list(self.processes.keys()):
            self.stop_service(name)

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.stop_all()


# =============================================================================
# Health Check Utilities
# =============================================================================

def wait_for_port(host: str, port: int, timeout: float = 30.0) -> bool:
    """Wait for a TCP port to be available."""
    start = time.time()
    while time.time() - start < timeout:
        try:
            with socket.create_connection((host, port), timeout=1):
                return True
        except (socket.timeout, ConnectionRefusedError, OSError):
            time.sleep(0.5)
    return False


def wait_for_http_health(url: str, timeout: float = 30.0) -> bool:
    """Wait for HTTP health endpoint to return 200."""
    start = time.time()
    while time.time() - start < timeout:
        try:
            resp = requests.get(url, timeout=2)
            if resp.status_code == 200:
                return True
        except requests.RequestException:
            pass
        time.sleep(0.5)
    return False


# =============================================================================
# API Client
# =============================================================================

class ObjectStoreClient:
    """HTTP client for the object storage API gateway."""

    def __init__(self, base_url: str = "http://localhost:8080", timeout: float = 30.0):
        self.base_url = base_url.rstrip("/")
        self.timeout = timeout
        self.session = requests.Session()

    def health_check(self) -> bool:
        """GET /health - returns True if healthy."""
        resp = self.session.get(f"{self.base_url}/health", timeout=self.timeout)
        return resp.status_code == 200 and resp.text == "OK"

    def put_object(self, name: str, data: bytes) -> requests.Response:
        """PUT /{objectName} with JSON body {"bytes": "base64data"}."""
        encoded = base64.b64encode(data).decode("utf-8")
        return self.session.put(
            f"{self.base_url}/{name}",
            json={"bytes": encoded},
            headers={"Content-Type": "application/json"},
            timeout=self.timeout,
        )

    def get_object(self, name: str) -> requests.Response:
        """GET /{objectName} - returns JSON {"bytes": "base64data"}."""
        return self.session.get(
            f"{self.base_url}/{name}",
            headers={"Accept": "application/json"},
            timeout=self.timeout,
        )

    def get_object_data(self, name: str) -> Optional[bytes]:
        """GET object and decode the base64 data."""
        resp = self.get_object(name)
        if resp.status_code == 200:
            return base64.b64decode(resp.json()["bytes"])
        return None

    def list_objects(self) -> requests.Response:
        """GET / - returns list of objects."""
        return self.session.get(f"{self.base_url}/", timeout=self.timeout)

    def delete_object(self, name: str) -> requests.Response:
        """DELETE /{objectName}."""
        return self.session.delete(f"{self.base_url}/{name}", timeout=self.timeout)

    def put_raw(self, name: str, json_body: dict) -> requests.Response:
        """PUT /{objectName} with a raw JSON body (no automatic base64 encoding)."""
        return self.session.put(
            f"{self.base_url}/{name}",
            json=json_body,
            headers={"Content-Type": "application/json"},
            timeout=self.timeout,
        )

    def get_metrics(self) -> requests.Response:
        """GET /metrics - returns Prometheus metrics."""
        return self.session.get(f"{self.base_url}/metrics", timeout=self.timeout)

    def put_object_multipart(self, name: str, data: bytes) -> requests.Response:
        """PUT /{objectName} with multipart/form-data file upload (streaming)."""
        return self.session.put(
            f"{self.base_url}/{name}",
            files={"file": ("data.bin", data, "application/octet-stream")},
            timeout=self.timeout,
        )

    def get_object_stream(self, name: str) -> requests.Response:
        """GET /{objectName} as binary stream (no Accept: application/json)."""
        return self.session.get(
            f"{self.base_url}/{name}",
            headers={"Accept": "application/octet-stream"},
            timeout=self.timeout,
        )


# =============================================================================
# Test Scenarios
# =============================================================================

def test_health_check(client: ObjectStoreClient):
    """Test 1: Health check endpoint."""
    assert client.health_check(), "Health check failed"


def test_upload_object(client: ObjectStoreClient):
    """Test 2: Upload object (PUT)."""
    data = b"hello world"
    resp = client.put_object("test-object", data)
    assert resp.status_code == 200, f"Upload failed: {resp.text}"
    body = resp.json()
    assert body["msg"] == "upload complete"
    assert body["decoded_bytes_len"] == len(data)
    # Cleanup
    client.delete_object("test-object")


def test_download_object(client: ObjectStoreClient):
    """Test 3: Download object (GET)."""
    expected_data = b"hello world download test"
    client.put_object("download-test", expected_data)

    actual_data = client.get_object_data("download-test")
    assert actual_data == expected_data, "Downloaded data mismatch"
    # Cleanup
    client.delete_object("download-test")


def test_list_objects(client: ObjectStoreClient):
    """Test 4: List objects (GET /)."""
    # Create test objects
    client.put_object("list-test-1", b"data1")
    client.put_object("list-test-2", b"data2")

    resp = client.list_objects()
    assert resp.status_code == 200, f"List failed: {resp.text}"
    objects = resp.json()
    names = [obj["name"] for obj in objects]
    assert "list-test-1" in names, "list-test-1 not found in list"
    assert "list-test-2" in names, "list-test-2 not found in list"
    # Cleanup
    client.delete_object("list-test-1")
    client.delete_object("list-test-2")


def test_delete_object(client: ObjectStoreClient):
    """Test 5: Delete object (DELETE)."""
    client.put_object("delete-test", b"to be deleted")

    resp = client.delete_object("delete-test")
    assert resp.status_code == 200, f"Delete failed: {resp.text}"
    assert resp.json()["msg"] == "object deleted"

    # Verify deletion
    get_resp = client.get_object("delete-test")
    assert get_resp.status_code == 404, f"Expected 404 after delete, got {get_resp.status_code}"


def test_duplicate_upload_conflict(client: ObjectStoreClient):
    """Test 6: Duplicate upload returns 409 Conflict."""
    client.put_object("duplicate-test", b"first upload")

    resp = client.put_object("duplicate-test", b"second upload")
    assert resp.status_code == 409, f"Expected 409, got {resp.status_code}"
    # Cleanup
    client.delete_object("duplicate-test")


def test_download_nonexistent_404(client: ObjectStoreClient):
    """Test 7: Download non-existent object returns 404."""
    resp = client.get_object("nonexistent-object-xyz-12345")
    assert resp.status_code == 404, f"Expected 404, got {resp.status_code}"


def test_update_flow_delete_reupload(client: ObjectStoreClient):
    """Test 8: Update flow - delete then re-upload."""
    # Initial upload
    client.put_object("update-test", b"version 1")

    # Verify initial data
    data_v1 = client.get_object_data("update-test")
    assert data_v1 == b"version 1", "Initial data mismatch"

    # Delete
    client.delete_object("update-test")

    # Re-upload with new data
    resp = client.put_object("update-test", b"version 2")
    assert resp.status_code == 200, f"Re-upload failed: {resp.text}"

    # Verify new data
    actual = client.get_object_data("update-test")
    assert actual == b"version 2", "Updated data mismatch"
    # Cleanup
    client.delete_object("update-test")


def test_metrics_endpoint(client: ObjectStoreClient):
    """Test: Metrics endpoint returns Prometheus metrics."""
    resp = client.get_metrics()
    assert resp.status_code == 200, f"Metrics endpoint failed: {resp.status_code}"
    text = resp.text
    assert "api_gateway_http_requests_total" in text, "Expected api_gateway_http_requests_total metric"


def test_multi_chunk_upload_download(client: ObjectStoreClient):
    """Test: Upload and download a multi-chunk object (>8MB)."""
    # SLOT_DATA_SIZE = 8*1024*1024 - 24 = 8388584 bytes per chunk
    # Create data slightly larger than one chunk to force 2 chunks
    chunk_data_size = 8 * 1024 * 1024 - 24
    data = (b"ABCDEFGHIJ" * ((chunk_data_size + 1024) // 10 + 1))[:chunk_data_size + 1024]

    resp = client.put_object("multi-chunk-test", data)
    assert resp.status_code == 200, f"Multi-chunk upload failed: {resp.text}"
    body = resp.json()
    assert body["decoded_bytes_len"] == len(data), "Decoded bytes len mismatch"

    # Download and verify exact byte equality
    actual = client.get_object_data("multi-chunk-test")
    assert actual == data, "Multi-chunk downloaded data mismatch"

    # Verify via list endpoint that object has 2 chunks
    list_resp = client.list_objects()
    assert list_resp.status_code == 200
    objects = list_resp.json()
    obj = next((o for o in objects if o["name"] == "multi-chunk-test"), None)
    assert obj is not None, "multi-chunk-test not found in list"
    assert len(obj["chunks"]) == 2, f"Expected 2 chunks, got {len(obj['chunks'])}"

    # Cleanup
    client.delete_object("multi-chunk-test")


def test_empty_object(client: ObjectStoreClient):
    """Test: Upload and download an empty (0-byte) object."""
    resp = client.put_object("empty-test", b"")
    assert resp.status_code == 200, f"Empty upload failed: {resp.text}"
    body = resp.json()
    assert body["decoded_bytes_len"] == 0, "Expected 0 bytes"

    actual = client.get_object_data("empty-test")
    assert actual == b"", "Expected empty bytes on download"

    # Cleanup
    client.delete_object("empty-test")


def test_valid_special_object_names(client: ObjectStoreClient):
    """Test: Upload/download objects with valid special characters in names."""
    names = ["dotted.name.txt", "dashed-name", "under_scored"]
    data = b"special name test data"

    for name in names:
        resp = client.put_object(name, data)
        assert resp.status_code == 200, f"Upload failed for '{name}': {resp.text}"

        actual = client.get_object_data(name)
        assert actual == data, f"Data mismatch for '{name}'"

        client.delete_object(name)


def test_delete_nonexistent_404(client: ObjectStoreClient):
    """Test: Delete a non-existent object returns 404."""
    resp = client.delete_object("never-created-object-xyz-99999")
    assert resp.status_code == 404, f"Expected 404, got {resp.status_code}"


def test_invalid_object_names(client: ObjectStoreClient):
    """Test: Invalid object names are rejected with 400."""
    invalid_names = [
        "has space",
        "has@symbol",
        "has!bang",
    ]
    for name in invalid_names:
        resp = client.put_object(name, b"data")
        assert resp.status_code == 400, f"Expected 400 for '{name}', got {resp.status_code}"


def test_invalid_base64(client: ObjectStoreClient):
    """Test: Malformed base64 in request body returns 400."""
    resp = client.put_raw("invalid-b64-test", {"bytes": "not-valid-base64!!!"})
    assert resp.status_code == 400, f"Expected 400 for invalid base64, got {resp.status_code}"


def test_binary_data_roundtrip(client: ObjectStoreClient):
    """Test: Binary data with all 256 byte values survives upload/download."""
    # Create data containing all 256 byte values (includes non-UTF-8 bytes)
    data = bytes(range(256)) * 4

    resp = client.put_object("binary-roundtrip-test", data)
    assert resp.status_code == 200, f"Binary upload failed: {resp.text}"

    actual = client.get_object_data("binary-roundtrip-test")
    assert actual == data, "Binary data roundtrip mismatch"

    # Cleanup
    client.delete_object("binary-roundtrip-test")


def test_multipart_upload(client: ObjectStoreClient):
    """Test: Upload via multipart/form-data and download via JSON."""
    data = b"multipart upload test data"

    resp = client.put_object_multipart("multipart-test", data)
    assert resp.status_code == 200, f"Multipart upload failed: {resp.text}"
    body = resp.json()
    assert body["msg"] == "upload complete"
    assert body["decoded_bytes_len"] == len(data)

    # Download via JSON API and verify
    actual = client.get_object_data("multipart-test")
    assert actual == data, "Multipart upload data mismatch on JSON download"

    # Cleanup
    client.delete_object("multipart-test")


def test_streaming_download(client: ObjectStoreClient):
    """Test: Upload via JSON and download via streaming binary."""
    data = b"streaming download test data"

    resp = client.put_object("stream-dl-test", data)
    assert resp.status_code == 200, f"Upload failed: {resp.text}"

    # Download via streaming endpoint
    stream_resp = client.get_object_stream("stream-dl-test")
    assert stream_resp.status_code == 200, f"Streaming download failed: {stream_resp.status_code}"
    assert stream_resp.content == data, "Streaming download data mismatch"

    # Cleanup
    client.delete_object("stream-dl-test")


def test_multipart_upload_streaming_download_roundtrip(client: ObjectStoreClient):
    """Test: Upload via multipart, download via streaming binary - full streaming roundtrip."""
    data = bytes(range(256)) * 100  # 25.6 KB of binary data

    resp = client.put_object_multipart("stream-roundtrip-test", data)
    assert resp.status_code == 200, f"Multipart upload failed: {resp.text}"

    stream_resp = client.get_object_stream("stream-roundtrip-test")
    assert stream_resp.status_code == 200, f"Streaming download failed: {stream_resp.status_code}"
    assert stream_resp.content == data, "Streaming roundtrip data mismatch"

    # Cleanup
    client.delete_object("stream-roundtrip-test")


# =============================================================================
# Data Cleanup
# =============================================================================

def clean_data_files(project_root: str):
    """Remove data files for a fresh test state."""
    files_to_remove = [
        "metadata.dat",
        "chunkstore.dat",
        "storage-metadata/metadata.dat",
        "storage-node/chunkstore.dat",
    ]
    # Also clean up cluster-mode metadata files
    for i in range(1, 4):
        files_to_remove.append(f"metadata-meta-{i}.dat")
    for f in files_to_remove:
        path = os.path.join(project_root, f)
        if os.path.exists(path):
            os.remove(path)
            print(f"  Removed {f}")
    # Remove clustered/partitioned metadata files like metadata-<node_id>.dat
    for path in glob.glob(os.path.join(project_root, "metadata-*.dat")):
        os.remove(path)
        print(f"  Removed {os.path.relpath(path, project_root)}")


# =============================================================================
# Main Orchestrator
# =============================================================================

def start_single_metadata(manager, project_root, timeout):
    """Start a single metadata instance (standalone mode)."""
    print("  Starting storage-metadata...")
    manager.start_service(
        name="storage-metadata",
        bazel_target="//storage-metadata:storage_metadata",
        cargo_package="storage-metadata",
    )
    if not wait_for_port("localhost", 3001, timeout=timeout):
        print("ERROR: storage-metadata failed to start (port 3001 not available)")
        return None
    print("  storage-metadata is ready (port 3001)")
    return "http://localhost:3001"


def start_cluster_metadata(manager, project_root, timeout):
    """Start 3 metadata instances in cluster mode."""
    metadata_configs = [
        {
            "name": "storage-metadata-1",
            "node_id": "meta-1",
            "grpc_port": "3001",
            "gossip_port": "3011",
            "metrics_port": "9091",
            "seed_nodes": "127.0.0.1:3012,127.0.0.1:3013",
        },
        {
            "name": "storage-metadata-2",
            "node_id": "meta-2",
            "grpc_port": "3002",
            "gossip_port": "3012",
            "metrics_port": "9191",
            "seed_nodes": "127.0.0.1:3011,127.0.0.1:3013",
        },
        {
            "name": "storage-metadata-3",
            "node_id": "meta-3",
            "grpc_port": "3003",
            "gossip_port": "3013",
            "metrics_port": "9291",
            "seed_nodes": "127.0.0.1:3011,127.0.0.1:3012",
        },
    ]

    data_dir = os.path.join(project_root, "test-cluster-data")
    os.makedirs(data_dir, exist_ok=True)

    for cfg in metadata_configs:
        print(f"  Starting {cfg['name']}...")
        manager.start_service(
            name=cfg["name"],
            bazel_target="//storage-metadata:storage_metadata",
            cargo_package="storage-metadata",
            env_vars={
                "NODE_ID": cfg["node_id"],
                "GRPC_PORT": cfg["grpc_port"],
                "GOSSIP_PORT": cfg["gossip_port"],
                "METRICS_PORT": cfg["metrics_port"],
                "SEED_NODES": cfg["seed_nodes"],
                "DATA_DIR": data_dir,
                "RUST_LOG": "info",
            },
        )

    # Wait for all 3 to be ready
    for cfg in metadata_configs:
        port = int(cfg["grpc_port"])
        if not wait_for_port("localhost", port, timeout=timeout):
            print(f"ERROR: {cfg['name']} failed to start (port {port} not available)")
            return None
        print(f"  {cfg['name']} is ready (port {port})")

    # Give the gossip protocol time to form the cluster
    print("  Waiting for cluster to form via gossip...")
    time.sleep(5)

    return "http://localhost:3001,http://localhost:3002,http://localhost:3003"


def main():
    parser = argparse.ArgumentParser(description="E2E tests for object storage")
    parser.add_argument(
        "--project-root",
        default=".",
        help="Path to project root directory",
    )
    parser.add_argument(
        "--timeout",
        type=int,
        default=60,
        help="Service startup timeout in seconds",
    )
    parser.add_argument(
        "--clean",
        action="store_true",
        help="Remove data files before running tests",
    )
    parser.add_argument(
        "--cluster",
        action="store_true",
        help="Run with 3 metadata instances in cluster mode",
    )
    parser.add_argument(
        "--use-bazel",
        action="store_true",
        help="Prefer Bazel over Cargo for running services (slower but hermetic)",
    )
    args = parser.parse_args()

    project_root = os.path.abspath(args.project_root)

    # Verify project root contains expected files
    if not os.path.exists(os.path.join(project_root, "Cargo.toml")):
        print(f"ERROR: {project_root} does not appear to be the project root")
        print("       (Cargo.toml not found)")
        return 1

    if args.clean:
        print("Cleaning data files...")
        clean_data_files(project_root)
        # Also clean cluster data dir
        cluster_data_dir = os.path.join(project_root, "test-cluster-data")
        if os.path.exists(cluster_data_dir):
            shutil.rmtree(cluster_data_dir)
            print(f"  Removed {cluster_data_dir}")

    mode = "cluster" if args.cluster else "single"
    with ServiceManager(project_root, prefer_bazel=args.use_bazel) as manager:
        print(f"\nStarting services in {mode} mode (using {'bazel' if manager.use_bazel else 'cargo'})...")

        # 1. Start metadata service(s)
        if args.cluster:
            metadata_urls = start_cluster_metadata(manager, project_root, args.timeout)
        else:
            metadata_urls = start_single_metadata(manager, project_root, args.timeout)

        if metadata_urls is None:
            return 1

        # 2. Start storage node
        print("  Starting storage-node...")
        manager.start_service(
            name="storage-node",
            bazel_target="//storage-node:storage_node",
            cargo_package="storage-node",
            env_vars={
                "CONFIG_PATH": os.path.join(project_root, "storage-node", "config.yaml")
            },
        )
        if not wait_for_port("localhost", 3000, timeout=args.timeout):
            print("ERROR: storage-node failed to start (port 3000 not available)")
            return 1
        print("  storage-node is ready (port 3000)")
        # Give storage node time to register with metadata service
        time.sleep(2)

        # 3. Start API gateway
        print("  Starting api-gateway...")
        gateway_env = {"METADATA_URLS": metadata_urls} if args.cluster else {"METADATA_URL": "http://localhost:3001"}
        manager.start_service(
            name="api-gateway",
            bazel_target="//api-gateway:api_gateway",
            cargo_package="api-gateway",
            env_vars=gateway_env,
        )
        if not wait_for_http_health("http://localhost:8080/health", timeout=args.timeout):
            print("ERROR: api-gateway failed to start (health check failed)")
            return 1
        print("  api-gateway is ready (port 8080)")

        print(f"\nAll services started successfully! (mode={mode})\n")

        # Run tests
        client = ObjectStoreClient("http://localhost:8080")

        tests = [
            ("Health Check", test_health_check),
            ("Metrics Endpoint", test_metrics_endpoint),
            ("Upload Object", test_upload_object),
            ("Download Object", test_download_object),
            ("Multi-Chunk Upload/Download", test_multi_chunk_upload_download),
            ("Empty Object", test_empty_object),
            ("Valid Special Object Names", test_valid_special_object_names),
            ("List Objects", test_list_objects),
            ("Delete Object", test_delete_object),
            ("Delete Non-existent (404)", test_delete_nonexistent_404),
            ("Duplicate Upload (409)", test_duplicate_upload_conflict),
            ("Download Non-existent (404)", test_download_nonexistent_404),
            ("Invalid Object Names", test_invalid_object_names),
            ("Invalid Base64", test_invalid_base64),
            ("Update Flow", test_update_flow_delete_reupload),
            ("Binary Data Roundtrip", test_binary_data_roundtrip),
            ("Multipart Upload", test_multipart_upload),
            ("Streaming Download", test_streaming_download),
            ("Multipart Upload + Streaming Download Roundtrip", test_multipart_upload_streaming_download_roundtrip),
        ]

        passed = 0
        failed = 0

        print("Running tests...")
        for name, test_func in tests:
            try:
                test_func(client)
                print(f"  [PASS] {name}")
                passed += 1
            except AssertionError as e:
                print(f"  [FAIL] {name}: {e}")
                failed += 1
            except Exception as e:
                print(f"  [ERROR] {name}: {type(e).__name__}: {e}")
                failed += 1

        print(f"\n{'='*50}")
        print(f"Results: {passed} passed, {failed} failed (mode={mode})")
        print("="*50)

        return 0 if failed == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
