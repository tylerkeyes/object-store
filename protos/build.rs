fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Tell Cargo that cfg(bazel) is expected (used for Bazel builds)
    println!("cargo::rustc-check-cfg=cfg(bazel)");

    // Compile proto files
    tonic_build::configure()
        .build_server(true)
        .compile_protos(
            &["storage_metadata.proto", "storage_node.proto"],
            &["."],
        )?;
    Ok(())
}
