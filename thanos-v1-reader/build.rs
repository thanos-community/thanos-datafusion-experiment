fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("cargo:rerun-if-changed=proto");
    tonic_prost_build::configure().compile_protos(
        &[
            "proto/store/storepb/rpc.proto",
            "proto/store/hintspb/hints.proto",
            "proto/info/infopb/rpc.proto",
        ],
        &["proto"],
    )?;
    Ok(())
}
