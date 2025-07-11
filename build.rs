fn main() -> Result<(), Box<dyn std::error::Error>> {
    prost_build::compile_protos(
        &[
            "proto/proto/rdf.proto",
            "proto/proto/patch.proto",
            "proto/proto/grpc.proto",
        ],
        &["proto/proto"],
    )?;
    Ok(())
}
