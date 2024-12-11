fn main() -> Result<(), Box<dyn std::error::Error>> {
    #[cfg(feature = "net")]
    {
        tonic_build::configure()
            .protoc_arg("--experimental_allow_proto3_optional")
            .compile_protos(&["src/net/proto/tonbo.proto"], &["src/net/proto"])?;
    }
    Ok(())
}