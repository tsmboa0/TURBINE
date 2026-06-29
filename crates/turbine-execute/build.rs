//! Generate the Jito searcher gRPC client from the vendored protos.
//!
//! We generate from Jito's published `.proto` files (rather than depending on the
//! `jito-searcher-client` crate) so the generated types use *our* prost/tonic and
//! never drag in a conflicting `solana-sdk`/`solana-pubkey` pin.

fn main() -> Result<(), Box<dyn std::error::Error>> {
    tonic_prost_build::configure()
        .build_server(false)
        .include_file("jito.rs")
        .compile_protos(&["proto/searcher.proto"], &["proto"])?;
    println!("cargo:rerun-if-changed=proto");
    Ok(())
}
