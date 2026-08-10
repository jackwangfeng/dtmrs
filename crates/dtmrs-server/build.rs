//! 编 proto。**只在 grpc feature 打开时跑** —— 否则关掉 grpc 的构建
//! （比如 dtmrs-ffi）会平白要求机器上装 protoc。

fn main() -> std::io::Result<()> {
    // Cargo 会为每个开启的 feature 设一个 CARGO_FEATURE_<大写> 环境变量
    if std::env::var_os("CARGO_FEATURE_GRPC").is_none() {
        return Ok(());
    }

    println!("cargo:rerun-if-changed=proto/dtmrs.proto");
    println!("cargo:rerun-if-changed=proto/busi.proto");

    tonic_prost_build::configure()
        .build_server(true)
        .build_client(true)
        .compile_protos(&["proto/dtmrs.proto", "proto/busi.proto"], &["proto"])
}
