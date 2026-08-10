//! 编 proto。**只在 grpc feature 打开时跑** —— 否则关掉 grpc 的构建
//! （比如 dtmrs-ffi）会平白要求机器上装 protoc。
//!
//! # 找不到 protoc 时自带一个
//!
//! 作为发布到 crates.io 的库，不能要求每个下游用户先手动装 protoc ——
//! 那是很实在的接入摩擦，docs.rs 的构建环境也不一定有。
//! 所以系统里没有就退回到 `protoc-bin-vendored` 自带的那份。
//!
//! 优先用系统的（版本通常更新、也尊重用户已有的环境），自带的只是兜底。

fn main() -> std::io::Result<()> {
    // Cargo 会为每个开启的 feature 设一个 CARGO_FEATURE_<大写> 环境变量
    if std::env::var_os("CARGO_FEATURE_GRPC").is_none() {
        return Ok(());
    }

    println!("cargo:rerun-if-changed=proto/dtmrs.proto");
    println!("cargo:rerun-if-changed=proto/busi.proto");
    println!("cargo:rerun-if-env-changed=PROTOC");

    // 用户显式指定了就听他的；否则系统里有就用系统的；都没有才用自带的
    if std::env::var_os("PROTOC").is_none() && which_protoc().is_none() {
        if let Ok(p) = protoc_bin_vendored::protoc_bin_path() {
            std::env::set_var("PROTOC", p);
        }
    }

    tonic_prost_build::configure()
        .build_server(true)
        .build_client(true)
        .compile_protos(&["proto/dtmrs.proto", "proto/busi.proto"], &["proto"])
}

/// 在 PATH 里找 protoc。不用额外依赖，自己扫一遍就够
fn which_protoc() -> Option<std::path::PathBuf> {
    let path = std::env::var_os("PATH")?;
    std::env::split_paths(&path)
        .map(|dir| dir.join("protoc"))
        .find(|p| p.is_file())
}
