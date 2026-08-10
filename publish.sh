#!/usr/bin/env bash
# 发布到 crates.io。先 cargo login（token 在 https://crates.io/settings/tokens 生成）。
#
# 用 --workspace：cargo 1.90+ 会自己算出依赖顺序并等索引同步，
# 不用手动一个个发、也不用自己 sleep。
set -euo pipefail
cd "$(dirname "$0")"

echo "== 发布前自查 =="
cargo fmt --all -- --check
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace
cargo publish --workspace --dry-run

echo
echo "自查通过。按回车真正发布（不可逆：crates.io 只能 yank，不能删）"
read -r
cargo publish --workspace
