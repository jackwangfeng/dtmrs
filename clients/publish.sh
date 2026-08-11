#!/usr/bin/env bash
# 把三个业务侧客户端发布到 PyPI / npm / Maven Central。
#
# ⚠ 发布不可逆：npm 和 PyPI 只能 yank/deprecate，Maven Central 完全不能删。
#    每一步都会先让你确认。
#
# 凭据从各工具自己的位置读，本脚本不碰、不打印：
#   PyPI   ~/.pypirc 或 TWINE_USERNAME=__token__ TWINE_PASSWORD=pypi-xxx
#   npm    ~/.npmrc 里的 //registry.npmjs.org/:_authToken=
#          开了 2FA 的话还要动态码：NPM_OTP=123456 ./publish.sh node
#          （或者生成 token 时勾上 "Bypass two-factor authentication"）
#          ⚠ SSH 环境下别用 `npm login`（默认要弹浏览器），见 README 的说明。
#          ⚠ 本机 registry 若指向淘宝等镜像不影响发布 —— package.json 里
#             publishConfig 已经把目标钉死在官方源，镜像是只读的发不上去
#   Maven  ~/.m2/settings.xml 里 id=central 的 server + GPG 密钥
set -euo pipefail
cd "$(dirname "$0")"
VERSION=0.2.0
ONLY="${1:-all}"

confirm() { echo; read -rp "→ $1 [y/N] " a; [ "$a" = y ] || { echo "  跳过"; return 1; }; }

if [ "$ONLY" = all ] || [ "$ONLY" = python ]; then
  echo "=== Python (PyPI) ==="
  ( cd python && rm -rf dist build *.egg-info
    python3 -m build --outdir dist && python3 -m twine check dist/*
    ls -la dist/
    confirm "上传 dtmrs-barrier $VERSION 到 PyPI？" && python3 -m twine upload dist/* )
fi

if [ "$ONLY" = all ] || [ "$ONLY" = node ]; then
  echo "=== Node (npm) ==="
  ( cd node
    # 发布前先确认认证是对着**官方源**的，而不是镜像
    who=$(npm whoami --registry https://registry.npmjs.org/ 2>/dev/null || true)
    if [ -z "$who" ]; then
      echo "  ✗ 未登录官方 npm 源。见 clients/README.md 的「SSH 环境怎么认证」"
      exit 1
    fi
    echo "  当前 npm 账号: $who"
    npm pack --dry-run
    # 账号开了 2FA 且 token 没勾 bypass 的话，必须带动态码：
    #   NPM_OTP=123456 ./publish.sh node
    OTP_ARG=""
    [ -n "${NPM_OTP:-}" ] && OTP_ARG="--otp=$NPM_OTP"
    confirm "发布 dtmrs-barrier@$VERSION 到 npm？" && npm publish $OTP_ARG )
fi

if [ "$ONLY" = all ] || [ "$ONLY" = java ]; then
  echo "=== Java (Maven Central) ==="
  echo "前置条件（缺一不可，都要你自己先办）："
  echo "  1. Sonatype Central 账号，且 io.github.jackwangfeng 这个 namespace 已通过 GitHub 验证"
  echo "  2. GPG 密钥已生成并推到公钥服务器（Central 会去验签名）"
  echo "  3. ~/.m2/settings.xml 里配好 <server><id>central</id> 的 token"
  ( cd java
    : "${JAVA_HOME:?请先 export JAVA_HOME，javadoc 插件要用}"
    confirm "构建并上传到 Maven Central（autoPublish=false，上传后停在门户等你点发布）？" \
      && mvn -B -Prelease clean deploy )
fi

echo; echo "完成。"
