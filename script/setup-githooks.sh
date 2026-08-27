#!/usr/bin/env bash
# 将本仓库的 githooks/ 注册为 Git hooks 目录（每人 clone 后执行一次即可）
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "${REPO_ROOT}"

HOOKS_PATH="githooks"
if [[ ! -d "${HOOKS_PATH}" ]]; then
  echo "未找到 ${HOOKS_PATH}/ 目录" >&2
  exit 1
fi

chmod +x "${HOOKS_PATH}"/* 2>/dev/null || true
git config core.hooksPath "${HOOKS_PATH}"

echo "已设置 core.hooksPath=${HOOKS_PATH}"
echo "当前值: $(git config --get core.hooksPath)"
echo
echo "说明:"
echo "  - 之后 git commit 会自动运行 spotless:apply"
echo "  - 临时跳过: SKIP_SPOTLESS=1 git commit ..."
echo "  - 恢复默认 hooks: git config --unset core.hooksPath"
