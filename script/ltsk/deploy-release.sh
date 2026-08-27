#!/usr/bin/env bash
# 公司环境 Dinky 自动部署脚本（人工触发，不由 CI 自动调用）
#
# 典型用法：
#   1) 从 GitLab Pipeline 下载 build/*.tar.gz 制品，或本地 mvn package 得到包
#   2) cp script/ltsk/deploy.env.example /safe/path/deploy.env 并填写
#   3) ./script/ltsk/deploy-release.sh --env /safe/path/deploy.env --package ./build/dinky-release-1.20-xxx.tar.gz
#
# 行为概要：
#   - scp 包到远端 releases/<timestamp>-<tag>/
#   - 解压、切换 current 软链
#   - 调用 script/bin/auto.sh restart <FLINK_VERSION>
#   - 可选 HTTP 健康检查
#
set -euo pipefail

RED='\033[31m'; GREEN='\033[32m'; YELLOW='\033[33m'; RESET='\033[0m'

usage() {
  cat <<'EOF'
用法:
  ./script/ltsk/deploy-release.sh --env <deploy.env> --package <dinky-release-*.tar.gz> [选项]

选项:
  --env PATH          部署环境变量文件（必填）
  --package PATH      本地 release tar.gz（必填）
  --tag NAME          本次发布名（默认取文件名去后缀，或 CI_COMMIT_TAG）
  --dry-run           只打印将执行的命令，不真正部署
  --skip-restart      只更新文件与软链，不执行 auto.sh restart
  -h, --help          显示帮助
EOF
}

log()  { echo -e "${GREEN}[deploy]${RESET} $*"; }
warn() { echo -e "${YELLOW}[deploy]${RESET} $*"; }
err()  { echo -e "${RED}[deploy]${RESET} $*" >&2; }

ENV_FILE=""
PACKAGE=""
RELEASE_TAG=""
DRY_RUN=0
SKIP_RESTART=0

while [[ $# -gt 0 ]]; do
  case "$1" in
    --env) ENV_FILE="${2:-}"; shift 2 ;;
    --package) PACKAGE="${2:-}"; shift 2 ;;
    --tag) RELEASE_TAG="${2:-}"; shift 2 ;;
    --dry-run) DRY_RUN=1; shift ;;
    --skip-restart) SKIP_RESTART=1; shift ;;
    -h|--help) usage; exit 0 ;;
    *) err "未知参数: $1"; usage; exit 1 ;;
  esac
done

[[ -n "${ENV_FILE}" && -f "${ENV_FILE}" ]] || { err "请用 --env 指定存在的 env 文件"; exit 1; }
[[ -n "${PACKAGE}" && -f "${PACKAGE}" ]] || { err "请用 --package 指定存在的 tar.gz"; exit 1; }

# shellcheck disable=SC1090
source "${ENV_FILE}"

: "${DEPLOY_HOST:?DEPLOY_HOST 未设置}"
: "${DEPLOY_USER:?DEPLOY_USER 未设置}"
: "${DEPLOY_ROOT:?DEPLOY_ROOT 未设置}"
: "${FLINK_VERSION:?FLINK_VERSION 未设置}"

DEPLOY_PORT="${DEPLOY_PORT:-22}"
BACKUP_BEFORE_DEPLOY="${BACKUP_BEFORE_DEPLOY:-true}"
KEEP_RELEASES="${KEEP_RELEASES:-5}"
HEALTHCHECK_URL="${HEALTHCHECK_URL:-}"
HEALTHCHECK_TIMEOUT_SEC="${HEALTHCHECK_TIMEOUT_SEC:-120}"

PACKAGE_ABS="$(cd "$(dirname "${PACKAGE}")" && pwd)/$(basename "${PACKAGE}")"
PACKAGE_BASE="$(basename "${PACKAGE_ABS}")"
PACKAGE_STEM="${PACKAGE_BASE%.tar.gz}"

if [[ -z "${RELEASE_TAG}" ]]; then
  RELEASE_TAG="${CI_COMMIT_TAG:-${PACKAGE_STEM}}"
fi

TS="$(date +%Y%m%d%H%M%S)"
REMOTE_RELEASE_DIR="${DEPLOY_ROOT}/releases/${TS}-${RELEASE_TAG}"
REMOTE_CURRENT="${DEPLOY_ROOT}/current"
REMOTE_PACKAGE="${REMOTE_RELEASE_DIR}/${PACKAGE_BASE}"

SSH_OPTS=(-p "${DEPLOY_PORT}" -o StrictHostKeyChecking=accept-new)
SCP_OPTS=(-P "${DEPLOY_PORT}" -o StrictHostKeyChecking=accept-new)
if [[ -n "${DEPLOY_SSH_KEY:-}" ]]; then
  SSH_OPTS+=(-i "${DEPLOY_SSH_KEY}")
  SCP_OPTS+=(-i "${DEPLOY_SSH_KEY}")
fi

ssh_run() {
  if [[ "${DRY_RUN}" -eq 1 ]]; then
    echo "DRY-RUN ssh ${DEPLOY_USER}@${DEPLOY_HOST} $*"
    return 0
  fi
  ssh "${SSH_OPTS[@]}" "${DEPLOY_USER}@${DEPLOY_HOST}" "$@"
}

run() {
  if [[ "${DRY_RUN}" -eq 1 ]]; then
    echo "DRY-RUN $*"
    return 0
  fi
  "$@"
}

log "目标 ${DEPLOY_USER}@${DEPLOY_HOST}:${DEPLOY_ROOT}"
log "制品 ${PACKAGE_ABS}"
log "发布目录 ${REMOTE_RELEASE_DIR}"

# 1) 远端准备目录
ssh_run "mkdir -p '${DEPLOY_ROOT}/releases' '${REMOTE_RELEASE_DIR}'"

# 2) 可选备份当前 current 指向
if [[ "${BACKUP_BEFORE_DEPLOY}" == "true" ]]; then
  ssh_run "if [ -L '${REMOTE_CURRENT}' ] || [ -d '${REMOTE_CURRENT}' ]; then
    cp -a '${REMOTE_CURRENT}' '${DEPLOY_ROOT}/releases/backup-${TS}' 2>/dev/null || true
  fi"
fi

# 3) 上传并解压
run scp "${SCP_OPTS[@]}" "${PACKAGE_ABS}" "${DEPLOY_USER}@${DEPLOY_HOST}:${REMOTE_PACKAGE}"
ssh_run "tar -xzf '${REMOTE_PACKAGE}' -C '${REMOTE_RELEASE_DIR}' && rm -f '${REMOTE_PACKAGE}'"

# 解压后通常得到 dinky-release-xxx/；以 script/bin/auto.sh 定位应用根目录
ssh_run "AUTO=\$(find '${REMOTE_RELEASE_DIR}' -maxdepth 4 -type f -path '*/script/bin/auto.sh' | head -n 1);
  if [ -z \"\${AUTO}\" ]; then echo '未找到 script/bin/auto.sh' >&2; ls -laR '${REMOTE_RELEASE_DIR}' >&2; exit 1; fi;
  APP_DIR=\$(cd \"\$(dirname \"\${AUTO}\")/../..\" && pwd);
  echo \"\${APP_DIR}\" > '${REMOTE_RELEASE_DIR}/.app_dir';
  ln -sfn \"\${APP_DIR}\" '${REMOTE_CURRENT}';
  echo \"APP_DIR=\${APP_DIR}\""

# 4) 清理旧 release
ssh_run "cd '${DEPLOY_ROOT}/releases' && ls -1dt */ 2>/dev/null | tail -n +$((KEEP_RELEASES + 1)) | xargs -r rm -rf"

# 5) 重启
if [[ "${SKIP_RESTART}" -eq 1 ]]; then
  warn "已跳过 restart（--skip-restart）"
else
  log "执行 auto.sh restart ${FLINK_VERSION}"
  ssh_run "cd '${REMOTE_CURRENT}' && export DINKY_HOME='${REMOTE_CURRENT}' && bash ./script/bin/auto.sh restart '${FLINK_VERSION}'"
fi

# 6) 健康检查
if [[ -n "${HEALTHCHECK_URL}" && "${DRY_RUN}" -eq 0 ]]; then
  log "健康检查 ${HEALTHCHECK_URL}（超时 ${HEALTHCHECK_TIMEOUT_SEC}s）"
  deadline=$((SECONDS + HEALTHCHECK_TIMEOUT_SEC))
  ok=0
  while (( SECONDS < deadline )); do
    if curl -fsS -o /dev/null "${HEALTHCHECK_URL}"; then
      ok=1
      break
    fi
    sleep 5
  done
  if [[ "${ok}" -ne 1 ]]; then
    err "健康检查失败"
    exit 1
  fi
  log "健康检查通过"
elif [[ -n "${HEALTHCHECK_URL}" && "${DRY_RUN}" -eq 1 ]]; then
  echo "DRY-RUN curl -fsS ${HEALTHCHECK_URL}"
fi

log "部署完成：${REMOTE_CURRENT} -> ${REMOTE_RELEASE_DIR}"
