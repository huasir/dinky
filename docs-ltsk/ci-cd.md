# CI/CD 说明（公司仓 ltsk）

## 原则

| 能力 | 是否自动 | 说明 |
|------|----------|------|
| Spotless 检查 | 是 | `.gitlab-ci.yml` → `spotless` |
| 打包 tar.gz | 是 | `vendor/*` 推送 / `ltsk-*` tag |
| 部署到生产 | **否** | 使用 `script/ltsk/deploy-release.sh` 人工执行 |

本地 commit 前格式化仍靠 `githooks/`（见 [git-hooks.md](./git-hooks.md)）。

## GitLab CI

文件：[`.gitlab-ci.yml`](../.gitlab-ci.yml)

对齐公司其它项目的关键配置：

| 项 | 值 | 说明 |
|----|-----|------|
| Runner `tags` | `global-ltd` | **必填**，否则 Job 一直 pending |
| Maven 镜像 | `10.0.0.17:3060/cicd/maven-microsoft-openjdk:3.9.6-17` | JDK17；Harbor 无现成 JDK11+Maven 组合 |
| 旧镜像（勿用） | `cicd/maven:ltd-cd-nexus` / `liudonglin/maven:ltd-cd-nexus` | JDK8，不满足 Spotless/Dinky 1.x |
| `MAVEN_CONFIG` | 必须清空 | 部分镜像设为 `/root/.m2`，会污染 Maven 参数 |
| Harbor | `http://10.0.0.17:3060/harbor`（`cicd` 项目） | 搜 maven / openjdk 可看其它 tag |

1. 如 Flink 版本不是 1.20，在 CI/CD Variables 设置：
   - `DINKY_MVN_PROFILES=prod,jdk11,flink-single-version,flink-1.xx,web`
2. Vastbase 私有 jar 在仓库 `lib/`，CI 打包前会跑 `dinky-flink/dinky-flink-1.20/install_jar.sh` 装进本地 `.m2`（公司 Nexus 没有这些包）。
3. 推送 `vendor/*` 或打 `ltsk-*` tag 后，到 Pipeline 下载 `build/*.tar.gz` 制品。
4. 若仍 pending：核对 Job 是否带有 `global-ltd`，以及该 Runner 是否 online。

## 自动部署脚本（手动触发）

脚本：[script/ltsk/deploy-release.sh](../script/ltsk/deploy-release.sh)  
配置样例：[script/ltsk/deploy.env.example](../script/ltsk/deploy.env.example)

```bash
# 1. 配置（勿提交含密钥的 deploy.env）
cp script/ltsk/deploy.env.example /safe/path/deploy.env
# 编辑 DEPLOY_HOST / DEPLOY_USER / DEPLOY_ROOT / FLINK_VERSION 等

# 2. 准备制品（CI 下载或本地打包）
#    build/dinky-release-*.tar.gz

# 3. 先演练
./script/ltsk/deploy-release.sh \
  --env /safe/path/deploy.env \
  --package ./build/dinky-release-1.20-1.2.4.tar.gz \
  --dry-run

# 4. 真正部署
./script/ltsk/deploy-release.sh \
  --env /safe/path/deploy.env \
  --package ./build/dinky-release-1.20-1.2.4.tar.gz \
  --tag ltsk-1.2.4.1
```

脚本会：上传包 → 解压到 `DEPLOY_ROOT/releases/<时间>-<tag>/` → 切换 `current` 软链 → `auto.sh restart` → 可选健康检查。

## 以后若要接到 CI「手动按钮」

在 `.gitlab-ci.yml` 增加 `when: manual` 的 job，调用同一脚本即可，例如仅在 `ltsk-*` tag 流水线出现「Deploy」按钮，**默认不会跑**。需要时再改，当前刻意不接，避免误部署。

## 与版本方案的关系

发版流程建议：

1. `vendor/x.y.z` 开发，CI 绿  
2. 打 `ltsk-x.y.z.N` tag → CI 出正式包  
3. 运维/负责人本机执行 `deploy-release.sh`  
4. 回滚：把 `current` 指回上一 `releases/` 目录后 `auto.sh restart`（或保留 backup 目录）
