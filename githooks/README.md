# Git Hooks（可提交）

本目录随仓库提交。启用后，在 **Cursor / 终端 / 任意 IDE** 里执行 `git commit` 时都会自动跑 `spotless:apply`。

## 启用（每人一次）

在仓库根目录执行：

```bash
./script/setup-githooks.sh
```

等价于：

```bash
git config core.hooksPath githooks
chmod +x githooks/*
```

`core.hooksPath` 是**本地** Git 配置，不会进远程；同事 clone 后需自己跑一次 setup。

## 行为

| Hook         | 时机            | 行为 |
|--------------|-----------------|------|
| `pre-commit` | `git commit` 前 | 对暂存的 `.java` / `.xml` / `.ts` / `.tsx` / `.js` 跑 Spotless，并把改动重新 `git add` |

- 使用 `-Dspotless.ratchetFrom=HEAD`，只格式化相对上次提交有改动的文件（快，且不依赖 `origin/dev`）。
- merge / rebase 过程中默认跳过；若仍要执行：`SPOTLESS_DURING_REBASE=1 git commit ...`
- 临时跳过：`SKIP_SPOTLESS=1 git commit ...`

## 关闭

```bash
git config --unset core.hooksPath
```

## 要求

- 本机可运行 `./mvnw`（JDK 需满足 Spotless / Palantir 要求，一般 JDK 11+）
- 首次可能下载依赖，耗时会稍长
