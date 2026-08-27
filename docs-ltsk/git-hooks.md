# Git Hooks 使用说明

公司二次开发推荐启用仓库内 `githooks/`，在 commit 前自动 `spotless:apply`，与是否使用 IDEA / Cursor 无关。

## 启用

```bash
./script/setup-githooks.sh
```

详情见 [githooks/README.md](../githooks/README.md)。

## 临时跳过

```bash
SKIP_SPOTLESS=1 git commit -m "..."
```
