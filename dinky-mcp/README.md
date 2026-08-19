# dinky-mcp

Dinky Flink SQL 平台的 MCP Server，将 Dinky OpenAPI 封装为 Cursor / Claude 等 AI 客户端可调用的工具。

## 环境要求

- Python >= 3.11
- 运行中的 Dinky 实例（默认 `http://127.0.0.1:8888`）
- Dinky 令牌（**认证中心 → 令牌** 中创建）

## 安装

```bash
cd dinky-mcp
uv sync
```

## 环境变量

| 变量 | 必填 | 默认值 | 说明 |
|------|------|--------|------|
| `DINKY_BASE_URL` | 否 | `http://127.0.0.1:8888` | Dinky 服务地址 |
| `DINKY_TOKEN` | **是** | — | OpenAPI 访问令牌 |

## Cursor MCP 配置

在 `~/.cursor/mcp.json` 中添加：

```json
{
  "mcpServers": {
    "dinky": {
      "command": "/Users/neo/workspace/dinky/dinky-mcp/.venv/bin/dinky-mcp",
      "env": {
        "DINKY_BASE_URL": "http://127.0.0.1:8888",
        "DINKY_TOKEN": "your-token-here"
      }
    }
  }
}
```

或使用 `uv run`：

```json
{
  "mcpServers": {
    "dinky": {
      "command": "uv",
      "args": ["--directory", "/Users/neo/workspace/dinky/dinky-mcp", "run", "dinky-mcp"],
      "env": {
        "DINKY_BASE_URL": "http://127.0.0.1:8888",
        "DINKY_TOKEN": "your-token-here"
      }
    }
  }
}
```

## 可用工具（Phase 1）

| 工具 | 说明 |
|------|------|
| `dinky_explain_sql` | 解释/校验 FlinkSQL |
| `dinky_submit_task` | 按任务 ID 提交作业 |
| `dinky_cancel_task` | 取消运行中的作业 |
| `dinky_get_job_status` | 查询任务最新作业实例 |
| `dinky_get_stream_graph` | 获取 Flink DAG |
| `dinky_get_lineage` | 获取任务血缘 |
| `dinky_trigger_savepoint` | 触发 Savepoint |
| `dinky_wait_for_job` | 轮询等待作业到达终态 |

## 本地调试

```bash
export DINKY_TOKEN=your-token
uv run dinky-mcp
```

## 开发

```bash
uv sync
uv run python -m dinky_mcp.server
```
