# Vastbase Connector JDK 11 配置指南

## 问题说明

使用 JDK 11 时，需要在 Flink 配置中添加 JVM 参数 `--add-opens` 来解决 Vastbase Connector 的访问权限问题。

## 配置方法

### 方法 1：在 Dinky Web UI 中配置（推荐）

1. **登录 Dinky Web UI**

2. **进入集群配置**
   - 点击左侧菜单 **注册中心** -> **集群配置**
   - 找到您使用的 Flink 集群配置，点击 **编辑** 按钮

3. **添加 Flink 自定义配置**
   - 在配置页面中找到 **Flink 自定义配置（高优先级）** 部分
   - 点击 **添加** 按钮，添加以下配置项：

   | 配置项名称 | 配置项值 |
   |---------|--------|
   | `env.java.opts` | `--add-opens java.base/java.lang=ALL-UNNAMED --add-opens java.base/java.util=ALL-UNNAMED` |

   **注意**：配置项值中不要包含引号，直接填写参数即可。

4. **保存配置**
   - 点击 **保存** 按钮

5. **重启 Flink 集群**
   - 如果集群正在运行，需要重启才能生效
   - 对于 Yarn Application 模式，下次提交任务时会自动使用新配置
   - 对于 Yarn Session 模式，需要重启 Session 集群

### 方法 2：在 flink-conf.yaml 中配置

如果您可以直接访问 Flink 集群的配置文件：

1. **编辑 flink-conf.yaml**
   - 找到 Flink 集群的配置文件 `flink-conf.yaml`
   - 通常位于 `$FLINK_HOME/conf/flink-conf.yaml`

2. **添加配置**
   在文件末尾添加：

   ```yaml
   env.java.opts: "--add-opens java.base/java.lang=ALL-UNNAMED --add-opens java.base/java.util=ALL-UNNAMED"
   ```

   **注意**：如果文件中已有 `env.java.opts` 配置，需要将参数追加到现有值后面，用空格分隔。

   例如，如果已有：
   ```yaml
   env.java.opts: "-Dfile.encoding=UTF-8"
   ```
   
   修改为：
   ```yaml
   env.java.opts: "-Dfile.encoding=UTF-8 --add-opens java.base/java.lang=ALL-UNNAMED --add-opens java.base/java.util=ALL-UNNAMED"
   ```

3. **重启 Flink 集群**
   - 修改配置后必须重启 Flink 集群才能生效

### 方法 3：分别配置 JobManager 和 TaskManager（可选）

如果需要分别配置 JobManager 和 TaskManager 的 JVM 参数：

在 **Flink 自定义配置** 中添加：

| 配置项名称 | 配置项值 |
|---------|--------|
| `env.java.opts.jobmanager` | `--add-opens java.base/java.lang=ALL-UNNAMED --add-opens java.base/java.util=ALL-UNNAMED` |
| `env.java.opts.taskmanager` | `--add-opens java.base/java.lang=ALL-UNNAMED --add-opens java.base/java.util=ALL-UNNAMED` |

## 验证配置

配置完成后，可以通过以下方式验证：

1. **查看 Flink Web UI**
   - 访问 Flink Web UI（通常是 `http://jobmanager:8081`）
   - 查看 TaskManager 日志，应该能看到 JVM 参数已加载

2. **查看启动日志**
   - 在 Flink TaskManager 的启动日志中，应该能看到类似以下内容：
   ```
   --add-opens java.base/java.lang=ALL-UNNAMED --add-opens java.base/java.util=ALL-UNNAMED
   ```

3. **重新运行任务**
   - 重新运行 Vastbase 到 MySQL 的整库同步任务
   - 应该不再出现 `IllegalAccessError` 错误

## 常见问题

### Q: 配置后还是报错怎么办？

A: 请检查以下几点：
1. ✅ 确认配置项名称是 `env.java.opts`（注意是下划线，不是点）
2. ✅ 确认配置项值中没有多余的引号
3. ✅ **确认已重启 Flink 集群**（重要！）
4. ✅ 查看 Flink TaskManager 日志，确认参数已加载

### Q: 如何查看当前生效的 JVM 参数？

A: 可以通过以下方式查看：
1. 查看 Flink TaskManager 进程的启动参数：
   ```bash
   ps aux | grep taskmanager
   ```
2. 查看 Flink Web UI 的 TaskManager 日志

### Q: 配置会影响其他任务吗？

A: 不会。`--add-opens` 参数只是开放了模块访问权限，不会影响其他任务的运行。

## 配置示例截图

### Dinky Web UI 配置示例

在 **Flink 自定义配置（高优先级）** 中添加：

```
配置项名称: env.java.opts
配置项值: --add-opens java.base/java.lang=ALL-UNNAMED --add-opens java.base/java.util=ALL-UNNAMED
```

### flink-conf.yaml 配置示例

```yaml
# ... 其他配置 ...

# Vastbase Connector 访问权限修复
env.java.opts: "--add-opens java.base/java.lang=ALL-UNNAMED --add-opens java.base/java.util=ALL-UNNAMED"
```

## 相关文档

- [Vastbase Connector 访问权限修复方案](./vastbase_access_fix.md)
- [Dinky 集群管理文档](../../user_guide/register_center/cluster_manage.md)

