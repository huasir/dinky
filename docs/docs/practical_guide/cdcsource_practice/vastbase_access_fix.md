# Vastbase Connector 访问权限修复方案

## 问题描述

在使用 Vastbase 向 MySQL 进行整库同步时，可能会遇到以下错误：

```
java.lang.IllegalAccessError: class io.debezium.connector.vastbase.VastbaseConnectorTask 
tried to access protected method 'io.debezium.connector.postgresql.spi.Snapshotter 
io.debezium.connector.postgresql.PostgresConnectorConfig.getSnapshotter()'
```

这是由于 Vastbase Connector 3.0.0-SNAPSHOT 版本与 Flink CDC 3.2.0 之间的版本兼容性问题导致的。

## 解决方案

### 方案 1：添加 JVM 参数（推荐，适用于 Java 9+）

如果您的 Java 版本是 9 或更高，可以在启动 Flink 任务时添加以下 JVM 参数：

```bash
--add-opens java.base/java.lang=ALL-UNNAMED
--add-opens java.base/java.util=ALL-UNNAMED
```

#### 在 Flink 配置文件中添加

编辑 `flink-conf.yaml` 文件，添加以下配置：

```yaml
env.java.opts: "--add-opens java.base/java.lang=ALL-UNNAMED --add-opens java.base/java.util=ALL-UNNAMED"
```

或者分别设置 JobManager 和 TaskManager 的 JVM 参数：

```yaml
env.java.opts.jobmanager: "--add-opens java.base/java.lang=ALL-UNNAMED --add-opens java.base/java.util=ALL-UNNAMED"
env.java.opts.taskmanager: "--add-opens java.base/java.lang=ALL-UNNAMED --add-opens java.base/java.util=ALL-UNNAMED"
```

#### 在 Dinky 中配置

1. 进入 **注册中心** -> **集群管理**
2. 编辑您的 Flink 集群配置
3. 在 **Flink 配置** 中添加：

```yaml
env.java.opts: "--add-opens java.base/java.lang=ALL-UNNAMED --add-opens java.base/java.util=ALL-UNNAMED"
```

### 方案 2：使用代码中的反射工具类（已实现）

Dinky 已经在 `VastbaseCDCBuilder` 中添加了自动初始化访问权限的代码，这会在类加载时尝试设置访问权限。但这种方法可能无法完全解决问题，因为错误发生在第三方 jar 包内部。

### 方案 3：升级 Vastbase Connector（最佳方案）

联系 Vastbase 团队获取与 Flink CDC 3.2.0 兼容的 connector 版本，这是最根本的解决方案。

## 验证修复

修复后，重新运行 Vastbase 到 MySQL 的整库同步任务，应该不再出现 `IllegalAccessError` 错误。

## 注意事项

1. **Java 版本要求**：方案 1 需要 Java 9 或更高版本
2. **临时方案**：方案 1 和方案 2 都是临时解决方案，建议尽快升级到兼容版本的 Vastbase Connector
3. **性能影响**：添加 JVM 参数不会对性能产生明显影响

## 相关文件

- `dinky-cdc/dinky-cdc-core/src/main/java/org/dinky/cdc/vastbase/VastbaseAccessHelper.java` - 反射工具类
- `dinky-cdc/dinky-cdc-core/src/main/java/org/dinky/cdc/vastbase/VastbaseCDCBuilder.java` - Vastbase CDC 构建器

