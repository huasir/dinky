# Vastbase Connector 访问权限修复

## 问题

在使用 Vastbase Connector 3.0.0-SNAPSHOT 与 Flink CDC 3.2.0 时，可能会遇到 `IllegalAccessError` 错误，因为 `VastbaseConnectorTask` 试图访问 `PostgresConnectorConfig` 的 protected 方法 `getSnapshotter()`。

## 解决方案

### 1. 代码层面的修复（已实现）

- `VastbaseAccessHelper.java`: 提供反射工具类，尝试在运行时设置访问权限
- `VastbaseCDCBuilder.java`: 在静态初始化块中调用 `VastbaseAccessHelper.initializeAccessPermissions()`

### 2. JVM 参数修复（推荐）

由于错误发生在第三方 jar 包内部，代码层面的修复可能无法完全解决问题。建议在 Flink 配置中添加以下 JVM 参数：

```yaml
env.java.opts: "--add-opens java.base/java.lang=ALL-UNNAMED --add-opens java.base/java.util=ALL-UNNAMED"
```

详细说明请参考：`docs/docs/practical_guide/cdcsource_practice/vastbase_access_fix.md`

## 注意事项

1. 这是临时解决方案，建议尽快升级到兼容版本的 Vastbase Connector
2. JVM 参数方案需要 Java 9 或更高版本
3. 如果问题仍然存在，请联系 Vastbase 团队获取兼容 Flink CDC 3.2.0 的版本

