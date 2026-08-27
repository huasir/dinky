#!/bin/sh

# 获取脚本所在目录的绝对路径（兼容 sh，不依赖 bash）
SCRIPT_DIR="$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)"
# 从 dinky-flink-1.19 或 dinky-flink-1.20 回到 dinky 项目根目录
PROJECT_ROOT="$(CDPATH= cd -- "$SCRIPT_DIR/../.." && pwd)"
LIB_DIR="$PROJECT_ROOT/lib"

echo "Installing vastbase jars to local Maven repository..."
echo "Project root: $PROJECT_ROOT"
echo "Lib directory: $LIB_DIR"

# 安装 flink-connector-vastbase-cdc
echo ""
echo "Installing flink-connector-vastbase-cdc..."
mvn install:install-file \
    -Dfile="$LIB_DIR/flink-connector-vastbase-cdc-3.0.0-SNAPSHOT_202411121803.jar" \
    -DgroupId=cn.com.exbase \
    -DartifactId=flink-connector-vastbase-cdc \
    -Dversion=3.0.0-SNAPSHOT \
    -Dpackaging=jar \
    -DgeneratePom=true

if [ $? -eq 0 ]; then
    echo "OK flink-connector-vastbase-cdc installed successfully"
else
    echo "FAIL Failed to install flink-connector-vastbase-cdc"
    exit 1
fi

# 安装 vastbase-connector-jdbc
echo ""
echo "Installing vastbase-connector-jdbc..."
mvn install:install-file \
    -Dfile="$LIB_DIR/JDBC-VB_V-2.10-2024090616.jar" \
    -DgroupId=cn.com.exbase \
    -DartifactId=vastbase-connector-jdbc \
    -Dversion=2.10 \
    -Dpackaging=jar \
    -DgeneratePom=true

if [ $? -eq 0 ]; then
    echo "OK vastbase-connector-jdbc installed successfully"
else
    echo "FAIL Failed to install vastbase-connector-jdbc"
    exit 1
fi

echo ""
echo "All jars installed successfully!"
