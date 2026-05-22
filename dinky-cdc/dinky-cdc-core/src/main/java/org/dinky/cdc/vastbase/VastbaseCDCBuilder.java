/*
 *
 *  Licensed to the Apache Software Foundation (ASF) under one or more
 *  contributor license agreements.  See the NOTICE file distributed with
 *  this work for additional information regarding copyright ownership.
 *  The ASF licenses this file to You under the Apache License, Version 2.0
 *  (the "License"); you may not use this file except in compliance with
 *  the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 */

package org.dinky.cdc.vastbase;

import org.dinky.assertion.Asserts;
import org.dinky.cdc.AbstractCDCBuilder;
import org.dinky.cdc.CDCBuilder;
import org.dinky.constant.FlinkParamConstant;
import org.dinky.data.model.FlinkCDCConfig;

import org.apache.flink.cdc.connectors.shaded.org.apache.kafka.connect.json.DecimalFormat;
import org.apache.flink.cdc.connectors.shaded.org.apache.kafka.connect.json.JsonConverterConfig;
import org.apache.flink.cdc.connectors.vastbase.VastbaseSource;
import org.apache.flink.cdc.debezium.JsonDebeziumDeserializationSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class VastbaseCDCBuilder extends AbstractCDCBuilder implements CDCBuilder {

    private static final Logger logger = LoggerFactory.getLogger(VastbaseCDCBuilder.class);
    public static final String KEY_WORD = "vastbase-cdc";
    private static final String METADATA_TYPE = "Vastbase";

    static {
        // Initialize access permissions for protected methods
        // This is a workaround for IllegalAccessError with Vastbase connector
        VastbaseAccessHelper.initializeAccessPermissions();
    }

    public VastbaseCDCBuilder() {}

    public VastbaseCDCBuilder(FlinkCDCConfig config) {
        super(config);
    }

    @Override
    public String getSchema() {
        return config.getSchema();
    }

    @Override
    protected String getMetadataType() {
        return METADATA_TYPE;
    }

    @Override
    protected String generateUrl(String schema) {
        String baseUrl =
                String.format("jdbc:vastbase://%s:%s/%s", config.getHostname(), config.getPort(), config.getDatabase());
        Map<String, String> source = config.getSource();
        String serverTimeZone = source != null ? source.get("server-time-zone") : null;

        StringBuilder urlBuilder = new StringBuilder(baseUrl);
        boolean hasParam = false;

        // Vastbase 基于 PostgreSQL，JDBC 通常不识别 serverTimezone，使用 options 设置会话时区
        if (Asserts.isNotNullString(serverTimeZone)) {
            try {
                String optionsValue = "-c timezone=" + serverTimeZone;
                urlBuilder.append("?options=").append(URLEncoder.encode(optionsValue, StandardCharsets.UTF_8));
                hasParam = true;
            } catch (Exception e) {
                urlBuilder.append("?serverTimezone=").append(serverTimeZone);
                hasParam = true;
            }
        }

        if (config.getJdbc() != null && !config.getJdbc().isEmpty()) {
            for (Map.Entry<String, String> entry : config.getJdbc().entrySet()) {
                if (Asserts.isNotNullString(entry.getKey()) && Asserts.isNotNullString(entry.getValue())) {
                    urlBuilder.append(hasParam ? "&" : "?");
                    urlBuilder.append(entry.getKey()).append("=").append(entry.getValue());
                    hasParam = true;
                }
            }
        }

        logger.info("VastbaseCDCBuilder: generated JDBC URL = [{}]", urlBuilder.toString());
        return urlBuilder.toString();
    }

    @Override
    public String getHandle() {
        return KEY_WORD;
    }

    @Override
    public CDCBuilder create(FlinkCDCConfig config) {
        return new VastbaseCDCBuilder(config);
    }

    @Override
    public DataStreamSource<String> build(StreamExecutionEnvironment env) {
        Map<String, String> source = config.getSource();
        String decodingPluginName = source != null ? source.get("decoding.plugin.name") : null;
        String slotName = source != null ? source.get("slot.name") : null;
        String serverTimeZone = source != null ? source.get("server-time-zone") : null;

        // 调试日志：记录配置读取情况
        logger.info("VastbaseCDCBuilder.build: Reading config from FlinkCDCConfig");
        logger.info("VastbaseCDCBuilder.build: config.getSource() = {}", source);
        logger.info("VastbaseCDCBuilder.build: source.server-time-zone = [{}]", serverTimeZone);
        logger.info("VastbaseCDCBuilder.build: source.decoding.plugin.name = [{}]", decodingPluginName);
        logger.info("VastbaseCDCBuilder.build: source.slot.name = [{}]", slotName);

        Properties debeziumProperties = new Properties();

        // 关键修复：注册 VastbaseDebeziumConverter
        // Debezium 使用 converters 属性来注册自定义 converter
        // 格式：converters=<converter_name>,<converter_name>.type=<converter_class>,<converter_name>.<property>=<value>
        String converterName = "vastbase";
        String converterClass = "org.dinky.cdc.debezium.converter.VastbaseDebeziumConverter";
        debeziumProperties.setProperty("converters", converterName);
        debeziumProperties.setProperty(converterName + ".type", converterClass);
        debeziumProperties.setProperty(converterName + ".database.type", "vastbase");
        logger.info(
                "VastbaseCDCBuilder.build: ✅ Registered VastbaseDebeziumConverter: converters=[{}], {}.type=[{}]",
                converterName,
                converterName,
                converterClass);

        // 将源库时区传给 converter（不同 connector 可能只转发部分 key，多写几个便于验证）
        if (Asserts.isNotNullString(serverTimeZone)) {
            debeziumProperties.setProperty("datetime.format.timestamp.zone", serverTimeZone);
            debeziumProperties.setProperty("database.timezone", serverTimeZone);
            debeziumProperties.setProperty("dinky.vastbase.server.timezone", serverTimeZone);
            // 将时区配置传递给 converter
            debeziumProperties.setProperty(converterName + ".dinky.vastbase.server.timezone", serverTimeZone);
            debeziumProperties.setProperty(converterName + ".datetime.format.timestamp.zone", serverTimeZone);
            debeziumProperties.setProperty(converterName + ".database.timezone", serverTimeZone);
            logger.info(
                    "VastbaseCDCBuilder.build: ✅ Set server timezone in debeziumProperties: datetime.format.timestamp.zone=[{}], database.timezone=[{}], dinky.vastbase.server.timezone=[{}]",
                    serverTimeZone,
                    serverTimeZone,
                    serverTimeZone);
        } else {
            logger.warn(
                    "VastbaseCDCBuilder.build: ⚠️ source.server-time-zone not set. Add 'source.server-time-zone'='Asia/Shanghai' in CDCSOURCE WITH to fix timestamp +8h.");
        }
        if (config.getDebezium() != null) {
            for (Map.Entry<String, String> entry : config.getDebezium().entrySet()) {
                if (Asserts.isNotNullString(entry.getKey()) && Asserts.isNotNullString(entry.getValue())) {
                    debeziumProperties.setProperty(entry.getKey(), entry.getValue());
                }
            }
        }
        VastbaseSource.Builder<String> sourceBuilder = VastbaseSource.<String>builder()
                .hostname(config.getHostname())
                .port(config.getPort())
                .database(config.getDatabase())
                .username(config.getUsername())
                .password(config.getPassword());

        if (Asserts.isNotNullString(config.getSchema())) {
            String[] schemas = config.getSchema().split(FlinkParamConstant.SPLIT);
            sourceBuilder.schemaList(schemas);
        } else {
            sourceBuilder.schemaList();
        }

        List<String> schemaTableNameList = config.getSchemaTableNameList();
        if (Asserts.isNotNullCollection(schemaTableNameList)) {
            sourceBuilder.tableList(schemaTableNameList.toArray(new String[0]));
        } else {
            sourceBuilder.tableList();
        }

        Map<String, Object> configs = new HashMap<>();
        configs.put(JsonConverterConfig.DECIMAL_FORMAT_CONFIG, DecimalFormat.NUMERIC.name());
        sourceBuilder.deserializer(new JsonDebeziumDeserializationSchema(false, configs));
        sourceBuilder.debeziumProperties(debeziumProperties);

        // 调试日志：记录最终传递给 connector 的配置
        logger.info(
                "VastbaseCDCBuilder.build: Final debeziumProperties keys: {}",
                debeziumProperties.stringPropertyNames());
        logger.info("VastbaseCDCBuilder.build: Final debeziumProperties count: {}", debeziumProperties.size());

        if (Asserts.isNotNullString(decodingPluginName)) {
            sourceBuilder.decodingPluginName(decodingPluginName);
            logger.info("VastbaseCDCBuilder.build: Set decodingPluginName = [{}]", decodingPluginName);
        }

        if (Asserts.isNotNullString(slotName)) {
            sourceBuilder.slotName(slotName);
            logger.info("VastbaseCDCBuilder.build: Set slotName = [{}]", slotName);
        }

        logger.info(
                "VastbaseCDCBuilder.build: Building VastbaseSource with database=[{}], schema=[{}], tables=[{}]",
                config.getDatabase(),
                config.getSchema(),
                config.getSchemaTableNameList());

        return env.addSource(sourceBuilder.build(), "Vastbase CDC Source");
    }
}
