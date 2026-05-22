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

package org.dinky.cdc.debezium.converter;

import org.dinky.cdc.debezium.DebeziumCustomConverter;

import org.apache.flink.cdc.connectors.shaded.org.apache.kafka.connect.data.SchemaBuilder;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.spi.converter.RelationalColumn;

/**
 * Vastbase 转换器。TIMESTAMP 按源库时区解释后转为 UTC 的 ISO 字符串（带 Z），
 * 避免 CDCSOURCE 入湖 Paimon 等下游将无时区字符串当 UTC 解析导致多 8 小时。
 *
 * @author <a href="mailto:kindbgen@gmail.com">Kindbgen<a/>
 * @description Vastbase 转换器
 * @date 2024/2/6
 */
public class VastbaseDebeziumConverter extends DebeziumCustomConverter {

    private static final Logger logger = LoggerFactory.getLogger(VastbaseDebeziumConverter.class);

    /** 源库时区，用于将 TIMESTAMP 解释为本地时间再转 UTC 输出 */
    private ZoneId serverZoneId;

    private static final DateTimeFormatter ISO_INSTANT = DateTimeFormatter.ISO_INSTANT;

    /** 可能传给 converter 的时区配置 key（不同 connector 实现可能不同） */
    private static final String[] ZONE_PROPERTY_KEYS = {
        "datetime.format.timestamp.zone",
        "database.timezone",
        "server.timezone",
        "timezone",
        "dinky.vastbase.server.timezone"
    };

    @Override
    public void configure(Properties properties) {
        super.configure(properties);
        // 调试日志：打印收到的全部 key-value（敏感值打码）
        logger.info("VastbaseDebeziumConverter.configure: ===== Configuration Debug Start =====");
        logger.info("VastbaseDebeziumConverter.configure: Received {} properties from connector", properties.size());
        if (logger.isDebugEnabled()) {
            properties.stringPropertyNames().forEach(k -> {
                String v = properties.getProperty(k);
                // 敏感信息打码
                if (k.toLowerCase().contains("password") || k.toLowerCase().contains("secret")) {
                    v = "***";
                }
                logger.debug("VastbaseDebeziumConverter.configure: property [{}] = [{}]", k, v);
            });
        } else {
            StringBuilder sb = new StringBuilder("VastbaseDebeziumConverter.configure: property keys: ");
            properties.stringPropertyNames().forEach(k -> sb.append(k).append(", "));
            logger.info(sb.toString());
        }

        String zone = null;
        String foundKey = null;
        // 尝试从多个可能的 key 读取时区
        for (String key : ZONE_PROPERTY_KEYS) {
            zone = properties.getProperty(key);
            if (zone != null && !zone.isEmpty()) {
                foundKey = key;
                logger.info(
                        "VastbaseDebeziumConverter.configure: ✅ Found timezone from property [{}] = [{}]", key, zone);
                break;
            } else {
                logger.debug("VastbaseDebeziumConverter.configure: property [{}] not found or empty", key);
            }
        }

        // 回退1：系统属性
        if (zone == null || zone.isEmpty()) {
            zone = System.getProperty("dinky.vastbase.server.timezone");
            if (zone != null && !zone.isEmpty()) {
                foundKey = "system.property";
                logger.info("VastbaseDebeziumConverter.configure: ✅ Found timezone from system property: [{}]", zone);
            }
        }

        // 回退2：环境变量
        if (zone == null || zone.isEmpty()) {
            String env = System.getenv("DINKY_VASTBASE_SERVER_TIMEZONE");
            if (env != null && !env.isEmpty()) {
                zone = env;
                foundKey = "env.DINKY_VASTBASE_SERVER_TIMEZONE";
                logger.info("VastbaseDebeziumConverter.configure: ✅ Found timezone from env: [{}]", zone);
            }
        }

        // 设置最终使用的时区
        if (zone != null && !zone.isEmpty()) {
            try {
                this.serverZoneId = ZoneId.of(zone);
                logger.info(
                        "VastbaseDebeziumConverter.configure: ✅ Final serverZoneId = [{}] (from {})",
                        this.serverZoneId,
                        foundKey);
            } catch (Exception e) {
                logger.error(
                        "VastbaseDebeziumConverter.configure: ❌ Invalid timezone [{}], using system default", zone, e);
                this.serverZoneId = ZoneId.systemDefault();
            }
        } else {
            this.serverZoneId = ZoneId.systemDefault();
            logger.warn(
                    "VastbaseDebeziumConverter.configure: ⚠️ No timezone config found, using system default [{}]. "
                            + "Set 'source.server-time-zone'='Asia/Shanghai' in CDCSOURCE WITH to fix timestamp +8h.",
                    this.serverZoneId);
        }
        logger.info("VastbaseDebeziumConverter.configure: ===== Configuration Debug End =====");
    }

    @Override
    public void converterFor(
            RelationalColumn relationalColumn, ConverterRegistration<SchemaBuilder> converterRegistration) {
        String columnType = relationalColumn.typeName().toUpperCase();
        this.registerConverter(columnType, converterRegistration);
    }

    /**
     * 将源库的 TIMESTAMP（无时区）按 serverZoneId 解释为本地时刻，再转为 UTC 的 ISO 字符串（带 Z），
     * 下游 DataTypeConverter.convertToTimestamp 用 Instant.parse 解析后与 Paimon server-time-zone 一致。
     *
     * 问题分析：
     * 源库的 TIMESTAMP WITHOUT TIME ZONE 存储的是东8区（Asia/Shanghai）的本地时间，但数据库本身无时区信息。
     *
     * 关键点：
     * 1. 我们通过 JDBC URL 的 timezone 参数设置了数据库会话时区为 serverZoneId（Asia/Shanghai）
     * 2. 如果会话时区设置正确，ts.toLocalDateTime() 应该返回源库存储的本地时间
     * 3. 如果会话时区设置错误（例如是UTC），ts.toLocalDateTime() 可能返回错误的本地时间
     *
     * 解决方案：
     * 直接使用 ts.toLocalDateTime()，假设它返回的是源库存储的本地时间（基于 serverZoneId）。
     * 然后将其解释为 serverZoneId 的本地时间，转换为 UTC ISO 字符串。
     *
     * 注意：如果数据库会话时区设置错误，这个转换可能不正确。
     * 因此，必须确保 JDBC URL 中的 timezone 参数正确设置。
     */
    private String formatTimestampAsUtcIso(Object value) {
        LocalDateTime ldt;
        if (value instanceof java.sql.Timestamp) {
            java.sql.Timestamp ts = (java.sql.Timestamp) value;
            // 关键修复：直接使用 toLocalDateTime()，假设它返回的是源库存储的本地时间
            // 这是因为我们在 JDBC URL 中设置了 timezone=serverZoneId，确保数据库会话时区是源库时区
            // 这样 ts.toLocalDateTime() 返回的就是源库的本地时间表示
            //
            // 数据流分析：
            // 1. 源库存储：2025-01-09 10:00:00（东8区本地时间，无时区信息）
            // 2. JDBC URL timezone=Asia/Shanghai：确保数据库会话时区是东8区
            // 3. Debezium 读取：将 "2025-01-09 10:00:00" 解释为东8区本地时间
            // 4. ts.getTime()：返回 2025-01-09T02:00:00Z 的 UTC 时间戳（正确）
            // 5. ts.toLocalDateTime()：返回 2025-01-09 10:00:00（源库的本地时间，正确）
            // 6. 将其解释为 Asia/Shanghai 本地时间并转换为 UTC：2025-01-09T02:00:00Z（正确）
            // 7. DataTypeConverter 将其转换为 Asia/Shanghai 本地时间：2025-01-09 10:00:00（正确）
            ldt = ts.toLocalDateTime();
        } else if (value instanceof java.time.LocalDateTime) {
            ldt = (java.time.LocalDateTime) value;
        } else {
            logger.debug(
                    "VastbaseDebeziumConverter.formatTimestampAsUtcIso: Unsupported value type: {}",
                    value != null ? value.getClass() : "null");
            return null;
        }

        // 调试日志：记录时间转换过程
        if (logger.isDebugEnabled()) {
            logger.debug(
                    "VastbaseDebeziumConverter.formatTimestampAsUtcIso: Input LocalDateTime = [{}], serverZoneId = [{}]",
                    ldt,
                    serverZoneId);
        }

        // 将 LocalDateTime 解释为 serverZoneId 的本地时间，然后转换为 UTC
        // 这确保了源库的本地时间被正确转换为 UTC ISO 字符串
        Instant instant = ldt.atZone(serverZoneId).toInstant();
        String utcIso = ISO_INSTANT.format(instant);

        // 调试日志：记录转换结果（每100条记录打印一次，帮助诊断问题）
        if (logger.isInfoEnabled()) {
            long currentTime = System.currentTimeMillis();
            // 每100条记录打印一次
            if (currentTime % 100 < 10) {
                logger.info(
                        "VastbaseDebeziumConverter.formatTimestampAsUtcIso: TIMESTAMP conversion: LocalDateTime=[{}] (interpreted as {}) -> UTC Instant=[{}] -> ISO String=[{}]",
                        ldt,
                        serverZoneId,
                        instant,
                        utcIso);
            }
        }

        return utcIso;
    }

    public void registerConverter(String columnType, ConverterRegistration<SchemaBuilder> converterRegistration) {
        String schemaName = this.schemaNamePrefix + "." + columnType.toLowerCase();
        schemaBuilder = SchemaBuilder.string().name(schemaName);
        switch (columnType) {
            case "DATE":
                converterRegistration.register(schemaBuilder, value -> {
                    if (value == null) {
                        return null;
                    } else if (value instanceof java.sql.Date) {
                        return dateFormatter.format(((java.sql.Date) value).toLocalDate());
                    } else if (value instanceof java.time.LocalDate) {
                        return dateFormatter.format((java.time.LocalDate) value);
                    } else {
                        return this.failConvert(value, schemaName);
                    }
                });
                break;
            case "TIME":
                converterRegistration.register(schemaBuilder, value -> {
                    if (value == null) {
                        return null;
                    } else if (value instanceof java.sql.Time) {
                        return timeFormatter.format(((java.sql.Time) value).toLocalTime());
                    } else if (value instanceof java.time.LocalTime) {
                        return timeFormatter.format((java.time.LocalTime) value);
                    } else {
                        return this.failConvert(value, schemaName);
                    }
                });
                break;
            case "TIMESTAMP":
                converterRegistration.register(schemaBuilder, value -> {
                    if (value == null) {
                        logger.debug("VastbaseDebeziumConverter: TIMESTAMP value is null");
                        return null;
                    }
                    // 调试日志：记录原始值类型和值（每100条记录打印一次）
                    if (logger.isInfoEnabled()) {
                        long currentTime = System.currentTimeMillis();
                        if (currentTime % 100 < 10) {
                            logger.info(
                                    "VastbaseDebeziumConverter: Converting TIMESTAMP value: type=[{}], value=[{}], serverZoneId=[{}]",
                                    value.getClass().getName(),
                                    value,
                                    serverZoneId);
                        }
                    }
                    String utcIso = formatTimestampAsUtcIso(value);
                    if (utcIso != null) {
                        if (logger.isInfoEnabled()) {
                            long currentTime = System.currentTimeMillis();
                            if (currentTime % 100 < 10) {
                                logger.info(
                                        "VastbaseDebeziumConverter: TIMESTAMP converted: [{}] -> [{}]", value, utcIso);
                            }
                        }
                        return utcIso;
                    }
                    logger.warn(
                            "VastbaseDebeziumConverter: Failed to convert TIMESTAMP value: type=[{}], value=[{}]",
                            value.getClass().getName(),
                            value);
                    return this.failConvert(value, schemaName);
                });
                break;
            default:
                schemaBuilder = null;
                break;
        }
    }
}
