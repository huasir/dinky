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
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.spi.converter.RelationalColumn;

/**
 * Vastbase 转换器。TIMESTAMP 按源库时区解释后转为 UTC 的 ISO 字符串（带 Z），
 * 避免 CDCSOURCE 入湖 Paimon 等下游将无时区字符串当 UTC 解析导致多 8 小时。
 *
 * <p>全量快照通常传入 {@link java.sql.Timestamp}；增量 WAL 通常传入 {@link Instant}，
 * 后者将源库墙钟时间误编码为 UTC，需单独处理。
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
        String zone = null;
        for (String key : ZONE_PROPERTY_KEYS) {
            zone = properties.getProperty(key);
            if (zone != null && !zone.isEmpty()) {
                break;
            }
        }
        if (zone == null || zone.isEmpty()) {
            zone = System.getProperty("dinky.vastbase.server.timezone");
        }
        if (zone == null || zone.isEmpty()) {
            zone = System.getenv("DINKY_VASTBASE_SERVER_TIMEZONE");
        }
        if (zone != null && !zone.isEmpty()) {
            try {
                this.serverZoneId = ZoneId.of(zone);
            } catch (Exception e) {
                logger.warn("Invalid timezone [{}], using system default", zone, e);
                this.serverZoneId = ZoneId.systemDefault();
            }
        } else {
            this.serverZoneId = ZoneId.systemDefault();
            logger.warn(
                    "No timezone config for VastbaseDebeziumConverter, using [{}]. "
                            + "Set 'source.server-time-zone'='Asia/Shanghai' in CDCSOURCE WITH.",
                    this.serverZoneId);
        }
        logger.info("VastbaseDebeziumConverter configured, serverZoneId={}", serverZoneId);
    }

    @Override
    public void converterFor(
            RelationalColumn relationalColumn, ConverterRegistration<SchemaBuilder> converterRegistration) {
        String columnType = relationalColumn.typeName().toUpperCase();
        this.registerConverter(columnType, converterRegistration);
    }

    /**
     * 从 Debezium 传入值中提取源库 TIMESTAMP 的墙钟时间（无时区 LocalDateTime）。
     *
     * <ul>
     *   <li>快照 {@link java.sql.Timestamp}：会话时区为 serverZoneId 时，toLocalDateTime 即墙钟。</li>
     *   <li>增量 {@link Instant} / epoch / ISO 字符串：墙钟被当作 UTC 编码，按 UTC 取出分量。</li>
     * </ul>
     */
    private LocalDateTime extractWallClock(Object value) {
        if (value instanceof java.sql.Timestamp) {
            return ((java.sql.Timestamp) value).toLocalDateTime();
        }
        if (value instanceof LocalDateTime) {
            return (LocalDateTime) value;
        }
        if (value instanceof Instant) {
            return LocalDateTime.ofInstant((Instant) value, ZoneOffset.UTC);
        }
        if (value instanceof OffsetDateTime) {
            return LocalDateTime.ofInstant(((OffsetDateTime) value).toInstant(), ZoneOffset.UTC);
        }
        if (value instanceof ZonedDateTime) {
            return LocalDateTime.ofInstant(((ZonedDateTime) value).toInstant(), ZoneOffset.UTC);
        }
        if (value instanceof Long) {
            return LocalDateTime.ofInstant(Instant.ofEpochMilli((Long) value), ZoneOffset.UTC);
        }
        if (value instanceof Integer) {
            return LocalDateTime.ofInstant(Instant.ofEpochMilli(((Integer) value).longValue()), ZoneOffset.UTC);
        }
        if (value instanceof String) {
            String s = ((String) value).trim();
            if (s.isEmpty()) {
                return null;
            }
            Instant instant = Instant.parse(s);
            return LocalDateTime.ofInstant(instant, ZoneOffset.UTC);
        }
        return null;
    }

    /**
     * 墙钟时间（源库 serverZoneId）→ 真 UTC Instant → ISO-8601 字符串（带 Z）。
     */
    private String formatTimestampAsUtcIso(Object value) {
        LocalDateTime wallClock = extractWallClock(value);
        if (wallClock == null) {
            return null;
        }
        Instant utc = wallClock.atZone(serverZoneId).toInstant();
        return ISO_INSTANT.format(utc);
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
                        return null;
                    }
                    String utcIso = formatTimestampAsUtcIso(value);
                    if (utcIso != null) {
                        return utcIso;
                    }
                    logger.warn(
                            "Unsupported TIMESTAMP value type [{}]: {}",
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
