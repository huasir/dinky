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

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

public class VastbaseCDCBuilder extends AbstractCDCBuilder implements CDCBuilder {
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
        String format = "jdbc:vastbase://%s:%s/%s";
        return String.format(format, config.getHostname(), config.getPort(), config.getDatabase());
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
        String decodingPluginName = config.getSource().get("decoding.plugin.name");
        String slotName = config.getSource().get("slot.name");

        Properties debeziumProperties = new Properties();
        for (Map.Entry<String, String> entry : config.getDebezium().entrySet()) {
            if (Asserts.isNotNullString(entry.getKey()) && Asserts.isNotNullString(entry.getValue())) {
                debeziumProperties.setProperty(entry.getKey(), entry.getValue());
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

        if (Asserts.isNotNullString(decodingPluginName)) {
            sourceBuilder.decodingPluginName(decodingPluginName);
        }

        if (Asserts.isNotNullString(slotName)) {
            sourceBuilder.slotName(slotName);
        }

        return env.addSource(sourceBuilder.build(), "Vastbase CDC Source");
    }
}
