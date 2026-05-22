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

import java.lang.reflect.Method;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.Properties;

import org.junit.Assert;
import org.junit.Test;

/** 验证全量快照与增量 WAL 两条路径输出一致的 UTC ISO 字符串。 */
public class VastbaseDebeziumConverterTest {

    private static final String SHANGHAI = "Asia/Shanghai";

    private VastbaseDebeziumConverter newConverter() {
        VastbaseDebeziumConverter converter = new VastbaseDebeziumConverter();
        Properties properties = new Properties();
        properties.setProperty("database.type", "vastbase");
        properties.setProperty("dinky.vastbase.server.timezone", SHANGHAI);
        converter.configure(properties);
        return converter;
    }

    private String toUtcIso(VastbaseDebeziumConverter converter, Object value) throws Exception {
        Method method =
                VastbaseDebeziumConverter.class.getDeclaredMethod("formatTimestampAsUtcIso", Object.class);
        method.setAccessible(true);
        return (String) method.invoke(converter, value);
    }

    /** 增量 WAL：Instant 墙钟被误标为 UTC，应减 8 小时输出真 UTC。 */
    @Test
    public void walInstantConvertsWallClockToTrueUtc() throws Exception {
        VastbaseDebeziumConverter converter = newConverter();
        Instant walInstant = Instant.parse("2026-05-13T17:32:21.237600Z");
        String utcIso = toUtcIso(converter, walInstant);
        Assert.assertEquals("2026-05-13T09:32:21.237600Z", utcIso);
    }

    /** 全量快照：java.sql.Timestamp 墙钟与 WAL 同值时应得到相同 UTC ISO。 */
    @Test
    public void snapshotTimestampMatchesWalInstantOutput() throws Exception {
        VastbaseDebeziumConverter converter = newConverter();
        Timestamp snapshot = Timestamp.valueOf("2026-05-13 17:32:21.237600");
        Instant walInstant = Instant.parse("2026-05-13T17:32:21.237600Z");
        Assert.assertEquals(toUtcIso(converter, snapshot), toUtcIso(converter, walInstant));
    }

    /** failConvert 旧路径：错误 ISO 字符串经转换后也应得到真 UTC。 */
    @Test
    public void wrongUtcIsoStringIsCorrected() throws Exception {
        VastbaseDebeziumConverter converter = newConverter();
        String utcIso = toUtcIso(converter, "2026-01-29T17:09:20.333501Z");
        Assert.assertEquals("2026-01-29T09:09:20.333501Z", utcIso);
    }
}
