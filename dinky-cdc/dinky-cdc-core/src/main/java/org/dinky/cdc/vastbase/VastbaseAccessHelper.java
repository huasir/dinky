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

import java.lang.reflect.Method;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Helper class to access protected methods in PostgresConnectorConfig
 * This is a workaround for the IllegalAccessError issue with Vastbase connector
 *
 * @description Vastbase Access Helper
 */
public class VastbaseAccessHelper {

    private static final Logger logger = LoggerFactory.getLogger(VastbaseAccessHelper.class);

    /**
     * Get Snapshotter from PostgresConnectorConfig using reflection
     *
     * @param config PostgresConnectorConfig instance
     * @return Snapshotter instance
     */
    public static Object getSnapshotter(Object config) {
        try {
            Class<?> configClass = config.getClass();
            Method getSnapshotterMethod = configClass.getDeclaredMethod("getSnapshotter");
            getSnapshotterMethod.setAccessible(true);
            return getSnapshotterMethod.invoke(config);
        } catch (Exception e) {
            logger.error("Failed to access getSnapshotter method via reflection", e);
            throw new RuntimeException("Failed to access getSnapshotter method", e);
        }
    }

    /**
     * Initialize access permissions at startup
     * This method should be called before using Vastbase connector
     */
    public static void initializeAccessPermissions() {
        try {
            // Try to set accessible for common methods that might be needed
            Class<?> postgresConfigClass = Class.forName("io.debezium.connector.postgresql.PostgresConnectorConfig");
            Method[] methods = postgresConfigClass.getDeclaredMethods();
            for (Method method : methods) {
                if (method.getName().equals("getSnapshotter")) {
                    method.setAccessible(true);
                    logger.info("Successfully set accessible for getSnapshotter method");
                    break;
                }
            }
        } catch (Exception e) {
            logger.warn("Failed to initialize access permissions, this may cause issues", e);
        }
    }
}
