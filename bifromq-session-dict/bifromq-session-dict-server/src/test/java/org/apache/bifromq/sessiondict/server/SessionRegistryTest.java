/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.bifromq.sessiondict.server;

import static org.apache.bifromq.metrics.ITenantMeter.stopGauging;
import static org.apache.bifromq.metrics.TenantMetric.MqttConnectionGauge;
import static org.apache.bifromq.metrics.TenantMetric.MqttLivePersistentSessionGauge;
import static org.apache.bifromq.type.MQTTClientInfoConstants.MQTT_CHANNEL_ID_KEY;
import static org.apache.bifromq.type.MQTTClientInfoConstants.MQTT_CLIENT_ID_KEY;
import static org.apache.bifromq.type.MQTTClientInfoConstants.MQTT_CLIENT_SESSION_TYPE;
import static org.apache.bifromq.type.MQTTClientInfoConstants.MQTT_CLIENT_SESSION_TYPE_P_VALUE;
import static org.apache.bifromq.type.MQTTClientInfoConstants.MQTT_CLIENT_SESSION_TYPE_T_VALUE;
import static org.apache.bifromq.type.MQTTClientInfoConstants.MQTT_USER_ID_KEY;
import static org.mockito.ArgumentMatchers.eq;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.Meter;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import lombok.SneakyThrows;
import org.apache.bifromq.metrics.ITenantMeter;
import org.apache.bifromq.metrics.TenantMetric;
import org.apache.bifromq.sessiondict.rpc.proto.ServerRedirection;
import org.apache.bifromq.type.ClientInfo;
import org.mockito.Mockito;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class SessionRegistryTest {
    private final String tenantId1 = "tenant1";
    private final String tenantId2 = "tenant2";
    private SimpleMeterRegistry meterRegistry;
    private SessionRegistry sessionRegistry;

    @BeforeMethod
    public void setUp() {
        sessionRegistry = new SessionRegistry();
        meterRegistry = new SimpleMeterRegistry();
        Metrics.globalRegistry.add(meterRegistry);
    }

    @AfterMethod
    @SneakyThrows
    public void tearDown() {
        Metrics.globalRegistry.getMeters().forEach(Metrics.globalRegistry::remove);
        Metrics.globalRegistry.remove(meterRegistry);
        stopGauging(tenantId1, MqttConnectionGauge);
        stopGauging(tenantId1, MqttLivePersistentSessionGauge);
        stopGauging(tenantId2, MqttLivePersistentSessionGauge);
        stopGauging(tenantId2, MqttLivePersistentSessionGauge);
    }

    @Test
    public void testAddAndGetSession() {
        ISessionRegister registerMock = Mockito.mock(ISessionRegister.class);

        ClientInfo sessionOwner = ClientInfo.newBuilder()
            .setTenantId(tenantId1)
            .putMetadata(MQTT_USER_ID_KEY, "user1")
            .putMetadata(MQTT_CLIENT_ID_KEY, "client1")
            .build();

        sessionRegistry.add(sessionOwner, registerMock);

        Optional<ClientInfo> retrieved = sessionRegistry.get("tenant1", "user1", "client1");
        assertTrue(retrieved.isPresent());
        assertEquals(sessionOwner, retrieved.get());
        assertGaugeValue(sessionOwner.getTenantId(), MqttConnectionGauge, 1.0);
        assertGaugeValue(sessionOwner.getTenantId(), MqttLivePersistentSessionGauge, 0.0);
    }

    @Test
    public void testAddMultipleSessions() {
        ISessionRegister registerMock1 = Mockito.mock(ISessionRegister.class);
        ISessionRegister registerMock2 = Mockito.mock(ISessionRegister.class);

        ClientInfo sessionOwner1 = ClientInfo.newBuilder()
            .setTenantId(tenantId1)
            .putMetadata(MQTT_USER_ID_KEY, "user1")
            .putMetadata(MQTT_CLIENT_ID_KEY, "client1")
            .putMetadata(MQTT_CLIENT_SESSION_TYPE, MQTT_CLIENT_SESSION_TYPE_T_VALUE)
            .build();

        ClientInfo sessionOwner2 = ClientInfo.newBuilder()
            .setTenantId(tenantId2)
            .putMetadata(MQTT_USER_ID_KEY, "user2")
            .putMetadata(MQTT_CLIENT_ID_KEY, "client2")
            .putMetadata(MQTT_CLIENT_SESSION_TYPE, MQTT_CLIENT_SESSION_TYPE_P_VALUE)
            .build();

        sessionRegistry.add(sessionOwner1, registerMock1);
        sessionRegistry.add(sessionOwner2, registerMock2);

        Optional<ClientInfo> retrieved1 = sessionRegistry.get(tenantId1, "user1", "client1");
        Optional<ClientInfo> retrieved2 = sessionRegistry.get(tenantId2, "user2", "client2");

        assertTrue(retrieved1.isPresent());
        assertEquals(sessionOwner1, retrieved1.get());
        assertTrue(retrieved2.isPresent());
        assertEquals(sessionOwner2, retrieved2.get());
        assertGaugeValue(sessionOwner1.getTenantId(), MqttConnectionGauge, 1.0);
        assertGaugeValue(sessionOwner1.getTenantId(), MqttLivePersistentSessionGauge, 0.0);

        assertGaugeValue(sessionOwner2.getTenantId(), MqttConnectionGauge, 1.0);
        assertGaugeValue(sessionOwner2.getTenantId(), MqttLivePersistentSessionGauge, 1.0);
    }

    @Test
    public void testAddDuplicatePersistentSession() {
        ISessionRegister registerMock1 = Mockito.mock(ISessionRegister.class);
        ISessionRegister registerMock2 = Mockito.mock(ISessionRegister.class);

        ClientInfo sessionOwner = ClientInfo.newBuilder()
            .setTenantId(tenantId1)
            .putMeta

...[Truncated]
