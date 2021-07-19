/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
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

package io.fusion.fusionmqttdataservice.service;

import com.github.tomakehurst.wiremock.WireMockServer;
import com.github.tomakehurst.wiremock.core.WireMockConfiguration;
import com.github.tomakehurst.wiremock.stubbing.ServeEvent;
import io.fusion.core.config.FusionDataServiceConfig;
import io.fusion.core.job.JobManager;
import io.fusion.fusionmqttdataservice.MqttBaseTest;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.junit.jupiter.SpringExtension;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

@ExtendWith(SpringExtension.class)
@SpringBootTest
@ActiveProfiles("single")
@Testcontainers
class SingleMqttMetricsTest extends MqttBaseTest {
    @Container
    protected final GenericContainer<?> mqttContainer = new GenericContainer<>("eclipse-mosquitto:1.6.8")
            .withExposedPorts(1883);

    protected static final WireMockServer wireMockServer = new WireMockServer(WireMockConfiguration.options().dynamicPort());

    @BeforeAll
    static void init() {
        wireMockServer.start();
    }

    @BeforeEach
    void setup() {
        wireMockServer.resetAll();
        fusionDataServiceConfig.setConnectionString(getMqttUrl());
        fusionDataServiceConfig.setDownstreamServiceBaseUrl("http://localhost:" + wireMockServer.port());
    }
    
    @AfterAll
    static void teardown() {
        wireMockServer.stop();
    }

    protected Callable<Boolean> webserviceCalls(final int count) {
        return () -> wireMockServer.getAllServeEvents().size() == count;
    }

    protected String getMqttUrl() {
        return "tcp://" + mqttContainer.getContainerIpAddress() + ":" + mqttContainer.getMappedPort(1883);
    }

    @Test
    void normal() throws MqttException {
        initJobManager();

        final Map<String, String> topicValueMap = new HashMap<>();
        topicValueMap.put("Leitungsdruck", "2.0");
        topicValueMap.put("Entnahmedruck_1", "12.1");
        topicValueMap.put("Flaschendruck_Rechts", "0.1");
        topicValueMap.put("Flaschendruck_Links", "137.8");

        initiateSingleMqtt(fusionDataServiceConfig.getConnectionString(), topicValueMap);

        await().until(webserviceCalls(4));

        List<ServeEvent> events = wireMockServer.getAllServeEvents();

        assertThat(events.size()).isEqualTo(4);

        final Map<String, String> expectedMetrics = new HashMap<>();
        expectedMetrics.put("pressure_bottle_right", "0.1");
        expectedMetrics.put("pressure_takeoff_1", "12.1");
        expectedMetrics.put("pressure_pipe", "2.0");
        expectedMetrics.put("pressure_bottle_left", "137.8");

        final Map<String, String> actualMetrics = collectAllReceivedMetrics(events);
        assertThat(actualMetrics).containsExactlyInAnyOrderEntriesOf(expectedMetrics);
        mqttMetricsPushService.stop();
    }

    @Test
    void normalJsonFewer() throws MqttException {
        initJobManager();

        final Map<String, String> topicValueMap = new HashMap<>();
        topicValueMap.put("Dummy", "YYY");
        topicValueMap.put("Flaschendruck_Rechts", "0.1");
        topicValueMap.put("Flaschendruck_Links", "137.8");

        initiateSingleMqtt(fusionDataServiceConfig.getConnectionString(), topicValueMap);

        await().until(webserviceCalls(2));

        List<ServeEvent> events = wireMockServer.getAllServeEvents();

        assertThat(events.size()).isEqualTo(2);

        final Map<String, String> expectedMetrics = new HashMap<>();
        expectedMetrics.put("pressure_bottle_right", "0.1");
        expectedMetrics.put("pressure_bottle_left", "137.8");

        final Map<String, String> actualMetrics = collectAllReceivedMetrics(events);
        assertThat(actualMetrics).containsExactlyInAnyOrderEntriesOf(expectedMetrics);
        jobManager.cancel();
    }

    @Test
    void emptyFile() throws IOException, MqttException {
        initJobManager();

        initiateMqtt(fusionDataServiceConfig.getConnectionString(), "emptyfile.txt", "Gasentnahmestelle");

        jobManager.cancel();
    }
}
