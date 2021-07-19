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

package io.fusion.fusionmqttdataservice;

import com.github.tomakehurst.wiremock.stubbing.ServeEvent;
import com.google.gson.JsonElement;
import com.google.gson.JsonParser;
import io.fusion.core.config.FusionDataServiceConfig;
import io.fusion.core.job.JobManager;
import io.fusion.fusionmqttdataservice.service.MqttMetricsPushService;
import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

public abstract class MqttBaseTest {
    private static final Logger LOG = LoggerFactory.getLogger(MqttBaseTest.class);

    @Autowired
    protected JobManager jobManager;

    @Autowired
    protected FusionDataServiceConfig fusionDataServiceConfig;

    @Autowired
    protected MqttMetricsPushService mqttMetricsPushService;

    protected Map<String, String> collectAllReceivedMetrics(List<ServeEvent> serveEvents) {
        final Map<String, String> collectedMetrics = new HashMap<>();
        serveEvents.forEach(serveEvent ->
                collectedMetrics.putAll(convertJsonToMetricsMap(serveEvent.getRequest().getBodyAsString())));
        return collectedMetrics;
    }

    protected Map<String, String> convertJsonToMetricsMap(final String json) {
        final JsonParser parser = new JsonParser();
        final JsonElement jsonElement = parser.parse(json);

        return jsonElement.getAsJsonObject().entrySet().stream()
                .collect(Collectors.toMap(Map.Entry::getKey, stringJsonElementEntry -> stringJsonElementEntry.getValue().getAsString()));
    }

    protected byte[] readMessageFileToByteArray(final String filename) throws IOException {
        File file = new File("src/test/resources/messages/" + filename);
        return Files.readAllBytes(file.toPath());
    }

    protected void initiateMqtt(final String url, final String filename, final String topic) throws MqttException, IOException {
        MqttClient mqttClient = new MqttClient(url, UUID.randomUUID().toString(),
                new MemoryPersistence());
        mqttClient.connect();
        MqttMessage mqttMessage = new MqttMessage(readMessageFileToByteArray(filename));
        mqttClient.publish(topic, mqttMessage);
        LOG.info("Sent message to topic: {} ({})", topic, mqttMessage);
    }

    protected void initiateSingleMqtt(final String url, final Map<String, String> topicValueMap) throws MqttException {
        MqttClient mqttClient = new MqttClient(url, UUID.randomUUID().toString(),
                new MemoryPersistence());
        mqttClient.connect();

        topicValueMap.forEach((topic, value) -> {
            MqttMessage mqttMessage = new MqttMessage(value.getBytes());
            try {
                mqttClient.publish(topic, mqttMessage);
            } catch (MqttException e) {
                LOG.warn("Error publishing message {} to topic {}: {}", value, topic, e);
            }
            LOG.info("Sent message to topic: {} ({})", topic, mqttMessage);
        });
    }

    protected Callable<Boolean> isFinished() {
        return () -> jobManager.isDone();
    }

    protected Callable<Boolean> isStarted() {
        return () -> mqttMetricsPushService.isStarted();
    }

    protected void initJobManager() {
        jobManager.cancel();
        await().until(isFinished());
        jobManager.start();
        await().until(isStarted());
    }
}
