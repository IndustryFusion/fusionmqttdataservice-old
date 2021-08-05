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

import io.fusion.core.config.FusionDataServiceConfig;
import io.fusion.core.exception.ConfigurationException;
import io.fusion.core.exception.JobNotFoundException;
import io.fusion.core.source.PushCallback;
import io.fusion.fusionmqttdataservice.exception.MqttClientException;
import io.fusion.fusionmqttdataservice.parser.JsonPayloadQuerier;
import lombok.extern.slf4j.Slf4j;
import org.eclipse.paho.client.mqttv3.IMqttDeliveryToken;
import org.eclipse.paho.client.mqttv3.MqttCallbackExtended;
import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;

@Slf4j
public class MqttSubscriber implements MqttCallbackExtended {
    private final FusionDataServiceConfig fusionDataServiceConfig;
    private final PushCallback pushCallback;
    private final String jobId;

    private final String clientId = MqttMetricsPushService.class.getName() + ":" + UUID.randomUUID();
    private MqttClient mqttClient;
    private Map<String, JsonPayloadQuerier> topicJsonPayloadQuerierMap;
    private boolean isStarted = false;

    public MqttSubscriber(String jobId, FusionDataServiceConfig fusionDataServiceConfig, PushCallback pushCallback) {
        this.jobId = jobId;
        this.fusionDataServiceConfig = fusionDataServiceConfig;
        this.pushCallback = pushCallback;
    }

    public void start() {
        log.info("Start subscription for job {}", jobId);
        var jobSpec = fusionDataServiceConfig.getJobSpecs().get(jobId);
        if (jobSpec == null) {
            throw new JobNotFoundException();
        }

        final Map<String, Map<String, String>> topicTargetQueryMap = new HashMap<>();
        for (FusionDataServiceConfig.FieldSpec fieldSpec : jobSpec.getFields()) {
            String[] topicQueryPair = fieldSpec.getSource().split(":");
            if (topicQueryPair.length != 2) {
                throw new ConfigurationException();
            }
            final String topic = topicQueryPair[0];
            final String query = topicQueryPair[1];
            Map<String, String> targetQueryMap = topicTargetQueryMap.computeIfAbsent(topic, k -> new HashMap<>());
            if (targetQueryMap.containsKey(fieldSpec.getTarget())) {
                throw new ConfigurationException();
            }
            targetQueryMap.put(fieldSpec.getTarget(), query);
        }
        topicJsonPayloadQuerierMap = topicTargetQueryMap.entrySet().stream()
                .collect(Collectors.toMap(Map.Entry::getKey, o -> new JsonPayloadQuerier(o.getValue())));

        final var brokerUrl = fusionDataServiceConfig.getConnectionString();

        var connOpts = new MqttConnectOptions();
        connOpts.setCleanSession(true);
        connOpts.setAutomaticReconnect(true);
        try {
            mqttClient = new MqttClient(brokerUrl, clientId, new MemoryPersistence());
        } catch (MqttException e) {
            throw new MqttClientException("Create", e);
        }
        log.info("Connecting to broker: {}", brokerUrl);
        try {
            mqttClient.connect(connOpts);
            mqttClient.setCallback(this);
        } catch (MqttException e) {
            throw new MqttClientException("Connect", e);
        }

        final String[] topics = extractTopics();
        if (log.isInfoEnabled()) {
            log.info("Subscribing to: {}", Arrays.toString(topics));
        }
        try {
            mqttClient.subscribe(topics);
        } catch (MqttException e) {
            throw new MqttClientException("Subscribe", e);
        }
        isStarted = true;
    }

    @Override
    public void connectionLost(Throwable cause) {
        log.warn("MqttClient connectionLost (reconnecting): ", cause);
    }

    @Override
    public void messageArrived(String topic, MqttMessage message) {
        try {
            processMessage(topic, message);
        } catch (Exception e) {
            log.warn("messageArrived: uncaught exception: " + message, e);
        }
    }

    private void processMessage(final String topic, final MqttMessage message) {
        if (message == null) {
            log.warn("messageArrived: message null");
            return;
        }

        var payloadParser = topicJsonPayloadQuerierMap.get(topic);
        Map<String, Object> metrics = payloadParser.queryPayload(message.getPayload());
        log.info("Metrics arrived on topic {}: {} mapped", topic, metrics.size());

        try {
            pushCallback.handleMetrics(jobId, metrics);
        } catch (Exception e) {
            log.error("messageArrived: metric filtering failed " + new String(message.getPayload()), e);
        }
    }

    @Override
    public void deliveryComplete(IMqttDeliveryToken token) {
        log.error("deliveryComplete called although no messages should be sent!!! ");
    }

    @Override
    public void connectComplete(boolean reconnect, String serverUri) {
        log.info("Re-Connection Attempt {}", reconnect);
        if (reconnect) {
            try {
                this.mqttClient.subscribe(extractTopics());
            } catch (MqttException e) {
                throw new MqttClientException("Subscribe", e);
            }
        }
    }

    private String[] extractTopics() {
        return this.topicJsonPayloadQuerierMap.keySet().toArray(String[]::new);
    }

    public void stop() {
        try {
            if (mqttClient != null && mqttClient.isConnected()) {
                mqttClient.disconnect();
            }
        } catch (MqttException e) {
            log.error("MqttClient topic unsubscribe error: ", e);
        }
        isStarted = false;
    }

    public boolean isStarted() {
        return isStarted;
    }
}
