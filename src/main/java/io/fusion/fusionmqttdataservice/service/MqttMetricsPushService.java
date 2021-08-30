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
import io.fusion.core.source.MetricsPushService;
import io.fusion.core.source.PushCallback;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Primary;
import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.Map;

@Component
@Slf4j
@Primary
public class MqttMetricsPushService implements MetricsPushService {
    private final FusionDataServiceConfig fusionDataServiceConfig;
    private final Map<String, MqttSubscriber> subscribers = new HashMap<>();

    @Autowired
    public MqttMetricsPushService(FusionDataServiceConfig fusionDataServiceConfig) {
        this.fusionDataServiceConfig = fusionDataServiceConfig;
    }

    @Override
    public void start(final String jobId, final PushCallback pushCallback) {
        final MqttSubscriber subscriber = new MqttSubscriber(jobId, fusionDataServiceConfig, pushCallback);
        subscribers.put(jobId, subscriber);
        subscriber.start();
    }

    public void stop(String jobId) {
        if (subscribers.containsKey(jobId)) {
            subscribers.get(jobId).stop();
            subscribers.remove(jobId);
        }
    }

    public void stopAll() {
        subscribers.values().forEach(MqttSubscriber::stop);
        subscribers.clear();
    }

    public boolean allStarted() {
        return subscribers.values().stream().allMatch(MqttSubscriber::isStarted);
    }
}
