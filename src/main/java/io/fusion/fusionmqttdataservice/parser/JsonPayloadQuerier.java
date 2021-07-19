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

package io.fusion.fusionmqttdataservice.parser;

import com.jayway.jsonpath.Configuration;
import com.jayway.jsonpath.JsonPath;
import com.jayway.jsonpath.Option;
import com.jayway.jsonpath.ReadContext;
import lombok.extern.slf4j.Slf4j;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

@Slf4j
public class JsonPayloadQuerier {
    final Map<String, JsonPath> targetPathMap;

    public JsonPayloadQuerier(Map<String, String> targetQueryMap) {
        this.targetPathMap = targetQueryMap.entrySet().stream()
                .collect(Collectors.toMap(Map.Entry::getKey,
                        o -> JsonPath.compile(o.getValue())));
    }


    public Map<String, String> queryPayload(byte[] payload) {
        var strPayload = new String(payload);
        final Configuration conf = Configuration.builder().options(Option.SUPPRESS_EXCEPTIONS).build();
        ReadContext ctx;
        try {
            ctx = JsonPath.parse(strPayload, conf);
        } catch (Exception e) {
            log.warn("messageArrived: json parse exception " + strPayload, e);
            return Collections.emptyMap();
        }

        final Map<String, String> selectedValues = new HashMap<>();
        for (Map.Entry<String, JsonPath> entry : targetPathMap.entrySet()) {
            String value = null;
            if (entry.getValue().isDefinite()) {
                Object found = ctx.read(entry.getValue());
                if (found != null) {
                    value = found.toString();
                }
            } else {
                String[] values = ctx.read(entry.getValue());
                if (values.length > 0) {
                    value = values[0];
                }
            }
            if (value != null) {
                selectedValues.put(entry.getKey(), value);
            }
        }

        return selectedValues;
    }
}
