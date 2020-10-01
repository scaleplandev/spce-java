// Copyright 2020 Scale Plan Yazılım A.Ş.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

import io.scaleplan.spce.CloudEvent;
import io.scaleplan.spce.MutableCloudEvent;

import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class TestUtils {
    static byte[] loadFromResource(String name) {
        try {
            URL fileUrl = Objects.requireNonNull(TestUtils.class.getClassLoader().getResource(name));
            return Files.readAllBytes(Paths.get(fileUrl.toURI()));
        } catch (IOException | URISyntaxException e) {
            throw new RuntimeException(e);
        }
    }

    public static void checkEvent(CloudEvent event, byte[] targetData, Object... targetNameValues) {
        assertArrayEquals(targetData, event.getData());
        assert targetNameValues.length % 2 == 0;
        Map<String, Object> targetMap = new HashMap<>(targetNameValues.length / 2);
        String name = "";
        for (int i = 0; i < targetNameValues.length; i++) {
            if (i % 2 == 0) name = (String) targetNameValues[i];
            else targetMap.put(name, targetNameValues[i]);
        }
        Map<String, Object> eventMap = event.getAttributes();
        assertEquals(targetMap.keySet(), eventMap.keySet());
        for (Map.Entry<String, Object> kv : targetMap.entrySet()) {
            Object value = kv.getValue();
            if (value instanceof byte[]) {
                assertArrayEquals((byte[]) value, (byte[]) eventMap.get(kv.getKey()));
            } else {
                assertEquals(kv.getValue(), eventMap.get(kv.getKey()));
            }
        }
    }

    public static byte[] sampleData() {
        String text = "{\"user_id\": \"bc1459c5-378d-4835-b4b6-a2c7d9ca75e3\", \"spo2\": 96, \"event\": \"OximiterMeasured\"}";
        return text.getBytes(StandardCharsets.UTF_8);
    }

    public static byte[] sampleBinaryData() {
        return new byte[]{1, 2, 3, 4};
    }

    public static CloudEvent sampleEventWithRequiredAttributes() {
        return CloudEvent.builder()
                .setType("OximeterMeasured")
                .setSource("/user/123#")
                .setId("567")
                .build();
    }

    public static CloudEvent sampleEventWithOptionalAttributes() {
        MutableCloudEvent event = (MutableCloudEvent) sampleEventWithRequiredAttributes();
        event
                .setTime("2020-07-13T09:15:12Z")
                .setDataContentType("application/json")
                .setDataUnsafe(TestUtils.sampleData())
                .setDataSchema("http://json-schema.org/draft-07/schema#")
                .setSubject("SampleSubject");
        return event;
    }

    public static CloudEvent sampleEventWithOptionalAttributesAndBinaryData() {
        MutableCloudEvent event = (MutableCloudEvent) sampleEventWithRequiredAttributes();
        event
                .setTime("2020-07-13T09:15:12Z")
                .setDataContentType("application/octet-stream")
                .setDataUnsafe(TestUtils.sampleBinaryData())
                .setDataSchema("http://json-schema.org/draft-07/schema#")
                .setSubject("SampleSubject");
        return event;
    }

    public static CloudEvent sampleEventWithOptionalAndExtendedAttributes() {
        MutableCloudEvent event = (MutableCloudEvent) sampleEventWithOptionalAttributes();
        event
                .put("compmstring", "string-value")
                .put("compmint", 42)
                .put("compmbool", true);
        return event;
    }

    public static Object[] stringToSortedChars(String s) {
        List<Character> characterList = new ArrayList<>();
        for (char c : s.toCharArray()) {
            characterList.add(c);
        }
        Object[] arr = characterList.toArray();
        Arrays.sort(arr);
        return arr;
    }

}
