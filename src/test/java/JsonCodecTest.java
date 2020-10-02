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
import io.scaleplan.spce.ValidationException;
import io.scaleplan.spce.codecs.DecodeException;
import io.scaleplan.spce.codecs.EncodeException;
import io.scaleplan.spce.codecs.Json;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

public class JsonCodecTest {
    @Test
    public void testJsonDecodeWithStringData() {
        byte[] text = Common.loadFromResource("fixtures/sample_event_extended.json");
        CloudEvent event = Json.decode(text);
        Common.checkEvent(event,
                Common.sampleStringData.getBytes(),
                "id", "1",
                "specversion", "1.0",
                "source", "/oid/A129F28C#",
                "type", "com.particlemetrics.OximeterMeasured.v1",
                "time", "2020-07-13T09:15:12Z",
                "datacontenttype", "application/json",
                "dataschema", "https://particlemetrics.com/schemas/event-v1.json",
                "subject", "Oximeter123",
                "compmstring", "string-value",
                "compmint", 42,
                "compmbool", true
        );
    }

    @Test
    public void testJsonDecodeWithBinaryData() {
        byte[] text = Common.loadFromResource("fixtures/sample_event_extended_binary_data.json");
        CloudEvent event = Json.decode(text);
        Common.checkEvent(event,
                Common.sampleBinaryData,
                "id", "1",
                "specversion", "1.0",
                "source", "/oid/A129F28C#",
                "type", "com.particlemetrics.OximeterMeasured.v1",
                "time", "2020-07-13T09:15:12Z",
                "datacontenttype", "application/octet-stream",
                "subject", "Oximeter123",
                "compmstring", "string-value",
                "compmint", 42,
                "compmbool", true
        );
    }

    @Test
    public void testJsonEncode() {
        CloudEvent event = Common.sampleEventWithOptionalAndExtendedAttributes();
        byte[] encodedEvent = Json.encode(event);
        String encodedString = new String(encodedEvent);
        String targetString = "{\"datacontenttype\":\"application/json\",\"compmstring\":\"string-value\",\"compmint\":42,\"compmbool\":true,\"subject\":\"SampleSubject\",\"specversion\":\"1.0\",\"source\":\"/user/123#\",\"id\":\"567\",\"time\":\"2020-07-13T09:15:12Z\",\"type\":\"OximeterMeasured\",\"dataschema\":\"http://json-schema.org/draft-07/schema#\",\"data\":\"{\\\"user_id\\\": \\\"bc1459c5-378d-4835-b4b6-a2c7d9ca75e3\\\", \\\"spo2\\\": 96, \\\"event\\\": \\\"OximiterMeasured\\\"}\"}";
        assertArrayEquals(Common.stringToSortedChars(targetString), Common.stringToSortedChars(encodedString));
    }

    @Test
    public void testJsonArrayDecode() {
        byte[] text = Common.loadFromResource("fixtures/sample_event_array.json");
        List<CloudEvent> events = Json.decodeBatch(text);

        assertEquals(2, events.size());
        // TODO: check all attributes the events
        assertEquals("1", events.get(0).getId());
        assertEquals("2020-07-13T09:15:12Z", events.get(0).getTime());
        assertEquals("2", events.get(1).getId());
        assertEquals("2020-07-13T09:16:12Z", events.get(1).getTime());
    }

    @Test
    public void testEmptyJsonArrayDecode() {
        List<CloudEvent> events = Json.decodeBatch("[]".getBytes());
        assertEquals(0, events.size());
    }

    @Test
    public void testInvalidJsonArrayDecode1() {
        assertThrows(DecodeException.class, () -> Json.decodeBatch("".getBytes()));
    }

    @Test
    public void testInvalidJsonArrayDecode2() {
        assertThrows(DecodeException.class, () -> Json.decodeBatch("{\"foo\":\"bar\"}".getBytes()));
    }

    @Test
    public void testInvalidJsonArrayDecode3() {
        assertThrows(ValidationException.class, () -> Json.decodeBatch("[{}]".getBytes()));
    }

    @Test
    public void testJsonArrayEncode() {
        MutableCloudEvent event1 = (MutableCloudEvent) Common.sampleEventWithOptionalAttributes();
        event1.setId("10");
        MutableCloudEvent event2 = (MutableCloudEvent) Common.sampleEventWithOptionalAttributes();
        event2.setId("20");

        byte[] encodedEvents = Json.encode(Arrays.asList(event1, event2));
        String target = "[{\"datacontenttype\":\"application/json\",\"subject\":\"SampleSubject\",\"specversion\":\"1.0\",\"source\":\"/user/123#\",\"id\":\"10\",\"time\":\"2020-07-13T09:15:12Z\",\"type\":\"OximeterMeasured\",\"dataschema\":\"http://json-schema.org/draft-07/schema#\",\"data\":\"{\\\"user_id\\\": \\\"bc1459c5-378d-4835-b4b6-a2c7d9ca75e3\\\", \\\"spo2\\\": 96, \\\"event\\\": \\\"OximiterMeasured\\\"}\"},{\"datacontenttype\":\"application/json\",\"subject\":\"SampleSubject\",\"specversion\":\"1.0\",\"source\":\"/user/123#\",\"id\":\"20\",\"time\":\"2020-07-13T09:15:12Z\",\"type\":\"OximeterMeasured\",\"dataschema\":\"http://json-schema.org/draft-07/schema#\",\"data\":\"{\\\"user_id\\\": \\\"bc1459c5-378d-4835-b4b6-a2c7d9ca75e3\\\", \\\"spo2\\\": 96, \\\"event\\\": \\\"OximiterMeasured\\\"}\"}]";
        assertArrayEquals(Common.stringToSortedChars(target), Common.stringToSortedChars(new String(encodedEvents)));
    }

    @Test
    public void testEmptyJsonArrayEncode() {
        byte[] encodedEvents = Json.encode(new ArrayList<>());
        assertEquals("[]", new String(encodedEvents));
    }

    @Test
    public void testEncodeInvalidType() {
        MutableCloudEvent event = ((MutableCloudEvent) Common.sampleEventWithOptionalAttributes())
                .put("invalidattr", 5.2);
        assertThrows(EncodeException.class, () -> Json.encode(event));

    }
}
