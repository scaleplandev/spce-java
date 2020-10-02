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
import io.scaleplan.spce.codecs.Avro;
import io.scaleplan.spce.codecs.Json;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class AvroCodecTest {
    private final static byte[] encodedEventWithNoData = new byte[]{
            8, 22, 115, 112, 101, 99, 118, 101, 114, 115, 105, 111, 110, 6, 6, 49, 46, 48, 12, 115, 111, 117, 114, 99, 101, 6, 20, 47, 117, 115, 101, 114, 47, 49, 50, 51, 35, 4, 105, 100, 6, 6, 53, 54, 55, 8, 116, 121, 112, 101, 6, 32, 79, 120, 105, 109, 101, 116, 101, 114, 77, 101, 97, 115, 117, 114, 101, 100, 0, 2
    };

    private final static byte[] encodedEventWithStringData = new byte[]{
            16, 30, 100, 97, 116, 97, 99, 111, 110, 116, 101, 110, 116, 116, 121, 112, 101, 6, 32, 97, 112, 112, 108, 105, 99, 97, 116, 105, 111, 110, 47, 106, 115, 111, 110, 14, 115, 117, 98, 106, 101, 99, 116, 6, 26, 83, 97, 109, 112, 108, 101, 83, 117, 98, 106, 101, 99, 116, 22, 115, 112, 101, 99, 118, 101, 114, 115, 105, 111, 110, 6, 6, 49, 46, 48, 12, 115, 111, 117, 114, 99, 101, 6, 20, 47, 117, 115, 101, 114, 47, 49, 50, 51, 35, 4, 105, 100, 6, 6, 53, 54, 55, 8, 116, 105, 109, 101, 6, 40, 50, 48, 50, 48, 45, 48, 55, 45, 49, 51, 84, 48, 57, 58, 49, 53, 58, 49, 50, 90, 8, 116, 121, 112, 101, 6, 32, 79, 120, 105, 109, 101, 116, 101, 114, 77, 101, 97, 115, 117, 114, 101, 100, 20, 100, 97, 116, 97, 115, 99, 104, 101, 109, 97, 6, 78, 104, 116, 116, 112, 58, 47, 47, 106, 115, 111, 110, 45, 115, 99, 104, 101, 109, 97, 46, 111, 114, 103, 47, 100, 114, 97, 102, 116, 45, 48, 55, 47, 115, 99, 104, 101, 109, 97, 35, 0, 12, -72, 1, 123, 34, 117, 115, 101, 114, 95, 105, 100, 34, 58, 32, 34, 98, 99, 49, 52, 53, 57, 99, 53, 45, 51, 55, 56, 100, 45, 52, 56, 51, 53, 45, 98, 52, 98, 54, 45, 97, 50, 99, 55, 100, 57, 99, 97, 55, 53, 101, 51, 34, 44, 32, 34, 115, 112, 111, 50, 34, 58, 32, 57, 54, 44, 32, 34, 101, 118, 101, 110, 116, 34, 58, 32, 34, 79, 120, 105, 109, 105, 116, 101, 114, 77, 101, 97, 115, 117, 114, 101, 100, 34, 125
    };

    private final static byte[] encodedEventWithBinaryData = new byte[]{
            16, 30, 100, 97, 116, 97, 99, 111, 110, 116, 101, 110, 116, 116, 121, 112, 101, 6, 48, 97, 112, 112, 108, 105, 99, 97, 116, 105, 111, 110, 47, 111, 99, 116, 101, 116, 45, 115, 116, 114, 101, 97, 109, 14, 115, 117, 98, 106, 101, 99, 116, 6, 26, 83, 97, 109, 112, 108, 101, 83, 117, 98, 106, 101, 99, 116, 22, 115, 112, 101, 99, 118, 101, 114, 115, 105, 111, 110, 6, 6, 49, 46, 48, 12, 115, 111, 117, 114, 99, 101, 6, 20, 47, 117, 115, 101, 114, 47, 49, 50, 51, 35, 4, 105, 100, 6, 6, 53, 54, 55, 8, 116, 105, 109, 101, 6, 40, 50, 48, 50, 48, 45, 48, 55, 45, 49, 51, 84, 48, 57, 58, 49, 53, 58, 49, 50, 90, 8, 116, 121, 112, 101, 6, 32, 79, 120, 105, 109, 101, 116, 101, 114, 77, 101, 97, 115, 117, 114, 101, 100, 20, 100, 97, 116, 97, 115, 99, 104, 101, 109, 97, 6, 78, 104, 116, 116, 112, 58, 47, 47, 106, 115, 111, 110, 45, 115, 99, 104, 101, 109, 97, 46, 111, 114, 103, 47, 100, 114, 97, 102, 116, 45, 48, 55, 47, 115, 99, 104, 101, 109, 97, 35, 0, 0, 8, 1, 2, 3, 4
    };

    @Test
    public void testEncodeWithNoData() {
        byte[] encodedEvent = Avro.encode(Common.eventWithNoData);
        assertArrayEquals(encodedEventWithNoData, encodedEvent);
    }

    @Test
    public void testEncodeWithStringData() {
        byte[] encodedEvent = Avro.encode(Common.eventWithStringData);
        assertArrayEquals(encodedEventWithStringData, encodedEvent);
    }

    @Test
    public void testEncodeWithBinaryData() {
        byte[] encodedEvent = Avro.encode(Common.eventWithBinaryData);
        assertArrayEquals(encodedEventWithBinaryData, encodedEvent);
    }

    @Test
    public void testDecodeWithNoData() {
        CloudEvent event = Avro.decode(encodedEventWithNoData);
        assertEquals(Common.eventWithNoData, event);
    }

    @Test
    public void testDecodeWithStringData() {
        CloudEvent event = Avro.decode(encodedEventWithStringData);
        assertEquals(Common.eventWithStringData, event);
    }

    @Test
    public void testDecodeWithBinaryData() {
        CloudEvent event = Avro.decode(encodedEventWithBinaryData);
        assertEquals(Common.eventWithBinaryData, event);
    }

    @Test
    public void testEncodeDecode1() {
        String text = "{\n" +
                "  \"specversion\" : \"1.0\",\n" +
                "  \"type\" : \"com.particlemetrics.OximeterMeasured.v1\",\n" +
                "  \"source\" : \"/oid/A129F28C#\",\n" +
                "  \"id\" : \"1\",\n" +
                "  \"time\" : \"2020-07-13T09:15:12Z\",\n" +
                "  \"subject\": \"Oximeter123\",\n" +
                "  \"datacontenttype\" : \"application/json\",\n" +
                "  \"dataschema\": \"https://particlemetrics.com/schemas/event-v1.json\",\n" +
                "  \"data\" : \"{\\\"user_id\\\": \\\"bc1459c5-378d-4835-b4b6-a2c7d9ca75e3\\\", \\\"spo2\\\": 96, \\\"event\\\": \\\"OximiterMeasured\\\"}\"\n" +
                "}\n";
        byte[] jsonEncodedEvent = text.getBytes();
        CloudEvent event = Json.decode(jsonEncodedEvent);
        assertEquals(event, Avro.decode(Avro.encode(event)));
    }

    @Test
    public void testEncodeDecode2() {
        String text = "{\"user_id\": \"bc1459c5-378d-4835-b4b6-a2c7d9ca75e3\", \"spo2\": 96, \"event\": \"OximiterMeasured\"}";
        CloudEvent event = CloudEvent.builder()
                .setType("OximeterMeasured")
                .setSource("/user/123#")
                .setId("567")
                .setTime("2020-07-13T09:15:12Z")
                .setDataContentType("application/octet-stream")
                .setDataUnsafe(text.getBytes(StandardCharsets.UTF_8))
                .setDataSchema("http://json-schema.org/draft-07/schema#")
                .setSubject("SampleSubject")
                .build();
        assertEquals(event, Avro.decode(Avro.encode(event)));
    }
}
