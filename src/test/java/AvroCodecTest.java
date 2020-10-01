import io.scaleplan.spce.CloudEvent;
import io.scaleplan.spce.codecs.Avro;
import io.scaleplan.spce.codecs.Json;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class AvroCodecTest {
    @Test
    public void testEncodeWithNoData() {
        CloudEvent event = TestUtils.sampleEventWithRequiredAttributes();
        byte[] encodedEvent = Avro.encode(event);
        byte[] target = new byte[]{
                8, 22, 115, 112, 101, 99, 118, 101, 114, 115, 105, 111, 110, 6, 6, 49, 46, 48, 12, 115, 111, 117, 114, 99, 101, 6, 20, 47, 117, 115, 101, 114, 47, 49, 50, 51, 35, 4, 105, 100, 6, 6, 53, 54, 55, 8, 116, 121, 112, 101, 6, 32, 79, 120, 105, 109, 101, 116, 101, 114, 77, 101, 97, 115, 117, 114, 101, 100, 0, 2
        };
        assertArrayEquals(target, encodedEvent);
    }

    @Test
    public void testEncodeWithStringData() {
        CloudEvent event = TestUtils.sampleEventWithOptionalAttributes();
        byte[] encodedEvent = Avro.encode(event);
        byte[] target = new byte[]{
                16, 30, 100, 97, 116, 97, 99, 111, 110, 116, 101, 110, 116, 116, 121, 112, 101, 6, 32, 97, 112, 112, 108, 105, 99, 97, 116, 105, 111, 110, 47, 106, 115, 111, 110, 14, 115, 117, 98, 106, 101, 99, 116, 6, 26, 83, 97, 109, 112, 108, 101, 83, 117, 98, 106, 101, 99, 116, 22, 115, 112, 101, 99, 118, 101, 114, 115, 105, 111, 110, 6, 6, 49, 46, 48, 12, 115, 111, 117, 114, 99, 101, 6, 20, 47, 117, 115, 101, 114, 47, 49, 50, 51, 35, 4, 105, 100, 6, 6, 53, 54, 55, 8, 116, 105, 109, 101, 6, 40, 50, 48, 50, 48, 45, 48, 55, 45, 49, 51, 84, 48, 57, 58, 49, 53, 58, 49, 50, 90, 8, 116, 121, 112, 101, 6, 32, 79, 120, 105, 109, 101, 116, 101, 114, 77, 101, 97, 115, 117, 114, 101, 100, 20, 100, 97, 116, 97, 115, 99, 104, 101, 109, 97, 6, 78, 104, 116, 116, 112, 58, 47, 47, 106, 115, 111, 110, 45, 115, 99, 104, 101, 109, 97, 46, 111, 114, 103, 47, 100, 114, 97, 102, 116, 45, 48, 55, 47, 115, 99, 104, 101, 109, 97, 35, 0, 0, -72, 1, 123, 34, 117, 115, 101, 114, 95, 105, 100, 34, 58, 32, 34, 98, 99, 49, 52, 53, 57, 99, 53, 45, 51, 55, 56, 100, 45, 52, 56, 51, 53, 45, 98, 52, 98, 54, 45, 97, 50, 99, 55, 100, 57, 99, 97, 55, 53, 101, 51, 34, 44, 32, 34, 115, 112, 111, 50, 34, 58, 32, 57, 54, 44, 32, 34, 101, 118, 101, 110, 116, 34, 58, 32, 34, 79, 120, 105, 109, 105, 116, 101, 114, 77, 101, 97, 115, 117, 114, 101, 100, 34, 125
        };
        assertArrayEquals(target, encodedEvent);
    }

    @Test
    public void testEncodeWithBinaryData() {
        CloudEvent event = TestUtils.sampleEventWithOptionalAttributesAndBinaryData();
        byte[] encodedEvent = Avro.encode(event);
        byte[] target = new byte[]{
                16, 30, 100, 97, 116, 97, 99, 111, 110, 116, 101, 110, 116, 116, 121, 112, 101, 6, 48, 97, 112, 112, 108, 105, 99, 97, 116, 105, 111, 110, 47, 111, 99, 116, 101, 116, 45, 115, 116, 114, 101, 97, 109, 14, 115, 117, 98, 106, 101, 99, 116, 6, 26, 83, 97, 109, 112, 108, 101, 83, 117, 98, 106, 101, 99, 116, 22, 115, 112, 101, 99, 118, 101, 114, 115, 105, 111, 110, 6, 6, 49, 46, 48, 12, 115, 111, 117, 114, 99, 101, 6, 20, 47, 117, 115, 101, 114, 47, 49, 50, 51, 35, 4, 105, 100, 6, 6, 53, 54, 55, 8, 116, 105, 109, 101, 6, 40, 50, 48, 50, 48, 45, 48, 55, 45, 49, 51, 84, 48, 57, 58, 49, 53, 58, 49, 50, 90, 8, 116, 121, 112, 101, 6, 32, 79, 120, 105, 109, 101, 116, 101, 114, 77, 101, 97, 115, 117, 114, 101, 100, 20, 100, 97, 116, 97, 115, 99, 104, 101, 109, 97, 6, 78, 104, 116, 116, 112, 58, 47, 47, 106, 115, 111, 110, 45, 115, 99, 104, 101, 109, 97, 46, 111, 114, 103, 47, 100, 114, 97, 102, 116, 45, 48, 55, 47, 115, 99, 104, 101, 109, 97, 35, 0, 0, 8, 1, 2, 3, 4
        };
        assertArrayEquals(target, encodedEvent);
    }

    @Test
    public void testDecodeWithNoData() {
        byte[] encodedEvent = new byte[]{
                8, 22, 115, 112, 101, 99, 118, 101, 114, 115, 105, 111, 110, 6, 6, 49, 46, 48, 12, 115, 111, 117, 114, 99, 101, 6, 20, 47, 117, 115, 101, 114, 47, 49, 50, 51, 35, 4, 105, 100, 6, 6, 53, 54, 55, 8, 116, 121, 112, 101, 6, 32, 79, 120, 105, 109, 101, 116, 101, 114, 77, 101, 97, 115, 117, 114, 101, 100, 0, 2
        };
        CloudEvent event = Avro.decode(encodedEvent);
        CloudEvent target = TestUtils.sampleEventWithRequiredAttributes();
        assertEquals(target, event);
    }

    @Test
    public void testDecodeWithStringData() {
        byte[] encoded_event = new byte[]{
                16, 30, 100, 97, 116, 97, 99, 111, 110, 116, 101, 110, 116, 116, 121, 112, 101, 6, 32, 97, 112, 112, 108, 105, 99, 97, 116, 105, 111, 110, 47, 106, 115, 111, 110, 14, 115, 117, 98, 106, 101, 99, 116, 6, 26, 83, 97, 109, 112, 108, 101, 83, 117, 98, 106, 101, 99, 116, 22, 115, 112, 101, 99, 118, 101, 114, 115, 105, 111, 110, 6, 6, 49, 46, 48, 12, 115, 111, 117, 114, 99, 101, 6, 20, 47, 117, 115, 101, 114, 47, 49, 50, 51, 35, 4, 105, 100, 6, 6, 53, 54, 55, 8, 116, 105, 109, 101, 6, 40, 50, 48, 50, 48, 45, 48, 55, 45, 49, 51, 84, 48, 57, 58, 49, 53, 58, 49, 50, 90, 8, 116, 121, 112, 101, 6, 32, 79, 120, 105, 109, 101, 116, 101, 114, 77, 101, 97, 115, 117, 114, 101, 100, 20, 100, 97, 116, 97, 115, 99, 104, 101, 109, 97, 6, 78, 104, 116, 116, 112, 58, 47, 47, 106, 115, 111, 110, 45, 115, 99, 104, 101, 109, 97, 46, 111, 114, 103, 47, 100, 114, 97, 102, 116, 45, 48, 55, 47, 115, 99, 104, 101, 109, 97, 35, 0, 0, -72, 1, 123, 34, 117, 115, 101, 114, 95, 105, 100, 34, 58, 32, 34, 98, 99, 49, 52, 53, 57, 99, 53, 45, 51, 55, 56, 100, 45, 52, 56, 51, 53, 45, 98, 52, 98, 54, 45, 97, 50, 99, 55, 100, 57, 99, 97, 55, 53, 101, 51, 34, 44, 32, 34, 115, 112, 111, 50, 34, 58, 32, 57, 54, 44, 32, 34, 101, 118, 101, 110, 116, 34, 58, 32, 34, 79, 120, 105, 109, 105, 116, 101, 114, 77, 101, 97, 115, 117, 114, 101, 100, 34, 125
        };
        CloudEvent event = Avro.decode(encoded_event);
        CloudEvent target = TestUtils.sampleEventWithOptionalAttributes();
        assertEquals(target, event);
    }

    @Test
    public void testDecodeWithBinaryData() {
        byte[] encoded_event = new byte[]{
                16, 30, 100, 97, 116, 97, 99, 111, 110, 116, 101, 110, 116, 116, 121, 112, 101, 6, 48, 97, 112, 112, 108, 105, 99, 97, 116, 105, 111, 110, 47, 111, 99, 116, 101, 116, 45, 115, 116, 114, 101, 97, 109, 14, 115, 117, 98, 106, 101, 99, 116, 6, 26, 83, 97, 109, 112, 108, 101, 83, 117, 98, 106, 101, 99, 116, 22, 115, 112, 101, 99, 118, 101, 114, 115, 105, 111, 110, 6, 6, 49, 46, 48, 12, 115, 111, 117, 114, 99, 101, 6, 20, 47, 117, 115, 101, 114, 47, 49, 50, 51, 35, 4, 105, 100, 6, 6, 53, 54, 55, 8, 116, 105, 109, 101, 6, 40, 50, 48, 50, 48, 45, 48, 55, 45, 49, 51, 84, 48, 57, 58, 49, 53, 58, 49, 50, 90, 8, 116, 121, 112, 101, 6, 32, 79, 120, 105, 109, 101, 116, 101, 114, 77, 101, 97, 115, 117, 114, 101, 100, 20, 100, 97, 116, 97, 115, 99, 104, 101, 109, 97, 6, 78, 104, 116, 116, 112, 58, 47, 47, 106, 115, 111, 110, 45, 115, 99, 104, 101, 109, 97, 46, 111, 114, 103, 47, 100, 114, 97, 102, 116, 45, 48, 55, 47, 115, 99, 104, 101, 109, 97, 35, 0, 0, 8, 1, 2, 3, 4
        };
        CloudEvent event = Avro.decode(encoded_event);
        CloudEvent target = TestUtils.sampleEventWithOptionalAttributesAndBinaryData();
        assertEquals(target, event);
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
                .setDataContentType("application/json")
                .setDataUnsafe(text.getBytes(StandardCharsets.UTF_8))
                .setDataSchema("http://json-schema.org/draft-07/schema#")
                .setSubject("SampleSubject")
                .build();
        Avro.encode(event);
    }

    private static List<Byte> byteArrayToList(final byte[] arr) {
        List<Byte> result = new ArrayList<>(arr.length);
        for (byte b : arr) {
            result.add(b);
        }
        return result;
    }
}
