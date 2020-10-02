import io.scaleplan.spce.CloudEvent;
import io.scaleplan.spce.codecs.AvroSpce;
import io.scaleplan.spce.codecs.Json;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class AvroSpceCodecTest {
    private final static byte[] encodedEventWithNoData = new byte[]{
            6, 49, 46, 48, 32, 79, 120, 105, 109, 101, 116, 101, 114, 77, 101, 97, 115, 117, 114, 101, 100, 20, 47, 117, 115, 101, 114, 47, 49, 50, 51, 35, 6, 53, 54, 55, 2, 2, 2, 2, 4, 0
    };
    private final static byte[] encodedEventWithStringData = new byte[]{
            6, 49, 46, 48, 32, 79, 120, 105, 109, 101, 116, 101, 114, 77, 101, 97, 115, 117, 114, 101, 100, 20, 47, 117, 115, 101, 114, 47, 49, 50, 51, 35, 6, 53, 54, 55, 0, 26, 83, 97, 109, 112, 108, 101, 83, 117, 98, 106, 101, 99, 116, 0, 40, 50, 48, 50, 48, 45, 48, 55, 45, 49, 51, 84, 48, 57, 58, 49, 53, 58, 49, 50, 90, 0, 32, 97, 112, 112, 108, 105, 99, 97, 116, 105, 111, 110, 47, 106, 115, 111, 110, 0, 78, 104, 116, 116, 112, 58, 47, 47, 106, 115, 111, 110, 45, 115, 99, 104, 101, 109, 97, 46, 111, 114, 103, 47, 100, 114, 97, 102, 116, 45, 48, 55, 47, 115, 99, 104, 101, 109, 97, 35, 2, -72, 1, 123, 34, 117, 115, 101, 114, 95, 105, 100, 34, 58, 32, 34, 98, 99, 49, 52, 53, 57, 99, 53, 45, 51, 55, 56, 100, 45, 52, 56, 51, 53, 45, 98, 52, 98, 54, 45, 97, 50, 99, 55, 100, 57, 99, 97, 55, 53, 101, 51, 34, 44, 32, 34, 115, 112, 111, 50, 34, 58, 32, 57, 54, 44, 32, 34, 101, 118, 101, 110, 116, 34, 58, 32, 34, 79, 120, 105, 109, 105, 116, 101, 114, 77, 101, 97, 115, 117, 114, 101, 100, 34, 125, 0
    };
    private final static byte[] encodedEventWithBinaryData = new byte[]{
            6, 49, 46, 48, 32, 79, 120, 105, 109, 101, 116, 101, 114, 77, 101, 97, 115, 117, 114, 101, 100, 20, 47, 117, 115, 101, 114, 47, 49, 50, 51, 35, 6, 53, 54, 55, 0, 26, 83, 97, 109, 112, 108, 101, 83, 117, 98, 106, 101, 99, 116, 0, 40, 50, 48, 50, 48, 45, 48, 55, 45, 49, 51, 84, 48, 57, 58, 49, 53, 58, 49, 50, 90, 0, 48, 97, 112, 112, 108, 105, 99, 97, 116, 105, 111, 110, 47, 111, 99, 116, 101, 116, 45, 115, 116, 114, 101, 97, 109, 0, 78, 104, 116, 116, 112, 58, 47, 47, 106, 115, 111, 110, 45, 115, 99, 104, 101, 109, 97, 46, 111, 114, 103, 47, 100, 114, 97, 102, 116, 45, 48, 55, 47, 115, 99, 104, 101, 109, 97, 35, 0, 8, 1, 2, 3, 4, 0
    };
    private final static byte[] encodedEventBatchSize0 = new byte[0];
    private final static byte[] encodedEventBatchSize1 = new byte[]{
            6, 49, 46, 48, 32, 79, 120, 105, 109, 101, 116, 101, 114, 77, 101, 97, 115, 117, 114, 101, 100, 20, 47, 117, 115, 101, 114, 47, 49, 50, 51, 35, 6, 53, 54, 55, 0, 26, 83, 97, 109, 112, 108, 101, 83, 117, 98, 106, 101, 99, 116, 0, 40, 50, 48, 50, 48, 45, 48, 55, 45, 49, 51, 84, 48, 57, 58, 49, 53, 58, 49, 50, 90, 0, 32, 97, 112, 112, 108, 105, 99, 97, 116, 105, 111, 110, 47, 106, 115, 111, 110, 0, 78, 104, 116, 116, 112, 58, 47, 47, 106, 115, 111, 110, 45, 115, 99, 104, 101, 109, 97, 46, 111, 114, 103, 47, 100, 114, 97, 102, 116, 45, 48, 55, 47, 115, 99, 104, 101, 109, 97, 35, 2, -72, 1, 123, 34, 117, 115, 101, 114, 95, 105, 100, 34, 58, 32, 34, 98, 99, 49, 52, 53, 57, 99, 53, 45, 51, 55, 56, 100, 45, 52, 56, 51, 53, 45, 98, 52, 98, 54, 45, 97, 50, 99, 55, 100, 57, 99, 97, 55, 53, 101, 51, 34, 44, 32, 34, 115, 112, 111, 50, 34, 58, 32, 57, 54, 44, 32, 34, 101, 118, 101, 110, 116, 34, 58, 32, 34, 79, 120, 105, 109, 105, 116, 101, 114, 77, 101, 97, 115, 117, 114, 101, 100, 34, 125, 0
    };
    private final static byte[] encodedEventBatchSize2 = new byte[]{
            6, 49, 46, 48, 32, 79, 120, 105, 109, 101, 116, 101, 114, 77, 101, 97, 115, 117, 114, 101, 100, 20, 47, 117, 115, 101, 114, 47, 49, 50, 51, 35, 6, 53, 54, 55, 0, 26, 83, 97, 109, 112, 108, 101, 83, 117, 98, 106, 101, 99, 116, 0, 40, 50, 48, 50, 48, 45, 48, 55, 45, 49, 51, 84, 48, 57, 58, 49, 53, 58, 49, 50, 90, 0, 32, 97, 112, 112, 108, 105, 99, 97, 116, 105, 111, 110, 47, 106, 115, 111, 110, 0, 78, 104, 116, 116, 112, 58, 47, 47, 106, 115, 111, 110, 45, 115, 99, 104, 101, 109, 97, 46, 111, 114, 103, 47, 100, 114, 97, 102, 116, 45, 48, 55, 47, 115, 99, 104, 101, 109, 97, 35, 2, -72, 1, 123, 34, 117, 115, 101, 114, 95, 105, 100, 34, 58, 32, 34, 98, 99, 49, 52, 53, 57, 99, 53, 45, 51, 55, 56, 100, 45, 52, 56, 51, 53, 45, 98, 52, 98, 54, 45, 97, 50, 99, 55, 100, 57, 99, 97, 55, 53, 101, 51, 34, 44, 32, 34, 115, 112, 111, 50, 34, 58, 32, 57, 54, 44, 32, 34, 101, 118, 101, 110, 116, 34, 58, 32, 34, 79, 120, 105, 109, 105, 116, 101, 114, 77, 101, 97, 115, 117, 114, 101, 100, 34, 125, 0, 6, 49, 46, 48, 32, 79, 120, 105, 109, 101, 116, 101, 114, 77, 101, 97, 115, 117, 114, 101, 100, 20, 47, 117, 115, 101, 114, 47, 49, 50, 51, 35, 6, 53, 54, 55, 0, 26, 83, 97, 109, 112, 108, 101, 83, 117, 98, 106, 101, 99, 116, 0, 40, 50, 48, 50, 48, 45, 48, 55, 45, 49, 51, 84, 48, 57, 58, 49, 53, 58, 49, 50, 90, 0, 48, 97, 112, 112, 108, 105, 99, 97, 116, 105, 111, 110, 47, 111, 99, 116, 101, 116, 45, 115, 116, 114, 101, 97, 109, 0, 78, 104, 116, 116, 112, 58, 47, 47, 106, 115, 111, 110, 45, 115, 99, 104, 101, 109, 97, 46, 111, 114, 103, 47, 100, 114, 97, 102, 116, 45, 48, 55, 47, 115, 99, 104, 101, 109, 97, 35, 0, 8, 1, 2, 3, 4, 0
    };

    @Test
    public void testAvroSpceEncodeWithNoData() {
        byte[] encodedEvent = AvroSpce.encode(Common.eventWithNoData);
        assertArrayEquals(encodedEventWithNoData, encodedEvent);
    }

    @Test
    public void testAvroSpceEncodeWithStringData() {
        byte[] encodedEvent = AvroSpce.encode(Common.eventWithStringData);
        assertArrayEquals(encodedEventWithStringData, encodedEvent);
    }

    @Test
    public void testAvroSpceEncodeWithBinaryData() {
        byte[] encodedEvent = AvroSpce.encode(Common.eventWithBinaryData);
        assertArrayEquals(encodedEventWithBinaryData, encodedEvent);
    }

    @Test
    public void testAvroSpceDecodeWithNoData() {
        CloudEvent event = AvroSpce.decode(encodedEventWithNoData);
        assertEquals(Common.eventWithNoData, event);
    }

    @Test
    public void testAvroSpceDecodeWithStringData() {
        CloudEvent event = AvroSpce.decode(encodedEventWithStringData);
        assertEquals(Common.eventWithStringData, event);
    }

    @Test
    public void testAvroSpceDecodeWithBinaryData() {
        CloudEvent event = AvroSpce.decode(encodedEventWithBinaryData);
        assertEquals(Common.eventWithBinaryData, event);
    }

    @Test
    public void testEncodeAvroSpceBatchNoItems() {
        byte[] encodedBatch = AvroSpce.encode(Common.eventBatchSize0);
        assertArrayEquals(encodedEventBatchSize0, encodedBatch);
    }

    @Test
    public void testEncodeAvroSpceBatch1Item() {
        byte[] encodedBatch = AvroSpce.encode(Common.eventBatchSize1);
        assertArrayEquals(encodedEventBatchSize1, encodedBatch);
    }

    @Test
    public void testEncodeAvroSpceBatch2Items() {
        byte[] encodedBatch = AvroSpce.encode(Common.eventBatchSize2);
        assertArrayEquals(encodedEventBatchSize2, encodedBatch);
    }

    @Test
    public void testDecodeAvroSpceBatchNoItems() {
        List<CloudEvent> decodedEvents = AvroSpce.decodeBatch(encodedEventBatchSize0);
        assertEquals(Common.eventBatchSize0, decodedEvents);
    }

    @Test
    public void testDecodeAvroSpceBatch1Item() {
        List<CloudEvent> decodedEvents = AvroSpce.decodeBatch(encodedEventBatchSize1);
        assertEquals(Common.eventBatchSize1, decodedEvents);
    }

    @Test
    public void testDecodeAvroSpceBatch2Items() {
        List<CloudEvent> decodedEvents = AvroSpce.decodeBatch(encodedEventBatchSize2);
        assertEquals(Common.eventBatchSize2, decodedEvents);
    }

    @Test
    public void testEncodeDecodeAvroSpce1() {
        String text = "{\n" +
                "  \"specversion\" : \"1.0\",\n" +
                "  \"type\" : \"com.particlemetrics.OximeterMeasured.v1\",\n" +
                "  \"source\" : \"/oid/A129F28C#\",\n" +
                "  \"id\" : \"1\",\n" +
                "  \"time\" : \"2020-07-13T09:15:12Z\",\n" +
                "  \"subject\": \"Oximeter123\",\n" +
                "  \"datacontenttype\" : \"application/json\",\n" +
                "  \"dataschema\": \"https://particlemetrics.com/schemas/event-v1.json\",\n" +
                "  \"data\" : \"{\\\"user_id\\\": \\\"bc1459c5-378d-4835-b4b6-a2c7d9ca75e3\\\", \\\"spo2\\\": 96, \\\"event\\\": \\\"OximiterMeasured\\\"}\",\n" +
                "  \"external1\": \"foobar\"\n" +
                "}\n";
        byte[] jsonEncodedEvent = text.getBytes();
        CloudEvent event = Json.decode(jsonEncodedEvent);
        assertEquals(event, AvroSpce.decode(AvroSpce.encode(event)));
    }

    @Test
    public void testEncodeDecodeAvroSpce2() {
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
        assertEquals(event, AvroSpce.decode(AvroSpce.encode(event)));
    }
}
