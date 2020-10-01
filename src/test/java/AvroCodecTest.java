import io.scaleplan.spce.CloudEvent;
import io.scaleplan.spce.codecs.Avro;
import io.scaleplan.spce.codecs.AvroSpce;
import io.scaleplan.spce.codecs.Json;
import io.scaleplan.spce.codecs.avro.AvroDecoder;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
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
    public void testAvroSpceEncodeWithNoData() {
        CloudEvent event = TestUtils.sampleEventWithRequiredAttributes();
        byte[] encodedEvent = AvroSpce.encode(event);
        byte[] target = new byte[]{
                6, 49, 46, 48, 32, 79, 120, 105, 109, 101, 116, 101, 114, 77, 101, 97, 115, 117, 114, 101, 100, 20, 47, 117, 115, 101, 114, 47, 49, 50, 51, 35, 6, 53, 54, 55, 2, 2, 2, 2, 4, 0
        };
        assertArrayEquals(target, encodedEvent);
    }

    @Test
    public void testAvroSpceEncodeWithStringData() {
        CloudEvent event = TestUtils.sampleEventWithOptionalAttributes();
        byte[] encodedEvent = AvroSpce.encode(event);
        byte[] target = new byte[]{
                6, 49, 46, 48, 32, 79, 120, 105, 109, 101, 116, 101, 114, 77, 101, 97, 115, 117, 114, 101, 100, 20, 47, 117, 115, 101, 114, 47, 49, 50, 51, 35, 6, 53, 54, 55, 0, 26, 83, 97, 109, 112, 108, 101, 83, 117, 98, 106, 101, 99, 116, 0, 40, 50, 48, 50, 48, 45, 48, 55, 45, 49, 51, 84, 48, 57, 58, 49, 53, 58, 49, 50, 90, 0, 32, 97, 112, 112, 108, 105, 99, 97, 116, 105, 111, 110, 47, 106, 115, 111, 110, 0, 78, 104, 116, 116, 112, 58, 47, 47, 106, 115, 111, 110, 45, 115, 99, 104, 101, 109, 97, 46, 111, 114, 103, 47, 100, 114, 97, 102, 116, 45, 48, 55, 47, 115, 99, 104, 101, 109, 97, 35, 0, -72, 1, 123, 34, 117, 115, 101, 114, 95, 105, 100, 34, 58, 32, 34, 98, 99, 49, 52, 53, 57, 99, 53, 45, 51, 55, 56, 100, 45, 52, 56, 51, 53, 45, 98, 52, 98, 54, 45, 97, 50, 99, 55, 100, 57, 99, 97, 55, 53, 101, 51, 34, 44, 32, 34, 115, 112, 111, 50, 34, 58, 32, 57, 54, 44, 32, 34, 101, 118, 101, 110, 116, 34, 58, 32, 34, 79, 120, 105, 109, 105, 116, 101, 114, 77, 101, 97, 115, 117, 114, 101, 100, 34, 125, 0
        };
        assertArrayEquals(target, encodedEvent);
    }

    @Test
    public void testAvroSpceEncodeWithBinaryData() {
        CloudEvent event = TestUtils.sampleEventWithOptionalAttributesAndBinaryData();
        byte[] encodedEvent = AvroSpce.encode(event);
        byte[] target = new byte[]{
                6, 49, 46, 48, 32, 79, 120, 105, 109, 101, 116, 101, 114, 77, 101, 97, 115, 117, 114, 101, 100, 20, 47, 117, 115, 101, 114, 47, 49, 50, 51, 35, 6, 53, 54, 55, 0, 26, 83, 97, 109, 112, 108, 101, 83, 117, 98, 106, 101, 99, 116, 0, 40, 50, 48, 50, 48, 45, 48, 55, 45, 49, 51, 84, 48, 57, 58, 49, 53, 58, 49, 50, 90, 0, 48, 97, 112, 112, 108, 105, 99, 97, 116, 105, 111, 110, 47, 111, 99, 116, 101, 116, 45, 115, 116, 114, 101, 97, 109, 0, 78, 104, 116, 116, 112, 58, 47, 47, 106, 115, 111, 110, 45, 115, 99, 104, 101, 109, 97, 46, 111, 114, 103, 47, 100, 114, 97, 102, 116, 45, 48, 55, 47, 115, 99, 104, 101, 109, 97, 35, 0, 8, 1, 2, 3, 4, 0
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
        byte[] encodedEvent = new byte[]{
                16, 30, 100, 97, 116, 97, 99, 111, 110, 116, 101, 110, 116, 116, 121, 112, 101, 6, 32, 97, 112, 112, 108, 105, 99, 97, 116, 105, 111, 110, 47, 106, 115, 111, 110, 14, 115, 117, 98, 106, 101, 99, 116, 6, 26, 83, 97, 109, 112, 108, 101, 83, 117, 98, 106, 101, 99, 116, 22, 115, 112, 101, 99, 118, 101, 114, 115, 105, 111, 110, 6, 6, 49, 46, 48, 12, 115, 111, 117, 114, 99, 101, 6, 20, 47, 117, 115, 101, 114, 47, 49, 50, 51, 35, 4, 105, 100, 6, 6, 53, 54, 55, 8, 116, 105, 109, 101, 6, 40, 50, 48, 50, 48, 45, 48, 55, 45, 49, 51, 84, 48, 57, 58, 49, 53, 58, 49, 50, 90, 8, 116, 121, 112, 101, 6, 32, 79, 120, 105, 109, 101, 116, 101, 114, 77, 101, 97, 115, 117, 114, 101, 100, 20, 100, 97, 116, 97, 115, 99, 104, 101, 109, 97, 6, 78, 104, 116, 116, 112, 58, 47, 47, 106, 115, 111, 110, 45, 115, 99, 104, 101, 109, 97, 46, 111, 114, 103, 47, 100, 114, 97, 102, 116, 45, 48, 55, 47, 115, 99, 104, 101, 109, 97, 35, 0, 0, -72, 1, 123, 34, 117, 115, 101, 114, 95, 105, 100, 34, 58, 32, 34, 98, 99, 49, 52, 53, 57, 99, 53, 45, 51, 55, 56, 100, 45, 52, 56, 51, 53, 45, 98, 52, 98, 54, 45, 97, 50, 99, 55, 100, 57, 99, 97, 55, 53, 101, 51, 34, 44, 32, 34, 115, 112, 111, 50, 34, 58, 32, 57, 54, 44, 32, 34, 101, 118, 101, 110, 116, 34, 58, 32, 34, 79, 120, 105, 109, 105, 116, 101, 114, 77, 101, 97, 115, 117, 114, 101, 100, 34, 125
        };
        CloudEvent event = Avro.decode(encodedEvent);
        CloudEvent target = TestUtils.sampleEventWithOptionalAttributes();
        assertEquals(target, event);
    }

    @Test
    public void testDecodeWithBinaryData() {
        byte[] encodedEvent = new byte[]{
                16, 30, 100, 97, 116, 97, 99, 111, 110, 116, 101, 110, 116, 116, 121, 112, 101, 6, 48, 97, 112, 112, 108, 105, 99, 97, 116, 105, 111, 110, 47, 111, 99, 116, 101, 116, 45, 115, 116, 114, 101, 97, 109, 14, 115, 117, 98, 106, 101, 99, 116, 6, 26, 83, 97, 109, 112, 108, 101, 83, 117, 98, 106, 101, 99, 116, 22, 115, 112, 101, 99, 118, 101, 114, 115, 105, 111, 110, 6, 6, 49, 46, 48, 12, 115, 111, 117, 114, 99, 101, 6, 20, 47, 117, 115, 101, 114, 47, 49, 50, 51, 35, 4, 105, 100, 6, 6, 53, 54, 55, 8, 116, 105, 109, 101, 6, 40, 50, 48, 50, 48, 45, 48, 55, 45, 49, 51, 84, 48, 57, 58, 49, 53, 58, 49, 50, 90, 8, 116, 121, 112, 101, 6, 32, 79, 120, 105, 109, 101, 116, 101, 114, 77, 101, 97, 115, 117, 114, 101, 100, 20, 100, 97, 116, 97, 115, 99, 104, 101, 109, 97, 6, 78, 104, 116, 116, 112, 58, 47, 47, 106, 115, 111, 110, 45, 115, 99, 104, 101, 109, 97, 46, 111, 114, 103, 47, 100, 114, 97, 102, 116, 45, 48, 55, 47, 115, 99, 104, 101, 109, 97, 35, 0, 0, 8, 1, 2, 3, 4
        };
        CloudEvent event = Avro.decode(encodedEvent);
        CloudEvent target = TestUtils.sampleEventWithOptionalAttributesAndBinaryData();
        assertEquals(target, event);
    }

    @Test
    public void testAvroSpceDecodeWithNoData() {
        byte[] encodedEvent = new byte[]{
                6, 49, 46, 48, 32, 79, 120, 105, 109, 101, 116, 101, 114, 77, 101, 97, 115, 117, 114, 101, 100, 20, 47, 117, 115, 101, 114, 47, 49, 50, 51, 35, 6, 53, 54, 55, 2, 2, 2, 2, 4, 0
        };
        CloudEvent event = AvroSpce.decode(encodedEvent);
        CloudEvent target = TestUtils.sampleEventWithRequiredAttributes();
        assertEquals(target, event);
    }

    @Test
    public void testAvroSpceDecodeWithStringData() {
        byte[] encodedEvent = new byte[]{
                6, 49, 46, 48, 32, 79, 120, 105, 109, 101, 116, 101, 114, 77, 101, 97, 115, 117, 114, 101, 100, 20, 47, 117, 115, 101, 114, 47, 49, 50, 51, 35, 6, 53, 54, 55, 0, 26, 83, 97, 109, 112, 108, 101, 83, 117, 98, 106, 101, 99, 116, 0, 40, 50, 48, 50, 48, 45, 48, 55, 45, 49, 51, 84, 48, 57, 58, 49, 53, 58, 49, 50, 90, 0, 32, 97, 112, 112, 108, 105, 99, 97, 116, 105, 111, 110, 47, 106, 115, 111, 110, 0, 78, 104, 116, 116, 112, 58, 47, 47, 106, 115, 111, 110, 45, 115, 99, 104, 101, 109, 97, 46, 111, 114, 103, 47, 100, 114, 97, 102, 116, 45, 48, 55, 47, 115, 99, 104, 101, 109, 97, 35, 0, -72, 1, 123, 34, 117, 115, 101, 114, 95, 105, 100, 34, 58, 32, 34, 98, 99, 49, 52, 53, 57, 99, 53, 45, 51, 55, 56, 100, 45, 52, 56, 51, 53, 45, 98, 52, 98, 54, 45, 97, 50, 99, 55, 100, 57, 99, 97, 55, 53, 101, 51, 34, 44, 32, 34, 115, 112, 111, 50, 34, 58, 32, 57, 54, 44, 32, 34, 101, 118, 101, 110, 116, 34, 58, 32, 34, 79, 120, 105, 109, 105, 116, 101, 114, 77, 101, 97, 115, 117, 114, 101, 100, 34, 125, 0
        };
        CloudEvent event = AvroSpce.decode(encodedEvent);
        CloudEvent target = TestUtils.sampleEventWithOptionalAttributes();
        assertEquals(target, event);
    }

    @Test
    public void testAvroSpceDecodeWithBinaryData() {
        byte[] encodedEvent = new byte[]{
                6, 49, 46, 48, 32, 79, 120, 105, 109, 101, 116, 101, 114, 77, 101, 97, 115, 117, 114, 101, 100, 20, 47, 117, 115, 101, 114, 47, 49, 50, 51, 35, 6, 53, 54, 55, 0, 26, 83, 97, 109, 112, 108, 101, 83, 117, 98, 106, 101, 99, 116, 0, 40, 50, 48, 50, 48, 45, 48, 55, 45, 49, 51, 84, 48, 57, 58, 49, 53, 58, 49, 50, 90, 0, 48, 97, 112, 112, 108, 105, 99, 97, 116, 105, 111, 110, 47, 111, 99, 116, 101, 116, 45, 115, 116, 114, 101, 97, 109, 0, 78, 104, 116, 116, 112, 58, 47, 47, 106, 115, 111, 110, 45, 115, 99, 104, 101, 109, 97, 46, 111, 114, 103, 47, 100, 114, 97, 102, 116, 45, 48, 55, 47, 115, 99, 104, 101, 109, 97, 35, 0, 8, 1, 2, 3, 4, 0
        };
        CloudEvent event = AvroSpce.decode(encodedEvent);
        CloudEvent target = TestUtils.sampleEventWithOptionalAttributesAndBinaryData();
        assertEquals(target, event);
    }

    @Test
    public void testDecodeFastWithNoData() {
        byte[] encodedEvent = new byte[]{
                8, 22, 115, 112, 101, 99, 118, 101, 114, 115, 105, 111, 110, 6, 6, 49, 46, 48, 12, 115, 111, 117, 114, 99, 101, 6, 20, 47, 117, 115, 101, 114, 47, 49, 50, 51, 35, 4, 105, 100, 6, 6, 53, 54, 55, 8, 116, 121, 112, 101, 6, 32, 79, 120, 105, 109, 101, 116, 101, 114, 77, 101, 97, 115, 117, 114, 101, 100, 0, 2
        };
        AvroDecoder decoder = AvroDecoder.create();
        CloudEvent event = decoder.decodeFast(encodedEvent);
        CloudEvent target = TestUtils.sampleEventWithRequiredAttributes();
        assertEquals(target, event);
    }

    @Test
    public void testDecodeFastWithStringData() {
        byte[] encodedEvent = new byte[]{
                16, 30, 100, 97, 116, 97, 99, 111, 110, 116, 101, 110, 116, 116, 121, 112, 101, 6, 32, 97, 112, 112, 108, 105, 99, 97, 116, 105, 111, 110, 47, 106, 115, 111, 110, 14, 115, 117, 98, 106, 101, 99, 116, 6, 26, 83, 97, 109, 112, 108, 101, 83, 117, 98, 106, 101, 99, 116, 22, 115, 112, 101, 99, 118, 101, 114, 115, 105, 111, 110, 6, 6, 49, 46, 48, 12, 115, 111, 117, 114, 99, 101, 6, 20, 47, 117, 115, 101, 114, 47, 49, 50, 51, 35, 4, 105, 100, 6, 6, 53, 54, 55, 8, 116, 105, 109, 101, 6, 40, 50, 48, 50, 48, 45, 48, 55, 45, 49, 51, 84, 48, 57, 58, 49, 53, 58, 49, 50, 90, 8, 116, 121, 112, 101, 6, 32, 79, 120, 105, 109, 101, 116, 101, 114, 77, 101, 97, 115, 117, 114, 101, 100, 20, 100, 97, 116, 97, 115, 99, 104, 101, 109, 97, 6, 78, 104, 116, 116, 112, 58, 47, 47, 106, 115, 111, 110, 45, 115, 99, 104, 101, 109, 97, 46, 111, 114, 103, 47, 100, 114, 97, 102, 116, 45, 48, 55, 47, 115, 99, 104, 101, 109, 97, 35, 0, 0, -72, 1, 123, 34, 117, 115, 101, 114, 95, 105, 100, 34, 58, 32, 34, 98, 99, 49, 52, 53, 57, 99, 53, 45, 51, 55, 56, 100, 45, 52, 56, 51, 53, 45, 98, 52, 98, 54, 45, 97, 50, 99, 55, 100, 57, 99, 97, 55, 53, 101, 51, 34, 44, 32, 34, 115, 112, 111, 50, 34, 58, 32, 57, 54, 44, 32, 34, 101, 118, 101, 110, 116, 34, 58, 32, 34, 79, 120, 105, 109, 105, 116, 101, 114, 77, 101, 97, 115, 117, 114, 101, 100, 34, 125
        };
        AvroDecoder decoder = AvroDecoder.create();
        CloudEvent event = decoder.decodeFast(encodedEvent);
        CloudEvent target = TestUtils.sampleEventWithOptionalAttributes();
        assertEquals(target, event);
    }

    @Test
    public void testDecodeFastWithBinaryData() {
        byte[] encodedEvent = new byte[]{
                16, 30, 100, 97, 116, 97, 99, 111, 110, 116, 101, 110, 116, 116, 121, 112, 101, 6, 48, 97, 112, 112, 108, 105, 99, 97, 116, 105, 111, 110, 47, 111, 99, 116, 101, 116, 45, 115, 116, 114, 101, 97, 109, 14, 115, 117, 98, 106, 101, 99, 116, 6, 26, 83, 97, 109, 112, 108, 101, 83, 117, 98, 106, 101, 99, 116, 22, 115, 112, 101, 99, 118, 101, 114, 115, 105, 111, 110, 6, 6, 49, 46, 48, 12, 115, 111, 117, 114, 99, 101, 6, 20, 47, 117, 115, 101, 114, 47, 49, 50, 51, 35, 4, 105, 100, 6, 6, 53, 54, 55, 8, 116, 105, 109, 101, 6, 40, 50, 48, 50, 48, 45, 48, 55, 45, 49, 51, 84, 48, 57, 58, 49, 53, 58, 49, 50, 90, 8, 116, 121, 112, 101, 6, 32, 79, 120, 105, 109, 101, 116, 101, 114, 77, 101, 97, 115, 117, 114, 101, 100, 20, 100, 97, 116, 97, 115, 99, 104, 101, 109, 97, 6, 78, 104, 116, 116, 112, 58, 47, 47, 106, 115, 111, 110, 45, 115, 99, 104, 101, 109, 97, 46, 111, 114, 103, 47, 100, 114, 97, 102, 116, 45, 48, 55, 47, 115, 99, 104, 101, 109, 97, 35, 0, 0, 8, 1, 2, 3, 4
        };
        AvroDecoder decoder = AvroDecoder.create();
        CloudEvent event = decoder.decodeFast(encodedEvent);
        CloudEvent target = TestUtils.sampleEventWithOptionalAttributesAndBinaryData();
        assertEquals(target, event);
    }

    @Test
    public void testEncodeAvroSpceBatchNoItems() {
        byte[] encodedBatch = AvroSpce.encode(new ArrayList<>(0));
        assertArrayEquals(new byte[0], encodedBatch);
    }

    @Test
    public void testEncodeAvroSpceBatch1Item() {
        List<CloudEvent> events = Collections.singletonList(event1());
        byte[] encodedBatch = AvroSpce.encode(events);
        byte[] target = new byte[]{
                6, 49, 46, 48, 78, 99, 111, 109, 46, 112, 97, 114, 116, 105, 99, 108, 101, 109, 101, 116, 114, 105, 99, 115, 46, 79, 120, 105, 109, 101, 116, 101, 114, 77, 101, 97, 115, 117, 114, 101, 100, 46, 118, 49, 28, 47, 111, 105, 100, 47, 65, 49, 50, 57, 70, 50, 56, 67, 35, 2, 49, 0, 22, 79, 120, 105, 109, 101, 116, 101, 114, 49, 50, 51, 0, 40, 50, 48, 50, 48, 45, 48, 55, 45, 49, 51, 84, 48, 57, 58, 49, 53, 58, 49, 50, 90, 0, 32, 97, 112, 112, 108, 105, 99, 97, 116, 105, 111, 110, 47, 106, 115, 111, 110, 0, 98, 104, 116, 116, 112, 115, 58, 47, 47, 112, 97, 114, 116, 105, 99, 108, 101, 109, 101, 116, 114, 105, 99, 115, 46, 99, 111, 109, 47, 115, 99, 104, 101, 109, 97, 115, 47, 101, 118, 101, 110, 116, 45, 118, 49, 46, 106, 115, 111, 110, 2, -48, 1, 123, 92, 34, 117, 115, 101, 114, 95, 105, 100, 92, 34, 58, 32, 92, 34, 98, 99, 49, 52, 53, 57, 99, 53, 45, 51, 55, 56, 100, 45, 52, 56, 51, 53, 45, 98, 52, 98, 54, 45, 97, 50, 99, 55, 100, 57, 99, 97, 55, 53, 101, 51, 92, 34, 44, 32, 92, 34, 115, 112, 111, 50, 92, 34, 58, 32, 57, 54, 44, 32, 92, 34, 101, 118, 101, 110, 116, 92, 34, 58, 32, 92, 34, 79, 120, 105, 109, 105, 116, 101, 114, 77, 101, 97, 115, 117, 114, 101, 100, 92, 34, 125, 34, 125, 0
        };
        assertArrayEquals(target, encodedBatch);
    }

    @Test
    public void testEncodeAvroSpceBatch2Items() {
        List<CloudEvent> events = Arrays.asList(event1(), event2());
        byte[] encodedBatch = AvroSpce.encode(events);
        byte[] target = new byte[]{
                6, 49, 46, 48, 78, 99, 111, 109, 46, 112, 97, 114, 116, 105, 99, 108, 101, 109, 101, 116, 114, 105, 99, 115, 46, 79, 120, 105, 109, 101, 116, 101, 114, 77, 101, 97, 115, 117, 114, 101, 100, 46, 118, 49, 28, 47, 111, 105, 100, 47, 65, 49, 50, 57, 70, 50, 56, 67, 35, 2, 49, 0, 22, 79, 120, 105, 109, 101, 116, 101, 114, 49, 50, 51, 0, 40, 50, 48, 50, 48, 45, 48, 55, 45, 49, 51, 84, 48, 57, 58, 49, 53, 58, 49, 50, 90, 0, 32, 97, 112, 112, 108, 105, 99, 97, 116, 105, 111, 110, 47, 106, 115, 111, 110, 0, 98, 104, 116, 116, 112, 115, 58, 47, 47, 112, 97, 114, 116, 105, 99, 108, 101, 109, 101, 116, 114, 105, 99, 115, 46, 99, 111, 109, 47, 115, 99, 104, 101, 109, 97, 115, 47, 101, 118, 101, 110, 116, 45, 118, 49, 46, 106, 115, 111, 110, 2, -48, 1, 123, 92, 34, 117, 115, 101, 114, 95, 105, 100, 92, 34, 58, 32, 92, 34, 98, 99, 49, 52, 53, 57, 99, 53, 45, 51, 55, 56, 100, 45, 52, 56, 51, 53, 45, 98, 52, 98, 54, 45, 97, 50, 99, 55, 100, 57, 99, 97, 55, 53, 101, 51, 92, 34, 44, 32, 92, 34, 115, 112, 111, 50, 92, 34, 58, 32, 57, 54, 44, 32, 92, 34, 101, 118, 101, 110, 116, 92, 34, 58, 32, 92, 34, 79, 120, 105, 109, 105, 116, 101, 114, 77, 101, 97, 115, 117, 114, 101, 100, 92, 34, 125, 34, 125, 0, 6, 49, 46, 48, 78, 99, 111, 109, 46, 112, 97, 114, 116, 105, 99, 108, 101, 109, 101, 116, 114, 105, 99, 115, 46, 79, 120, 105, 109, 101, 116, 101, 114, 77, 101, 97, 115, 117, 114, 101, 100, 46, 118, 49, 28, 47, 111, 105, 100, 47, 65, 49, 50, 57, 70, 50, 56, 67, 35, 6, 53, 54, 55, 0, 22, 79, 120, 105, 109, 101, 116, 101, 114, 49, 50, 51, 0, 40, 50, 48, 50, 48, 45, 48, 55, 45, 49, 51, 84, 48, 57, 58, 49, 53, 58, 49, 50, 90, 0, 48, 97, 112, 112, 108, 105, 99, 97, 116, 105, 111, 110, 47, 111, 99, 116, 101, 116, 45, 115, 116, 114, 101, 97, 109, 2, 0, 8, 1, 2, 3, 4, 2, 18, 101, 120, 116, 101, 114, 110, 97, 108, 49, 4, 12, 102, 111, 111, 98, 97, 114, 0
        };
        assertArrayEquals(target, encodedBatch);
    }

    @Test
    public void testDecodeAvroSpceBatchNoItems() {
        byte[] encodedBatch = new byte[0];
        List<CloudEvent> decodedEvents = AvroSpce.decodeBatch(encodedBatch);
        List<CloudEvent> target = new ArrayList<>(0);
        assertEquals(target, decodedEvents);
    }

    @Test
    public void testDecodeAvroSpceBatch1Item() {
        byte[] encodedEvents = new byte[]{
                6, 49, 46, 48, 78, 99, 111, 109, 46, 112, 97, 114, 116, 105, 99, 108, 101, 109, 101, 116, 114, 105, 99, 115, 46, 79, 120, 105, 109, 101, 116, 101, 114, 77, 101, 97, 115, 117, 114, 101, 100, 46, 118, 49, 28, 47, 111, 105, 100, 47, 65, 49, 50, 57, 70, 50, 56, 67, 35, 2, 49, 0, 22, 79, 120, 105, 109, 101, 116, 101, 114, 49, 50, 51, 0, 40, 50, 48, 50, 48, 45, 48, 55, 45, 49, 51, 84, 48, 57, 58, 49, 53, 58, 49, 50, 90, 0, 32, 97, 112, 112, 108, 105, 99, 97, 116, 105, 111, 110, 47, 106, 115, 111, 110, 0, 98, 104, 116, 116, 112, 115, 58, 47, 47, 112, 97, 114, 116, 105, 99, 108, 101, 109, 101, 116, 114, 105, 99, 115, 46, 99, 111, 109, 47, 115, 99, 104, 101, 109, 97, 115, 47, 101, 118, 101, 110, 116, 45, 118, 49, 46, 106, 115, 111, 110, 2, -48, 1, 123, 92, 34, 117, 115, 101, 114, 95, 105, 100, 92, 34, 58, 32, 92, 34, 98, 99, 49, 52, 53, 57, 99, 53, 45, 51, 55, 56, 100, 45, 52, 56, 51, 53, 45, 98, 52, 98, 54, 45, 97, 50, 99, 55, 100, 57, 99, 97, 55, 53, 101, 51, 92, 34, 44, 32, 92, 34, 115, 112, 111, 50, 92, 34, 58, 32, 57, 54, 44, 32, 92, 34, 101, 118, 101, 110, 116, 92, 34, 58, 32, 92, 34, 79, 120, 105, 109, 105, 116, 101, 114, 77, 101, 97, 115, 117, 114, 101, 100, 92, 34, 125, 34, 125, 0
        };
        List<CloudEvent> target = Collections.singletonList(event1());
        List<CloudEvent> decodedEvents = AvroSpce.decodeBatch(encodedEvents);
        assertEquals(target, decodedEvents);
    }

    @Test
    public void testDecodeAvroSpceBatch2Items() {
        byte[] encodedEvents = new byte[]{
                6, 49, 46, 48, 78, 99, 111, 109, 46, 112, 97, 114, 116, 105, 99, 108, 101, 109, 101, 116, 114, 105, 99, 115, 46, 79, 120, 105, 109, 101, 116, 101, 114, 77, 101, 97, 115, 117, 114, 101, 100, 46, 118, 49, 28, 47, 111, 105, 100, 47, 65, 49, 50, 57, 70, 50, 56, 67, 35, 2, 49, 0, 22, 79, 120, 105, 109, 101, 116, 101, 114, 49, 50, 51, 0, 40, 50, 48, 50, 48, 45, 48, 55, 45, 49, 51, 84, 48, 57, 58, 49, 53, 58, 49, 50, 90, 0, 32, 97, 112, 112, 108, 105, 99, 97, 116, 105, 111, 110, 47, 106, 115, 111, 110, 0, 98, 104, 116, 116, 112, 115, 58, 47, 47, 112, 97, 114, 116, 105, 99, 108, 101, 109, 101, 116, 114, 105, 99, 115, 46, 99, 111, 109, 47, 115, 99, 104, 101, 109, 97, 115, 47, 101, 118, 101, 110, 116, 45, 118, 49, 46, 106, 115, 111, 110, 2, -48, 1, 123, 92, 34, 117, 115, 101, 114, 95, 105, 100, 92, 34, 58, 32, 92, 34, 98, 99, 49, 52, 53, 57, 99, 53, 45, 51, 55, 56, 100, 45, 52, 56, 51, 53, 45, 98, 52, 98, 54, 45, 97, 50, 99, 55, 100, 57, 99, 97, 55, 53, 101, 51, 92, 34, 44, 32, 92, 34, 115, 112, 111, 50, 92, 34, 58, 32, 57, 54, 44, 32, 92, 34, 101, 118, 101, 110, 116, 92, 34, 58, 32, 92, 34, 79, 120, 105, 109, 105, 116, 101, 114, 77, 101, 97, 115, 117, 114, 101, 100, 92, 34, 125, 34, 125, 0, 6, 49, 46, 48, 78, 99, 111, 109, 46, 112, 97, 114, 116, 105, 99, 108, 101, 109, 101, 116, 114, 105, 99, 115, 46, 79, 120, 105, 109, 101, 116, 101, 114, 77, 101, 97, 115, 117, 114, 101, 100, 46, 118, 49, 28, 47, 111, 105, 100, 47, 65, 49, 50, 57, 70, 50, 56, 67, 35, 6, 53, 54, 55, 0, 22, 79, 120, 105, 109, 101, 116, 101, 114, 49, 50, 51, 0, 40, 50, 48, 50, 48, 45, 48, 55, 45, 49, 51, 84, 48, 57, 58, 49, 53, 58, 49, 50, 90, 0, 48, 97, 112, 112, 108, 105, 99, 97, 116, 105, 111, 110, 47, 111, 99, 116, 101, 116, 45, 115, 116, 114, 101, 97, 109, 2, 0, 8, 1, 2, 3, 4, 2, 18, 101, 120, 116, 101, 114, 110, 97, 108, 49, 4, 12, 102, 111, 111, 98, 97, 114, 0
        };
        List<CloudEvent> target = Arrays.asList(event1(), event2());
        List<CloudEvent> decodedEvents = AvroSpce.decodeBatch(encodedEvents);
        assertEquals(target, decodedEvents);
    }

    @Test
    public void testDecodeFast() {
        CloudEvent ce1 = CloudEvent.builder()
                .setType("type1")
                .setSource("source1")
                .setId("id1")
                .put("foo", "bar")
                .build();
        CloudEvent ce2 = CloudEvent.builder()
                .setType("type1")
                .setSource("source1")
                .setId("id2")
                .put("qoo", "far")
                .build();
        AvroDecoder decoder = AvroDecoder.create();
        CloudEvent ce1Decoded = decoder.decodeFast(Avro.encode(ce1));
        assertEquals(ce1, ce1Decoded);
        CloudEvent ce2Decoded = decoder.decodeFast(Avro.encode(ce2));
        assertEquals(ce2, ce2Decoded);

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

    private static CloudEvent event1() {
        return CloudEvent.builder()
                .setType("com.particlemetrics.OximeterMeasured.v1")
                .setSource("/oid/A129F28C#")
                .setId("1")
                .setTime("2020-07-13T09:15:12Z")
                .setSubject("Oximeter123")
                .setDataContentType("application/json")
                .setDataSchema("https://particlemetrics.com/schemas/event-v1.json")
                .setData("{\\\"user_id\\\": \\\"bc1459c5-378d-4835-b4b6-a2c7d9ca75e3\\\", \\\"spo2\\\": 96, \\\"event\\\": \\\"OximiterMeasured\\\"}\"}")
                .build();
    }

    private static CloudEvent event2() {
        return CloudEvent.builder()
                .setType("com.particlemetrics.OximeterMeasured.v1")
                .setSource("/oid/A129F28C#")
                .setId("567")
                .setTime("2020-07-13T09:15:12Z")
                .setSubject("Oximeter123")
                .setDataContentType("application/octet-stream")
                .setData(new byte[]{1, 2, 3, 4})
                .put("external1", "foobar")
                .build();
    }

    private static List<Byte> byteArrayToList(final byte[] arr) {
        List<Byte> result = new ArrayList<>(arr.length);
        for (byte b : arr) {
            result.add(b);
        }
        return result;
    }
}
