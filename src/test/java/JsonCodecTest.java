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
        byte[] text = TestUtils.loadFromResource("fixtures/sample_event_extended.json");
        CloudEvent event = Json.decode(text);
        TestUtils.checkEvent(event,
                TestUtils.sampleData(),
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
        byte[] text = TestUtils.loadFromResource("fixtures/sample_event_extended_binary_data.json");
        CloudEvent event = Json.decode(text);
        TestUtils.checkEvent(event,
                TestUtils.sampleBinaryData(),
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
        CloudEvent event = TestUtils.sampleEventWithOptionalAndExtendedAttributes();
        byte[] encodedEvent = Json.encode(event);
        String encodedString = new String(encodedEvent);
        String targetString = "{\"specversion\":\"1.0\",\"type\":\"OximeterMeasured\",\"source\":\"/user/123#\",\"id\":\"567\",\"time\":\"2020-07-13T09:15:12Z\",\"subject\":\"SampleSubject\",\"datacontenttype\":\"application/json\",\"dataschema\":\"http://json-schema.org/draft-07/schema#\",\"data_base64\":\"eyJ1c2VyX2lkIjogImJjMTQ1OWM1LTM3OGQtNDgzNS1iNGI2LWEyYzdkOWNhNzVlMyIsICJzcG8yIjogOTYsICJldmVudCI6ICJPeGltaXRlck1lYXN1cmVkIn0=\",\"compmstring\":\"string-value\",\"compmint\":42,\"compmbool\":true}";
        assertArrayEquals(TestUtils.stringToSortedChars(targetString), TestUtils.stringToSortedChars(encodedString));
    }

    @Test
    public void testJsonArrayDecode() {
        byte[] text = TestUtils.loadFromResource("fixtures/sample_event_array.json");
        List<CloudEvent> events = Json.decodeArray(text);

        assertEquals(2, events.size());
        // TODO: check all attributes the events
        assertEquals("1", events.get(0).getId());
        assertEquals("2020-07-13T09:15:12Z", events.get(0).getTime());
        assertEquals("2", events.get(1).getId());
        assertEquals("2020-07-13T09:16:12Z", events.get(1).getTime());
    }

    @Test
    public void testEmptyJsonArrayDecode() {
        List<CloudEvent> events = Json.decodeArray("[]".getBytes());
        assertEquals(0, events.size());
    }

    @Test
    public void testInvalidJsonArrayDecode1() {
        assertThrows(DecodeException.class, () -> Json.decodeArray("".getBytes()));
    }

    @Test
    public void testInvalidJsonArrayDecode2() {
        assertThrows(DecodeException.class, () -> Json.decodeArray("{\"foo\":\"bar\"}".getBytes()));
    }

    @Test
    public void testInvalidJsonArrayDecode3() {
        assertThrows(ValidationException.class, () -> Json.decodeArray("[{}]".getBytes()));
    }

    @Test
    public void testJsonArrayEncode() {
        MutableCloudEvent event1 = (MutableCloudEvent) TestUtils.sampleEventWithOptionalAttributes();
        event1.setId("10");
        MutableCloudEvent event2 = (MutableCloudEvent) TestUtils.sampleEventWithOptionalAttributes();
        event2.setId("20");

        byte[] encodedEvents = Json.encode(Arrays.asList(event1, event2));
        String target = "[{\"datacontenttype\":\"application/json\",\"data_base64\":\"eyJ1c2VyX2lkIjogImJjMTQ1OWM1LTM3OGQtNDgzNS1iNGI2LWEyYzdkOWNhNzVlMyIsICJzcG8yIjogOTYsICJldmVudCI6ICJPeGltaXRlck1lYXN1cmVkIn0=\",\"subject\":\"SampleSubject\",\"specversion\":\"1.0\",\"source\":\"/user/123#\",\"id\":\"10\",\"time\":\"2020-07-13T09:15:12Z\",\"type\":\"OximeterMeasured\",\"dataschema\":\"http://json-schema.org/draft-07/schema#\"},{\"datacontenttype\":\"application/json\",\"data_base64\":\"eyJ1c2VyX2lkIjogImJjMTQ1OWM1LTM3OGQtNDgzNS1iNGI2LWEyYzdkOWNhNzVlMyIsICJzcG8yIjogOTYsICJldmVudCI6ICJPeGltaXRlck1lYXN1cmVkIn0=\",\"subject\":\"SampleSubject\",\"specversion\":\"1.0\",\"source\":\"/user/123#\",\"id\":\"20\",\"time\":\"2020-07-13T09:15:12Z\",\"type\":\"OximeterMeasured\",\"dataschema\":\"http://json-schema.org/draft-07/schema#\"}]";
        assertArrayEquals(TestUtils.stringToSortedChars(target), TestUtils.stringToSortedChars(new String(encodedEvents)));
    }

    @Test
    public void testEmptyJsonArrayEncode() {
        byte[] encodedEvents = Json.encode(new ArrayList<>());
        assertEquals("[]", new String(encodedEvents));
    }

    @Test
    public void testEncodeInvalidType() {
        MutableCloudEvent event = ((MutableCloudEvent) TestUtils.sampleEventWithOptionalAttributes())
                .put("invalidattr", 5.2);
        assertThrows(EncodeException.class, () -> Json.encode(event));

    }
}
