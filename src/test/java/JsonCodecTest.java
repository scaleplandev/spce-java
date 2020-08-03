import com.particlemetrics.events.Event;
import com.particlemetrics.events.MutableEvent;
import com.particlemetrics.events.ValidationException;
import com.particlemetrics.events.codecs.DecodeException;
import com.particlemetrics.events.codecs.Json;
import com.particlemetrics.events.codecs.impl.JsonDecoder;
import com.particlemetrics.events.codecs.impl.JsonEncoder;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

public class JsonCodecTest {
    static JsonDecoder decoder = Json.getDecoder();
    static JsonEncoder encoder = Json.getEncoder();

    @Test
    public void testJsonDecode() {
        byte[] text = TestUtils.loadFromResource("fixtures/sample_event_extended.json");
        Event event = decoder.decode(text);
        TestUtils.checkEvent(event,
                "id", "1",
                "specversion", "1.0",
                "source", "/oid/A129F28C#",
                "type", "com.particlemetrics.OximeterMeasured.v1",
                "time", "2020-07-13T09:15:12Z",
                "datacontenttype", "application/json",
                "data", TestUtils.sampleData(),
                "dataschema", "https://particlemetrics.com/schemas/event-v1.json",
                "subject", "Oximeter123",
                "compmstring", "string-value",
                "compmint", 42,
                "compmbool", true
        );
    }

    @Test
    public void testJsonEncode() {
        Event event = TestUtils.sampleEventWithOptionalAndExtendedAttributes();
        byte[] encodedEvent = encoder.encode(event);
        String encodedString = new String(encodedEvent);
        String targetString = "{\"specversion\":\"1.0\",\"type\":\"OximeterMeasured\",\"source\":\"/user/123#\",\"id\":\"567\",\"time\":\"2020-07-13T09:15:12Z\",\"subject\":\"SampleSubject\",\"datacontenttype\":\"application/json\",\"dataschema\":\"http://json-schema.org/draft-07/schema#\",\"data_base64\":\"eyJ1c2VyX2lkIjogImJjMTQ1OWM1LTM3OGQtNDgzNS1iNGI2LWEyYzdkOWNhNzVlMyIsICJzcG8yIjogOTYsICJldmVudCI6ICJPeGltaXRlck1lYXN1cmVkIn0=\",\"compmstring\":\"string-value\",\"compmint\":42,\"compmbool\":true}";
        assertArrayEquals(TestUtils.stringToSortedChars(targetString), TestUtils.stringToSortedChars(encodedString));
    }

    @Test
    public void testJsonArrayDecode() {
        byte[] text = TestUtils.loadFromResource("fixtures/sample_event_array.json");
        List<Event> events = decoder.decodeArray(text);

        assertEquals(2, events.size());
        // TODO: check all attributes the events
        assertEquals("1", events.get(0).getId());
        assertEquals("2020-07-13T09:15:12Z", events.get(0).getTime());
        assertEquals("2", events.get(1).getId());
        assertEquals("2020-07-13T09:16:12Z", events.get(1).getTime());
    }

    @Test
    public void testEmptyJsonArrayDecode() {
        List<Event> events = decoder.decodeArray("[]".getBytes());
        assertEquals(0, events.size());
    }

    @Test
    public void testInvalidJsonArrayDecode1() {
        assertThrows(DecodeException.class, () -> decoder.decodeArray("".getBytes()));
    }

    @Test
    public void testInvalidJsonArrayDecode2() {
        assertThrows(DecodeException.class, () -> decoder.decodeArray("{\"foo\":\"bar\"}".getBytes()));
    }

    @Test
    public void testInvalidJsonArrayDecode3() {
        assertThrows(ValidationException.class, () -> decoder.decodeArray("[{}]".getBytes()));
    }

    @Test
    public void testJsonArrayEncode() {
        MutableEvent event1 = (MutableEvent) TestUtils.sampleEventWithOptionalAttributes();
        event1.setId("10");
        MutableEvent event2 = (MutableEvent) TestUtils.sampleEventWithOptionalAttributes();
        event2.setId("20");

        byte[] encodedEvents = encoder.encode(Arrays.asList(event1, event2));
        String target = "[{\"datacontenttype\":\"application/json\",\"data_base64\":\"eyJ1c2VyX2lkIjogImJjMTQ1OWM1LTM3OGQtNDgzNS1iNGI2LWEyYzdkOWNhNzVlMyIsICJzcG8yIjogOTYsICJldmVudCI6ICJPeGltaXRlck1lYXN1cmVkIn0=\",\"subject\":\"SampleSubject\",\"specversion\":\"1.0\",\"source\":\"/user/123#\",\"id\":\"10\",\"time\":\"2020-07-13T09:15:12Z\",\"type\":\"OximeterMeasured\",\"dataschema\":\"http://json-schema.org/draft-07/schema#\"},{\"datacontenttype\":\"application/json\",\"data_base64\":\"eyJ1c2VyX2lkIjogImJjMTQ1OWM1LTM3OGQtNDgzNS1iNGI2LWEyYzdkOWNhNzVlMyIsICJzcG8yIjogOTYsICJldmVudCI6ICJPeGltaXRlck1lYXN1cmVkIn0=\",\"subject\":\"SampleSubject\",\"specversion\":\"1.0\",\"source\":\"/user/123#\",\"id\":\"20\",\"time\":\"2020-07-13T09:15:12Z\",\"type\":\"OximeterMeasured\",\"dataschema\":\"http://json-schema.org/draft-07/schema#\"}]";
        assertArrayEquals(TestUtils.stringToSortedChars(target), TestUtils.stringToSortedChars(new String(encodedEvents)));
    }

    @Test
    public void testEmptyJsonArrayEncode() {
        byte[] encodedEvents = encoder.encode(new ArrayList<>());
        assertEquals("[]", new String(encodedEvents));
    }
}
