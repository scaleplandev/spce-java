import com.particlemetrics.events.Event;
import com.particlemetrics.events.codecs.impl.JsonDecoder;
import com.particlemetrics.events.codecs.impl.JsonEncoder;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class JsonCodecTest {
    @Test
    public void testJsonDecode() {
        byte[] text = TestUtils.loadFromResource("fixtures/sample_event.json");
        JsonDecoder decoder = JsonDecoder.create();
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
                "subject", "Oximeter123"
        );
    }

    @Test
    public void testJsonEncode() {
        JsonEncoder encoder = JsonEncoder.create();
        Event event = TestUtils.sampleEventWithOptionalAttributes();
        byte[] encodedEvent = encoder.encode(event);
        String encodedString = new String(encodedEvent);
        String targetString = "{\"specversion\":\"1.0\",\"type\":\"OximeterMeasured\",\"source\":\"/user/123#\",\"id\":\"567\",\"time\":\"2020-07-13T09:15:12Z\",\"subject\":\"SampleSubject\",\"datacontenttype\":\"application/json\",\"dataschema\":\"http://json-schema.org/draft-07/schema#\",\"data_base64\":\"eyJ1c2VyX2lkIjogImJjMTQ1OWM1LTM3OGQtNDgzNS1iNGI2LWEyYzdkOWNhNzVlMyIsICJzcG8yIjogOTYsICJldmVudCI6ICJPeGltaXRlck1lYXN1cmVkIn0=\"}";
        assertEquals(targetString, encodedString);
    }
}
