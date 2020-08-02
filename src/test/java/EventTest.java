import com.particlemetrics.events.Event;
import com.particlemetrics.events.MutableEvent;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class EventTest {
    @Test
    public void testRequiredAttributes() {
        Event event = TestUtils.sampleEventWithRequiredAttributes();
        TestUtils.checkEvent(event,
                "id", "567",
                "specversion", "1.0",
                "source", "/user/123#",
                "type", "OximeterMeasured"
        );
        assertEquals("1.0", event.getSpecVersion());
        assertEquals("OximeterMeasured", event.getType());
        assertEquals("/user/123#", event.getSource());
        assertEquals("567", event.getId());
    }

    @Test
    public void testOptionalAttributes() {
        final Event baseEvent = TestUtils.sampleEventWithRequiredAttributes();
        // MutableEventImpl do
        Event event = ((MutableEvent) baseEvent)
                .setTime("2020-07-13T09:15:12Z")
                .setDataContentType("application/json")
                .setData(TestUtils.sampleData())
                .setDataSchema("https://particlemetrics.com/oximeter-schema#")
                .setSubject("SampleSubject");
        TestUtils.checkEvent(event,
                "id", "567",
                "specversion", "1.0",
                "source", "/user/123#",
                "type", "OximeterMeasured",
                "time", "2020-07-13T09:15:12Z",
                "datacontenttype", "application/json",
                "data", TestUtils.sampleData(),
                "dataschema", "https://particlemetrics.com/oximeter-schema#",
                "subject", "SampleSubject"
        );
        assertEquals("1.0", event.getSpecVersion());
        assertEquals("OximeterMeasured", event.getType());
        assertEquals("/user/123#", event.getSource());
        assertEquals("567", event.getId());
        assertEquals("2020-07-13T09:15:12Z", event.getTime());
        assertEquals("application/json", event.getDataContentType());
        assertArrayEquals(TestUtils.sampleData(), event.getData());
        assertEquals("https://particlemetrics.com/oximeter-schema#", event.getDataSchema());
        assertEquals("SampleSubject", event.getSubject());
    }
}
