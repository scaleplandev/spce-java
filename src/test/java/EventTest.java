import com.particlemetrics.events.Event;
import com.particlemetrics.events.MutableEvent;
import com.particlemetrics.events.impl.MutableEventImpl;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

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
        Event baseEvent = TestUtils.sampleEventWithRequiredAttributes();
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

    @Test
    public void testExtendedAttributes() {
        Event baseEvent = TestUtils.sampleEventWithRequiredAttributes();
        // MutableEventImpl do
        Event event = ((MutableEvent) baseEvent)
                .setAttribute("compmstring", "string-value")
                .setAttribute("compmint", 42)
                .setAttribute("compmbool", true);
        assertEquals("1.0", event.getSpecVersion());
        assertEquals("OximeterMeasured", event.getType());
        assertEquals("/user/123#", event.getSource());
        assertEquals("567", event.getId());
        assertEquals("string-value", event.getAttribute("compmstring"));
        assertEquals("string-value", event.getStringAttribute("compmstring"));
        assertEquals(42, event.getIntAttribute("compmint"));
        assertEquals(true, event.getBoolAttribute("compmbool"));
    }

    @Test
    public void testBinaryData() {
        Event event = MutableEventImpl.create()
                .setData("foobar");
        assertFalse(event.hasBinaryData());
    }

    @Test
    public void testStringData() {
        Event event = MutableEventImpl.create()
                .setData("foobar".getBytes());
        assertTrue(event.hasBinaryData());
    }

    @Test
    public void testDeleteAttribute() {
        MutableEvent event = MutableEventImpl.create()
                .setAttribute("foobar", "zoo");
        assertEquals("zoo", event.getAttribute("foobar"));
        event.removeAttribute("foobar");
        assertNull(event.getAttribute("foobar"));
    }

    @Test
    public void testCannotDeleteRequiredAttribute() {
        MutableEvent event = MutableEventImpl.create()
                .setType("MyEvent");
        assertEquals("MyEvent", event.getType());
        assertThrows(IllegalArgumentException.class, () -> event.removeAttribute("type"));
    }
}
