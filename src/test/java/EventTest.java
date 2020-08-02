import com.particlemetrics.events.Event;
import com.particlemetrics.events.MutableEvent;
import org.junit.jupiter.api.Test;

public class EventTest {
    @Test
    public void testRequiredAttributes() {
        TestUtils.checkEvent(TestUtils.sampleEventWithRequiredAttributes(),
                "id", "567",
                "specversion", "1.0",
                "source", "/user/123#",
                "type", "OximeterMeasured"
        );
    }

    @Test
    public void testOptionalAttributes() {
        Event event = ((MutableEvent) TestUtils.sampleEventWithRequiredAttributes())
                .setTime("2020-07-13T09:15:12Z")
                .setDataContentType("application/json")
                .setData(TestUtils.sampleData())
                .setDataSchema("http://json-schema.org/draft-07/schema#")
                .setSubject("SampleSubject");
        TestUtils.checkEvent(event,
                "id", "567",
                "specversion", "1.0",
                "source", "/user/123#",
                "type", "OximeterMeasured",
                "time", "2020-07-13T09:15:12Z",
                "datacontenttype", "application/json",
                "data", TestUtils.sampleData(),
                "dataschema", "http://json-schema.org/draft-07/schema#",
                "subject", "Oximeter123"
        );
    }

}
