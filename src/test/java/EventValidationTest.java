import io.scaleplan.spce.MutableCloudEvent;
import io.scaleplan.spce.ValidationException;
import io.scaleplan.spce.impl.MutableCloudEventImpl;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertThrows;

public class EventValidationTest {
    @Test
    public void testValidEvent() {
        MutableCloudEventImpl event = (MutableCloudEventImpl) MutableCloudEventImpl.create("OximeterMeasured", "/user/123", "2");
        event.validate();
    }

    @Test
    public void testInvalidEventMissingSpec() {
        MutableCloudEvent event = MutableCloudEventImpl.create()
                .setType("OximeterMeasured")
                .setSource("/user/123")
                .setId("10");
        assertThrows(ValidationException.class, ((MutableCloudEventImpl) event)::validate);
    }

    @Test
    public void testInvalidEventMissingType() {
        MutableCloudEvent event = MutableCloudEventImpl.create()
                .setSpecVersion("OximeterMeasured")
                .setSource("/user/123")
                .setId("10");
        assertThrows(ValidationException.class, ((MutableCloudEventImpl) event)::validate);
    }

    @Test
    public void testInvalidEventMissingSource() {
        MutableCloudEvent event = MutableCloudEventImpl.create()
                .setSpecVersion("OximeterMeasured")
                .setType("OximeterMeasured")
                .setId("10");
        assertThrows(ValidationException.class, ((MutableCloudEventImpl) event)::validate);
    }

    @Test
    public void testInvalidEventMissingId() {
        MutableCloudEvent event = MutableCloudEventImpl.create()
                .setSpecVersion("OximeterMeasured")
                .setType("OximeterMeasured")
                .setSource("/user/123");
        assertThrows(ValidationException.class, ((MutableCloudEventImpl) event)::validate);
    }
}
