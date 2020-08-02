import com.particlemetrics.events.MutableEvent;
import com.particlemetrics.events.ValidationException;
import com.particlemetrics.events.impl.MutableEventImpl;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertThrows;

public class EventValidationTest {
    @Test
    public void testValidEvent() {
        MutableEventImpl event = (MutableEventImpl) MutableEventImpl.create("OximeterMeasured", "/user/123", "2");
        event.validate();
    }

    @Test
    public void testInvalidEventMissingSpec() {
        MutableEvent event = MutableEventImpl.create()
                .setType("OximeterMeasured")
                .setSource("/user/123")
                .setId("10");
        assertThrows(ValidationException.class, ((MutableEventImpl) event)::validate);
    }

    @Test
    public void testInvalidEventMissingType() {
        MutableEvent event = MutableEventImpl.create()
                .setSpecVersion("OximeterMeasured")
                .setSource("/user/123")
                .setId("10");
        assertThrows(ValidationException.class, ((MutableEventImpl) event)::validate);
    }

    @Test
    public void testInvalidEventMissingSource() {
        MutableEvent event = MutableEventImpl.create()
                .setSpecVersion("OximeterMeasured")
                .setType("OximeterMeasured")
                .setId("10");
        assertThrows(ValidationException.class, ((MutableEventImpl) event)::validate);
    }

    @Test
    public void testInvalidEventMissingId() {
        MutableEvent event = MutableEventImpl.create()
                .setSpecVersion("OximeterMeasured")
                .setType("OximeterMeasured")
                .setSource("/user/123");
        assertThrows(ValidationException.class, ((MutableEventImpl) event)::validate);
    }
}
