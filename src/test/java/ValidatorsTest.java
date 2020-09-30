import io.scaleplan.spce.validators.Validators;
import org.junit.jupiter.api.Test;

import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.*;

public class ValidatorsTest {
    @Test
    public void testValidURI() {
        String uri = "https://scaleplan.io/cloudevents";
        assertTrue(Validators.isValidURI(uri));
    }

    @Test
    public void testRequireValidURI() {
        String uri = "https://scaleplan.io/cloudevents";
        String returnedUri = Validators.requireValidURI(uri);
        assertEquals(uri, returnedUri);

        uri = "http://localhost/cloudevents";
        returnedUri = Validators.requireValidURI(uri);
        assertEquals(uri, returnedUri);
    }

    @Test
    public void testValidURIRef() {
        String uriRef = "/foo";
        assertTrue(Validators.isValidURIRef(uriRef));

        String uri = "https://scaleplan.io/cloudevents";
        assertTrue(Validators.isValidURIRef(uri));
    }

    @Test
    public void testRequireValidURIRef() {
        String uriRef = "/foo";
        String returnedUriRef = Validators.requireValidURIRef(uriRef);
        assertEquals(uriRef, returnedUriRef);
    }

    @Test
    public void testValidTimestamp() {
        String timestamp = "2020-09-10T12:33:46Z";
        assertTrue(Validators.isValidTimestamp(timestamp));
    }

    @Test
    public void testRequireValidTimestamp() {
        String timestamp = "2020-09-10T12:33:46Z";
        String returnedTimestamp = Validators.requireValidTimestamp(timestamp);
        assertEquals(timestamp, returnedTimestamp);
    }

    @Test
    public void testInvalidURI() {
        for (String uri : Arrays.asList("/foo", "123", "123:123", "")) {
            assertFalse(Validators.isValidURI(uri));
        }
    }

    @Test
    public void testInvalidTimestamp() {
        String timestamp1 = "2020-09-10T12:33:46";
        assertFalse(Validators.isValidTimestamp(timestamp1));
    }


}
