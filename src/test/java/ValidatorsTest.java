import io.scaleplan.cloudevents.validators.Validators;
import org.junit.jupiter.api.Test;

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
        String timestamp1 = "2020-09-10T12:33:46Z";
        assertTrue(Validators.isValidTimestamp(timestamp1));
    }

    @Test
    public void testInvalidURI() {
        String uri = "/foo";
        assertFalse(Validators.isValidURI(uri));
    }

    @Test
    public void testInvalidTimestamp() {
        String timestamp1 = "2020-09-10T12:33:46";
        assertFalse(Validators.isValidTimestamp(timestamp1));
    }


}
