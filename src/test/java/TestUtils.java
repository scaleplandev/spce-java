import com.particlemetrics.events.Event;
import com.particlemetrics.events.MutableEvent;
import com.particlemetrics.events.impl.MutableEventImpl;

import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class TestUtils {
    static byte[] loadFromResource(String name) {
        try {
            URL fileUrl = Objects.requireNonNull(TestUtils.class.getClassLoader().getResource(name));
            return Files.readAllBytes(Paths.get(fileUrl.toURI()));
        } catch (IOException | URISyntaxException e) {
            throw new RuntimeException(e);
        }
    }

    public static void checkEvent(Event event, Object... targetNameValues) {
        assert targetNameValues.length % 2 == 0;
        Map<String, Object> targetMap = new HashMap<>(targetNameValues.length / 2);
        String name = "";
        for (int i = 0; i < targetNameValues.length; i++) {
            if (i % 2 == 0) name = (String) targetNameValues[i];
            else targetMap.put(name, targetNameValues[i]);
        }
        Map<String, Object> eventMap = event.asMap();
        assertEquals(targetMap.keySet(), eventMap.keySet());
        for (Map.Entry<String, Object> kv : targetMap.entrySet()) {
            Object value = kv.getValue();
            if (value instanceof byte[]) {
                assertArrayEquals((byte[]) value, (byte[]) eventMap.get(kv.getKey()));
            } else {
                assertEquals(kv.getValue(), eventMap.get(kv.getKey()));
            }
        }
    }

    public static byte[] sampleData() {
        String text = "{\"user_id\": \"bc1459c5-378d-4835-b4b6-a2c7d9ca75e3\", \"spo2\": 96, \"event\": \"OximiterMeasured\"}";
        return text.getBytes(StandardCharsets.UTF_8);
    }

    public static Event sampleEventWithRequiredAttributes() {
        return MutableEventImpl.create(
                "OximeterMeasured",
                "/user/123#",
                "567"
        );
    }

    public static Event sampleEventWithOptionalAttributes() {
        MutableEvent event = (MutableEvent) sampleEventWithRequiredAttributes();
        event
                .setTime("2020-07-13T09:15:12Z")
                .setDataContentType("application/json")
                .setDataUnsafe(TestUtils.sampleData())
                .setDataSchema("http://json-schema.org/draft-07/schema#")
                .setSubject("SampleSubject");
        return event;
    }

    public static Event sampleEventWithOptionalAndExtendedAttributes() {
        MutableEvent event = (MutableEvent) sampleEventWithOptionalAttributes();
        event
                .setAttribute("compmstring", "string-value")
                .setAttribute("compmint", 42)
                .setAttribute("compmbool", true);
        return event;
    }

    public static Object[] stringToSortedChars(String s) {
        List<Character> characterList = new ArrayList<>();
        for (char c : s.toCharArray()) {
            characterList.add(c);
        }
        Object[] arr = characterList.toArray();
        Arrays.sort(arr);
        return arr;
    }

}
