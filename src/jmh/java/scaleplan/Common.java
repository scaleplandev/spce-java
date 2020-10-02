package scaleplan;

import io.scaleplan.spce.CloudEvent;

import java.util.ArrayList;
import java.util.List;

public class Common {
    public static String sampleStringData = "{\"user_id\": \"bc1459c5-378d-4835-b4b6-a2c7d9ca75e3\", \"spo2\": 96, \"event\": \"OximiterMeasured\"}";
    public static byte[] sampleBinaryData = sampleStringData.getBytes();
    public static CloudEvent sampleEvent = makeSampleEvent();
    public static List<CloudEvent> sampleEventBundle = makeSampleEventBundle(10);

    public static CloudEvent makeSampleEvent() {
        return CloudEvent.builder()
                .setType("OximeterMeasured")
                .setSource("/user/123#")
                .setId("567")
                .setTime("2020-07-13T09:15:12Z")
                .setDataContentType("application/json")
                .setData(sampleStringData)
                .setDataSchema("http://json-schema.org/draft-07/schema#")
                .setSubject("SampleSubject")
                .put("compmstring", "string-value")
                .build();
    }

    public static List<CloudEvent> makeSampleEventBundle(int n) {
        List<CloudEvent> events = new ArrayList<>(n);
        for (int i = 0; i < n; i++) events.add(makeSampleEvent());
        return events;
    }
}
