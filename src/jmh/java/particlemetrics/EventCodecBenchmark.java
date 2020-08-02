package particlemetrics;

import com.particlemetrics.events.Event;
import com.particlemetrics.events.MutableEvent;
import com.particlemetrics.events.codecs.impl.JsonDecoder;
import com.particlemetrics.events.codecs.impl.JsonEncoder;
import com.particlemetrics.events.impl.MutableEventImpl;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.infra.Blackhole;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.TimeUnit;

@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Fork(value = 2)
@Warmup(iterations = 2)
public class EventCodecBenchmark {
    private static final byte[] text = sampleEventText();
    private static final Event sampleEvent = sampleEventWithOptionalAttributes();
    private static final JsonDecoder decoder = JsonDecoder.create();
    private static final JsonEncoder encoder = JsonEncoder.create();

    @Benchmark
    public void benchmarkDecodeEvent(Blackhole blackhole) {
        blackhole.consume(decoder.decode(text));
    }

    @Benchmark
    public void benchmarkEncodeEvent(Blackhole blackhole) {
        blackhole.consume(encoder.encode(sampleEvent));
    }

    private static byte[] sampleEventText() {
        String text = "{\n" +
                "  \"specversion\" : \"1.0\",\n" +
                "  \"type\" : \"com.particlemetrics.OximeterMeasured.v1\",\n" +
                "  \"source\" : \"/oid/A129F28C#\",\n" +
                "  \"id\" : \"1\",\n" +
                "  \"time\" : \"2020-07-13T09:15:12Z\",\n" +
                "  \"subject\": \"Oximeter123\",\n" +
                "  \"datacontenttype\" : \"application/json\",\n" +
                "  \"dataschema\": \"https://particlemetrics.com/schemas/event-v1.json\",\n" +
                "  \"data\" : \"{\\\"user_id\\\": \\\"bc1459c5-378d-4835-b4b6-a2c7d9ca75e3\\\", \\\"spo2\\\": 96, \\\"event\\\": \\\"OximiterMeasured\\\"}\"\n" +
                "}\n";
        return text.getBytes();
    }

    private static Event sampleEventWithRequiredAttributes() {
        return MutableEventImpl.create(
                "OximeterMeasured",
                "/user/123#",
                "567"
        );
    }

    private static Event sampleEventWithOptionalAttributes() {
        MutableEvent event = (MutableEvent) sampleEventWithRequiredAttributes();
        event
                .setTime("2020-07-13T09:15:12Z")
                .setDataContentType("application/json")
                .setData(sampleData())
                .setDataSchema("http://json-schema.org/draft-07/schema#")
                .setSubject("SampleSubject");
        return event;
    }

    private static byte[] sampleData() {
        String text = "{\"user_id\": \"bc1459c5-378d-4835-b4b6-a2c7d9ca75e3\", \"spo2\": 96, \"event\": \"OximiterMeasured\"}";
        return text.getBytes(StandardCharsets.UTF_8);
    }


}
