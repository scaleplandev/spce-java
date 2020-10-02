package scaleplan;

import io.scaleplan.spce.CloudEvent;
import io.scaleplan.spce.MutableCloudEvent;
import io.scaleplan.spce.codecs.Avro;
import io.scaleplan.spce.codecs.AvroSpce;
import io.scaleplan.spce.codecs.Json;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.infra.Blackhole;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.TimeUnit;

@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Fork(value = 2)
@Warmup(iterations = 2)
public class EventCodecBenchmark {
    private static final byte[] encodedJson = sampleEventText();
    private static final byte[] encodedAvro = Avro.encode(Json.decode(encodedJson));
    private static final CloudEvent sampleEvent = sampleEventWithOptionalAttributes();
    private static final byte[] bundleText = sampleEventBundle10();
    //    private static final AvroDecoder avroDecoder = AvroDecoder.create();
    private static final List<CloudEvent> bundleEvent10 = Json.decodeBatch(sampleEventBundle10());
    private static final byte[] encodedAvroSpce = AvroSpce.encode(sampleEvent);
    private static final byte[] bundleAvroSpce = AvroSpce.encode(bundleEvent10);


    @Benchmark
    public void benchmarkDecodeJsonEvent(Blackhole blackhole) {
        blackhole.consume(Json.decode(encodedJson));
    }

    @Benchmark
    public void benchmarkDecodeJsonEventBundle(Blackhole blackhole) {
        blackhole.consume(Json.decodeBatch(bundleText));
    }

    @Benchmark
    public void benchmarkDecodeAvroEvent(Blackhole blackhole) {
        blackhole.consume(Avro.decode(encodedAvro));
    }

    @Benchmark
    public void benchmarkDecodeAvroEventAlt(Blackhole blackhole) {
        blackhole.consume(AvroSpce.decode(encodedAvroSpce));
    }

    @Benchmark
    public void benchmarkDecodeBatchAvroEventAlt(Blackhole blackhole) {
        blackhole.consume(AvroSpce.decodeBatch(bundleAvroSpce));
    }

    /*
    @Benchmark
    public void benchmarkDecodeAvroEventFast(Blackhole blackhole) {
        blackhole.consume(avroDecoder.decodeFast(encodedAvro));
    }
    */

    @Benchmark
    public void benchmarkEncodeJsonEvent(Blackhole blackhole) {
        blackhole.consume(Json.encode(sampleEvent));
    }

    @Benchmark
    public void benchmarkEncodeAvroEvent(Blackhole blackhole) {
        blackhole.consume(Avro.encode(sampleEvent));
    }

    @Benchmark
    public void benchmarkEncodeAvroEventAlt(Blackhole blackhole) {
        blackhole.consume(AvroSpce.encode(sampleEvent));
    }

    @Benchmark
    public void benchmarkEncodeBatchAvroEventAlt(Blackhole blackhole) {
        blackhole.consume(AvroSpce.encode(bundleEvent10));
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

    public static CloudEvent sampleEventWithRequiredAttributes() {
        return CloudEvent.builder()
                .setType("OximeterMeasured")
                .setSource("/user/123#")
                .setId("567")
                .build();
    }

    private static CloudEvent sampleEventWithOptionalAttributes() {
        MutableCloudEvent event = (MutableCloudEvent) sampleEventWithRequiredAttributes();
        event
                .setTime("2020-07-13T09:15:12Z")
                .setDataContentType("application/json")
                .setDataUnsafe(sampleData())
                .setDataSchema("http://json-schema.org/draft-07/schema#")
                .setSubject("SampleSubject");
        return event;
    }

    private static byte[] sampleData() {
        String text = "{\"user_id\": \"bc1459c5-378d-4835-b4b6-a2c7d9ca75e3\", \"spo2\": 96, \"event\": \"OximiterMeasured\"}";
        return text.getBytes(StandardCharsets.UTF_8);
    }

    private static byte[] sampleEventBundle10() {
        String text = "[\n" +
                "  {\n" +
                "    \"specversion\" : \"1.0\",\n" +
                "    \"type\" : \"com.particlemetrics.OximeterMeasured.v1\",\n" +
                "    \"source\" : \"/oid/A129F28C#\",\n" +
                "    \"id\" : \"1\",\n" +
                "    \"time\" : \"2020-07-13T09:15:12Z\",\n" +
                "    \"datacontenttype\" : \"application/json\",\n" +
                "    \"dataschema\": \"https://particlemetrics.com/schemas/event-v1.json\",\n" +
                "    \"data\" : \"{\\\"user_id\\\": \\\"bc1459c5-378d-4835-b4b6-a2c7d9ca75e3\\\", \\\"spo2\\\": 96, \\\"event\\\": \\\"OximiterMeasured\\\"}\"\n" +
                "  },\n" +
                "  {\n" +
                "    \"specversion\" : \"1.0\",\n" +
                "    \"type\" : \"com.particlemetrics.OximeterMeasured.v1\",\n" +
                "    \"source\" : \"/oid/A129F28C#\",\n" +
                "    \"id\" : \"1\",\n" +
                "    \"time\" : \"2020-07-13T09:15:12Z\",\n" +
                "    \"datacontenttype\" : \"application/json\",\n" +
                "    \"dataschema\": \"https://particlemetrics.com/schemas/event-v1.json\",\n" +
                "    \"data\" : \"{\\\"user_id\\\": \\\"bc1459c5-378d-4835-b4b6-a2c7d9ca75e3\\\", \\\"spo2\\\": 96, \\\"event\\\": \\\"OximiterMeasured\\\"}\"\n" +
                "  },\n" +
                "  {\n" +
                "    \"specversion\" : \"1.0\",\n" +
                "    \"type\" : \"com.particlemetrics.OximeterMeasured.v1\",\n" +
                "    \"source\" : \"/oid/A129F28C#\",\n" +
                "    \"id\" : \"1\",\n" +
                "    \"time\" : \"2020-07-13T09:15:12Z\",\n" +
                "    \"datacontenttype\" : \"application/json\",\n" +
                "    \"dataschema\": \"https://particlemetrics.com/schemas/event-v1.json\",\n" +
                "    \"data\" : \"{\\\"user_id\\\": \\\"bc1459c5-378d-4835-b4b6-a2c7d9ca75e3\\\", \\\"spo2\\\": 96, \\\"event\\\": \\\"OximiterMeasured\\\"}\"\n" +
                "  },\n" +
                "  {\n" +
                "    \"specversion\" : \"1.0\",\n" +
                "    \"type\" : \"com.particlemetrics.OximeterMeasured.v1\",\n" +
                "    \"source\" : \"/oid/A129F28C#\",\n" +
                "    \"id\" : \"1\",\n" +
                "    \"time\" : \"2020-07-13T09:15:12Z\",\n" +
                "    \"datacontenttype\" : \"application/json\",\n" +
                "    \"dataschema\": \"https://particlemetrics.com/schemas/event-v1.json\",\n" +
                "    \"data\" : \"{\\\"user_id\\\": \\\"bc1459c5-378d-4835-b4b6-a2c7d9ca75e3\\\", \\\"spo2\\\": 96, \\\"event\\\": \\\"OximiterMeasured\\\"}\"\n" +
                "  },\n" +
                "  {\n" +
                "    \"specversion\" : \"1.0\",\n" +
                "    \"type\" : \"com.particlemetrics.OximeterMeasured.v1\",\n" +
                "    \"source\" : \"/oid/A129F28C#\",\n" +
                "    \"id\" : \"1\",\n" +
                "    \"time\" : \"2020-07-13T09:15:12Z\",\n" +
                "    \"datacontenttype\" : \"application/json\",\n" +
                "    \"dataschema\": \"https://particlemetrics.com/schemas/event-v1.json\",\n" +
                "    \"data\" : \"{\\\"user_id\\\": \\\"bc1459c5-378d-4835-b4b6-a2c7d9ca75e3\\\", \\\"spo2\\\": 96, \\\"event\\\": \\\"OximiterMeasured\\\"}\"\n" +
                "  },\n" +
                "  {\n" +
                "    \"specversion\" : \"1.0\",\n" +
                "    \"type\" : \"com.particlemetrics.OximeterMeasured.v1\",\n" +
                "    \"source\" : \"/oid/A129F28C#\",\n" +
                "    \"id\" : \"1\",\n" +
                "    \"time\" : \"2020-07-13T09:15:12Z\",\n" +
                "    \"datacontenttype\" : \"application/json\",\n" +
                "    \"dataschema\": \"https://particlemetrics.com/schemas/event-v1.json\",\n" +
                "    \"data\" : \"{\\\"user_id\\\": \\\"bc1459c5-378d-4835-b4b6-a2c7d9ca75e3\\\", \\\"spo2\\\": 96, \\\"event\\\": \\\"OximiterMeasured\\\"}\"\n" +
                "  },\n" +
                "  {\n" +
                "    \"specversion\" : \"1.0\",\n" +
                "    \"type\" : \"com.particlemetrics.OximeterMeasured.v1\",\n" +
                "    \"source\" : \"/oid/A129F28C#\",\n" +
                "    \"id\" : \"1\",\n" +
                "    \"time\" : \"2020-07-13T09:15:12Z\",\n" +
                "    \"datacontenttype\" : \"application/json\",\n" +
                "    \"dataschema\": \"https://particlemetrics.com/schemas/event-v1.json\",\n" +
                "    \"data\" : \"{\\\"user_id\\\": \\\"bc1459c5-378d-4835-b4b6-a2c7d9ca75e3\\\", \\\"spo2\\\": 96, \\\"event\\\": \\\"OximiterMeasured\\\"}\"\n" +
                "  },\n" +
                "  {\n" +
                "    \"specversion\" : \"1.0\",\n" +
                "    \"type\" : \"com.particlemetrics.OximeterMeasured.v1\",\n" +
                "    \"source\" : \"/oid/A129F28C#\",\n" +
                "    \"id\" : \"1\",\n" +
                "    \"time\" : \"2020-07-13T09:15:12Z\",\n" +
                "    \"datacontenttype\" : \"application/json\",\n" +
                "    \"dataschema\": \"https://particlemetrics.com/schemas/event-v1.json\",\n" +
                "    \"data\" : \"{\\\"user_id\\\": \\\"bc1459c5-378d-4835-b4b6-a2c7d9ca75e3\\\", \\\"spo2\\\": 96, \\\"event\\\": \\\"OximiterMeasured\\\"}\"\n" +
                "  },\n" +
                "  {\n" +
                "    \"specversion\" : \"1.0\",\n" +
                "    \"type\" : \"com.particlemetrics.OximeterMeasured.v1\",\n" +
                "    \"source\" : \"/oid/A129F28C#\",\n" +
                "    \"id\" : \"1\",\n" +
                "    \"time\" : \"2020-07-13T09:15:12Z\",\n" +
                "    \"datacontenttype\" : \"application/json\",\n" +
                "    \"dataschema\": \"https://particlemetrics.com/schemas/event-v1.json\",\n" +
                "    \"data\" : \"{\\\"user_id\\\": \\\"bc1459c5-378d-4835-b4b6-a2c7d9ca75e3\\\", \\\"spo2\\\": 96, \\\"event\\\": \\\"OximiterMeasured\\\"}\"\n" +
                "  },\n" +
                "  {\n" +
                "    \"specversion\" : \"1.0\",\n" +
                "    \"type\" : \"com.particlemetrics.OximeterMeasured.v1\",\n" +
                "    \"source\" : \"/oid/A129F28C#\",\n" +
                "    \"id\" : \"1\",\n" +
                "    \"time\" : \"2020-07-13T09:15:12Z\",\n" +
                "    \"datacontenttype\" : \"application/json\",\n" +
                "    \"dataschema\": \"https://particlemetrics.com/schemas/event-v1.json\",\n" +
                "    \"data\" : \"{\\\"user_id\\\": \\\"bc1459c5-378d-4835-b4b6-a2c7d9ca75e3\\\", \\\"spo2\\\": 96, \\\"event\\\": \\\"OximiterMeasured\\\"}\"\n" +
                "  }\n" +
                "]\n";
        return text.getBytes();
    }


}
