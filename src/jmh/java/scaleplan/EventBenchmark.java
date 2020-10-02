package scaleplan;

import io.scaleplan.spce.CloudEvent;
import io.scaleplan.spce.impl.MutableCloudEventImpl;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.infra.Blackhole;

import java.util.concurrent.TimeUnit;

@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Fork(value = 2)
@Warmup(iterations = 2)
public class EventBenchmark {
    @Benchmark
    public void builderCreateEventWithStringData(Blackhole blackhole) {
        CloudEvent event = CloudEvent.builder()
                .setType("OximeterMeasured")
                .setSource("/user/123#")
                .setId("567")
                .setTime("2020-07-13T09:15:12Z")
                .setDataContentType("application/json")
                .setData(Common.sampleStringData)
                .setDataSchema("http://json-schema.org/draft-07/schema#")
                .setSubject("SampleSubject")
                .put("compmstring", "string-value")
                .build();
        blackhole.consume(event);
    }

    @Benchmark
    public void builderCreateEventWithBinaryData(Blackhole blackhole) {
        CloudEvent event = CloudEvent.builder()
                .setType("OximeterMeasured")
                .setSource("/user/123#")
                .setId("567")
                .setTime("2020-07-13T09:15:12Z")
                .setDataContentType("application/nosj")
                .setData(Common.sampleBinaryData)
                .setDataSchema("http://json-schema.org/draft-07/schema#")
                .setSubject("SampleSubject")
                .put("compmstring", "string-value")
                .build();
        blackhole.consume(event);
    }

    @Benchmark
    public void directCreateEventWithStringData(Blackhole blackhole) {
        CloudEvent event = MutableCloudEventImpl.create()
                .setType("OximeterMeasured")
                .setSource("/user/123#")
                .setId("567")
                .setTime("2020-07-13T09:15:12Z")
                .setDataContentType("application/json")
                .setData(Common.sampleStringData)
                .setDataSchema("http://json-schema.org/draft-07/schema#")
                .setSubject("SampleSubject")
                .put("compmstring", "string-value");
        blackhole.consume(event);
    }

    @Benchmark
    public void directCreateEventWithBinaryData(Blackhole blackhole) {
        CloudEvent event = MutableCloudEventImpl.create()
                .setType("OximeterMeasured")
                .setSource("/user/123#")
                .setId("567")
                .setTime("2020-07-13T09:15:12Z")
                .setDataContentType("application/nosj")
                .setData(Common.sampleBinaryData)
                .setDataSchema("http://json-schema.org/draft-07/schema#")
                .setSubject("SampleSubject")
                .put("compmstring", "string-value");
        blackhole.consume(event);
    }

    @Benchmark
    public void unsafeCreateEventWithStringData(Blackhole blackhole) {
        CloudEvent event = MutableCloudEventImpl.create()
                .putUnsafe("type", "OximeterMeasured")
                .putUnsafe("source", "/user/123#")
                .putUnsafe("id", "567")
                .putUnsafe("time", "2020-07-13T09:15:12Z")
                .putUnsafe("datacontenttype", "application/json")
                .putUnsafe("data", Common.sampleStringData)
                .putUnsafe("dataschema", "http://json-schema.org/draft-07/schema#")
                .putUnsafe("subject", "SampleSubject")
                .putUnsafe("compmstring", "string-value");
        blackhole.consume(event);
    }

    @Benchmark
    public void unsafeCreateEventWithBinaryData(Blackhole blackhole) {
        CloudEvent event = MutableCloudEventImpl.create()
                .putUnsafe("type", "OximeterMeasured")
                .putUnsafe("source", "/user/123#")
                .putUnsafe("id", "567")
                .putUnsafe("time", "2020-07-13T09:15:12Z")
                .putUnsafe("datacontenttype", "application/json")
                .putUnsafe("data", Common.sampleBinaryData)
                .putUnsafe("dataschema", "http://json-schema.org/draft-07/schema#")
                .putUnsafe("subject", "SampleSubject")
                .putUnsafe("compmstring", "string-value");
        blackhole.consume(event);
    }
}
