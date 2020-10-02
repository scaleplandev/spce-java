package scaleplan;

import io.scaleplan.spce.codecs.Json;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.infra.Blackhole;

import java.util.concurrent.TimeUnit;

@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Fork(value = 2)
@Warmup(iterations = 2)
public class JsonCodecBenchmark {
    private static final byte[] encodedJson = Json.encode(Common.sampleEvent);
    private static final byte[] encodedJsonBundle = Json.encode(Common.sampleEventBundle);

    @Benchmark
    public void decodeEvent(Blackhole blackhole) {
        blackhole.consume(Json.decode(encodedJson));
    }

    @Benchmark
    public void encodeEvent(Blackhole blackhole) {
        blackhole.consume(Json.encode(Common.sampleEvent));
    }

    @Benchmark
    public void decodeEventBundle(Blackhole blackhole) {
        blackhole.consume(Json.decodeBatch(encodedJsonBundle));
    }

    @Benchmark
    public void encodeEventBundle(Blackhole blackhole) {
        blackhole.consume(Json.encode(Common.sampleEventBundle));
    }
}
