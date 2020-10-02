package scaleplan;

import io.scaleplan.spce.codecs.AvroSpce;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.infra.Blackhole;

import java.util.concurrent.TimeUnit;

@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Fork(value = 2)
@Warmup(iterations = 2)
public class AvroSpceCodecBenchmark {
    private static final byte[] encodedAvroSpce = AvroSpce.encode(Common.sampleEvent);
    private static final byte[] bundleAvroSpce = AvroSpce.encode(Common.sampleEventBundle);

    @Benchmark
    public void decodeEvent(Blackhole blackhole) {
        blackhole.consume(AvroSpce.decode(encodedAvroSpce));
    }

    @Benchmark
    public void encodeEvent(Blackhole blackhole) {
        blackhole.consume(AvroSpce.encode(Common.sampleEvent));
    }

    @Benchmark
    public void decodeEventBundle(Blackhole blackhole) {
        blackhole.consume(AvroSpce.decodeBatch(bundleAvroSpce));
    }

    @Benchmark
    public void encodeEventBundle(Blackhole blackhole) {
        blackhole.consume(AvroSpce.encode(Common.sampleEventBundle));
    }

}
