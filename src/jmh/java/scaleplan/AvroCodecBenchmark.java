package scaleplan;

import io.scaleplan.spce.codecs.Avro;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.infra.Blackhole;

import java.util.concurrent.TimeUnit;

@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Fork(value = 2)
@Warmup(iterations = 2)
public class AvroCodecBenchmark {
    private static final byte[] encodedAvro = Avro.encode(Common.sampleEvent);

    @Benchmark
    public void decodeEvent(Blackhole blackhole) {
        blackhole.consume(Avro.decode(encodedAvro));
    }

    @Benchmark
    public void encodeEvent(Blackhole blackhole) {
        blackhole.consume(Avro.encode(Common.sampleEvent));
    }
}
