package io.scaleplan.spce.codecs;

import io.scaleplan.spce.CloudEvent;
import io.scaleplan.spce.codecs.avro.AvroSpceDecoder;
import io.scaleplan.spce.codecs.avro.AvroSpceEncoder;
import org.jetbrains.annotations.NotNull;

public class AvroSpce {
    public static @NotNull byte[] encode(final @NotNull CloudEvent event) {
        return AvroSpce.EncoderHolder.instance.encode(event);
    }

    public static @NotNull CloudEvent decode(@NotNull byte[] data) {
        return AvroSpce.DecoderHolder.instance.decode(data);
    }

    private static class EncoderHolder {
        private static final AvroSpceEncoder instance = AvroSpceEncoder.create();
    }

    private static class DecoderHolder {
        private static final AvroSpceDecoder instance = AvroSpceDecoder.create();
    }

}
