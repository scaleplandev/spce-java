package io.scaleplan.spce.codecs;

import io.scaleplan.spce.CloudEvent;
import io.scaleplan.spce.codecs.avro.AvroDecoder;
import io.scaleplan.spce.codecs.avro.AvroEncoder;
import org.jetbrains.annotations.NotNull;

public class Avro {
    public static @NotNull byte[] encode(final @NotNull CloudEvent event) {
        return EncoderHolder.instance.encode(event);
    }

    public static @NotNull CloudEvent decode(@NotNull byte[] data) {
        return DecoderHolder.instance.decode(data);
    }

    private static class EncoderHolder {
        private static final AvroEncoder instance = AvroEncoder.create();
    }

    private static class DecoderHolder {
        private static final AvroDecoder instance = AvroDecoder.create();
    }
}
