package io.scaleplan.spce.codecs;

import io.scaleplan.spce.CloudEvent;
import io.scaleplan.spce.codecs.impl.JsonDecoder;
import io.scaleplan.spce.codecs.impl.JsonEncoder;
import org.jetbrains.annotations.NotNull;

import java.io.OutputStream;
import java.util.Collection;
import java.util.List;

public class Json {
    public static @NotNull byte[] encode(final @NotNull CloudEvent event) {
        return EncoderHolder.instance.encode(event);
    }

    public static void encode(@NotNull final CloudEvent event, @NotNull final OutputStream outputStream) {
        EncoderHolder.instance.encode(event, outputStream);
    }

    public static @NotNull byte[] encode(@NotNull final Collection<CloudEvent> events) {
        return EncoderHolder.instance.encode(events);
    }

    public static void encode(@NotNull final Collection<CloudEvent> events, @NotNull final OutputStream outputStream) {
        EncoderHolder.instance.encode(events, outputStream);
    }

    public static @NotNull CloudEvent decode(@NotNull byte[] data) {
        return DecoderHolder.instance.decode(data);
    }

    public static @NotNull List<CloudEvent> decodeArray(@NotNull byte[] data) {
        return DecoderHolder.instance.decodeArray(data);
    }

    public static @NotNull DecodeIterator arrayDecoder(@NotNull byte[] data) {
        return DecoderHolder.instance.arrayDecoder(data);
    }

    private static class DecoderHolder {
        private static final JsonDecoder instance = JsonDecoder.create();
    }

    private static class EncoderHolder {
        private static final JsonEncoder instance = JsonEncoder.create();
    }

    private Json() {
    }
}
