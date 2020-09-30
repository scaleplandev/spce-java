package io.scaleplan.spce.codecs;

import io.scaleplan.spce.CloudEvent;
import org.jetbrains.annotations.NotNull;

import java.util.List;

public interface Decoder {
    @NotNull CloudEvent decode(@NotNull byte[] data);

    @NotNull List<CloudEvent> decodeArray(@NotNull byte[] data);

    @NotNull DecodeIterator arrayDecoder(@NotNull byte[] data);
}
