package com.particlemetrics.events.codecs;

import com.particlemetrics.events.Event;
import org.jetbrains.annotations.NotNull;

import java.util.List;

public interface Decoder {
    @NotNull Event decode(@NotNull byte[] data);

    @NotNull List<Event> decodeArray(@NotNull byte[] data);

    @NotNull DecodeIterator arrayDecoder(@NotNull byte[] data);
}
