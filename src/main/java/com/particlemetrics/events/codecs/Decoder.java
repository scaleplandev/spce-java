package com.particlemetrics.events.codecs;

import com.particlemetrics.events.Event;
import org.jetbrains.annotations.NotNull;

public interface Decoder {
    @NotNull Event decode(@NotNull byte[] data);
}
