package com.particlemetrics.events;

import org.jetbrains.annotations.NotNull;

import java.io.OutputStream;

public interface Encoder {
    @NotNull byte[] encode(final @NotNull Event event);

    void encode(final @NotNull Event event, @NotNull OutputStream outputStream);
}
