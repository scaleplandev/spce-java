package com.particlemetrics.events.codecs;

import com.particlemetrics.events.Event;
import org.jetbrains.annotations.NotNull;

import java.io.OutputStream;
import java.util.Collection;

public interface Encoder {
    @NotNull byte[] encode(final @NotNull Event event);

    void encode(@NotNull final Event event, @NotNull final OutputStream outputStream);

    byte[] encode(@NotNull final Collection<Event> events);

    void encode(@NotNull final Collection<Event> events, @NotNull final OutputStream outputStream);
}
