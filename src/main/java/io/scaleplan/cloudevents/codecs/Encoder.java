package io.scaleplan.cloudevents.codecs;

import io.scaleplan.cloudevents.CloudEvent;
import org.jetbrains.annotations.NotNull;

import java.io.OutputStream;
import java.util.Collection;

public interface Encoder {
    @NotNull byte[] encode(final @NotNull CloudEvent event);

    void encode(@NotNull final CloudEvent event, @NotNull final OutputStream outputStream);

    byte[] encode(@NotNull final Collection<CloudEvent> events);

    void encode(@NotNull final Collection<CloudEvent> events, @NotNull final OutputStream outputStream);
}