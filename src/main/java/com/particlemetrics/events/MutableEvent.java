package com.particlemetrics.events;

import org.jetbrains.annotations.NotNull;

public interface MutableEvent extends Event {
    MutableEvent reset();

    void fromEvent(@NotNull Event event);

    MutableEvent removeAttribute(@NotNull String name);

    // Required attributes

    MutableEvent setSpecVersion(@NotNull String specVersion);

    MutableEvent setId(@NotNull String id);

    MutableEvent setSource(@NotNull String source);

    MutableEvent setType(@NotNull String type);

    // Optional attributes

    MutableEvent setData(@NotNull String data);

    MutableEvent setData(@NotNull byte[] data);

    MutableEvent setDataUnsafe(@NotNull byte[] data);

    MutableEvent setDataContentType(@NotNull String contentType);

    MutableEvent setDataSchema(@NotNull String dataSchema);

    MutableEvent setSubject(@NotNull String subject);

    MutableEvent setTime(@NotNull String time);

    MutableEvent setTime(long milliseconds);

    MutableEvent setTimeNow();

    // Extended attributes

    <T> MutableEvent setAttribute(String key, @NotNull T value);
}
