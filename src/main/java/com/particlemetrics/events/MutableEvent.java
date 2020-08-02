package com.particlemetrics.events;

import org.jetbrains.annotations.NotNull;

public interface MutableEvent extends Event {
    MutableEvent reset();

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

    /**
     * Adds or replaces an extended attribute.
     * Cannot add or replace a required or optional attribute.
     *
     * @param name  Extended attribute name
     * @param value Extended attribute value
     * @return a MutableEvent
     * @throws IllegalArgumentException if name is a required or optional attribute.
     * @throws NullPointerException     if either name or value is null.
     */
    <T> MutableEvent put(@NotNull String name, @NotNull T value);

    /**
     * Adds or replaces an extended attribute.
     * Can add or replace a required or optional attribute.
     *
     * @param name  Extended attribute name
     * @param value Extended attribute value
     * @return a MutableEvent
     * @throws NullPointerException if either name or value is null.
     */
    <T> MutableEvent putUnsafe(@NotNull String name, @NotNull T value);

    MutableEvent remove(@NotNull String name);

}
