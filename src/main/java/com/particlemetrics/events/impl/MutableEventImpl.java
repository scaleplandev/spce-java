package com.particlemetrics.events.impl;

import com.particlemetrics.events.Event;
import com.particlemetrics.events.MutableEvent;
import com.particlemetrics.events.ValidationException;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.*;

public class MutableEventImpl implements MutableEvent {
    private static final String SPEC_VERSION = "1.0";

    private final Map<String, Object> attributes = new HashMap<>(10);
    private boolean _hasBinaryData = false;

    public static MutableEvent create(@NotNull String type, @NotNull String source, @NotNull String id) {
        return new MutableEventImpl()
                .setSpecVersion(SPEC_VERSION)
                .setType(Objects.requireNonNull(type, "type is cannot be null"))
                .setSource(Objects.requireNonNull(source, "source cannot be null"))
                .setId(Objects.requireNonNull(id, "id cannot be null"));
    }

    /**
     * Creates a MutableEvent with no required fields set except specversion which is set to 1.0.
     * Warning: Required fields, type and id must always be set.
     *
     * @return a new MutableEvent object
     */
    public static MutableEvent create() {
        return new MutableEventImpl();
    }

    MutableEventImpl() {
    }

    public void validate() {
        if (attributes.get(ATTRIBUTE_SPEC_VERSION) == null) {
            throw new ValidationException(String.format("%s must exist", ATTRIBUTE_SPEC_VERSION));
        }
        if (attributes.get(ATTRIBUTE_TYPE) == null) {
            throw new ValidationException(String.format("%s must exist", ATTRIBUTE_TYPE));
        }
        if (attributes.get(ATTRIBUTE_ID) == null) {
            throw new ValidationException(String.format("%s must exist", ATTRIBUTE_ID));
        }
        if (attributes.get(ATTRIBUTE_SOURCE) == null) {
            throw new ValidationException(String.format("%s must exist", ATTRIBUTE_SOURCE));
        }
    }

    @Override
    public Map<String, Object> asMap() {
        return Collections.unmodifiableMap(attributes);
    }

    @Override
    public MutableEvent reset() {
        attributes.clear();
//        attributes.put(ATTRIBUTE_SPEC_VERSION, SPEC_VERSION);
        return this;
    }

    @Override
    public boolean hasBinaryData() {
        return _hasBinaryData;
    }

    @Override
    public void fromEvent(final @NotNull Event event) {
        reset();
        this.attributes.putAll(event.asMap());
        this._hasBinaryData = event.hasBinaryData();
    }

    @Override
    public MutableEvent setSpecVersion(@NotNull String specVersion) {
        attributes.put(ATTRIBUTE_SPEC_VERSION, Objects.requireNonNull(specVersion));
        return this;
    }

    @Override
    public MutableEvent setType(@NotNull String type) {
        attributes.put(ATTRIBUTE_TYPE, Objects.requireNonNull(type));
        return this;
    }

    @Override
    public MutableEvent setSource(@NotNull String source) {
        attributes.put(ATTRIBUTE_SOURCE, Objects.requireNonNull(source));
        return this;
    }

    @Override
    public MutableEvent setId(@NotNull String id) {
        attributes.put(ATTRIBUTE_ID, Objects.requireNonNull(id));
        return this;
    }

    @Override
    public MutableEvent setTime(@NotNull String time) {
        // TODO: validate time
        attributes.put(Event.ATTRIBUTE_TIME, time);
        return this;
    }

    @Override
    public MutableEvent setTime(long milliseconds) {
        // TODO: timezone stuff
        String timeStr = Instant.ofEpochMilli(milliseconds).toString();
        attributes.put(Event.ATTRIBUTE_TIME, timeStr);
        return this;
    }

    @Override
    public MutableEvent setTimeNow() {
        setTime(System.currentTimeMillis());
        return this;
    }

    @Override
    public <T> MutableEvent setAttribute(String key, @NotNull T value) {
        Objects.requireNonNull(value, "Attribute cannot be null");
        attributes.put(key, value);
        return this;
    }

    @Override
    public MutableEvent setDataContentType(@NotNull String contentType) {
        Objects.requireNonNull(contentType, "DataContentType cannot be null");
        attributes.put(ATTRIBUTE_DATA_CONTENT_TYPE, contentType);
        return this;
    }

    @Override
    public MutableEvent setDataSchema(@NotNull String dataSchema) {
        Objects.requireNonNull(dataSchema, "DataSchema cannot be null");
        attributes.put(ATTRIBUTE_DATA_SCHEMA, dataSchema);
        return this;
    }

    @Override
    public MutableEvent setSubject(@NotNull String subject) {
        Objects.requireNonNull(subject, "Subject cannot be null");
        attributes.put(ATTRIBUTE_SUBJECT, subject);
        return this;
    }

    /**
     * Sets the data attribute of the event.
     * Creates a copy.
     *
     * @param data Non-null data. The data is copied.
     * @return a mutable event
     */
    @Override
    public MutableEvent setData(@NotNull byte[] data) {
        Objects.requireNonNull(data, "Data cannot be null");
        attributes.put(ATTRIBUTE_DATA, Arrays.copyOf(data, data.length));
        _hasBinaryData = true;
        return this;
    }

    /**
     * Sets the data attribute of the event.
     * DOES NOT create a copy. Do not change the data after.
     *
     * @param data Non-null data.
     * @return a mutable event
     */
    @Override
    public MutableEvent setData(@NotNull String data) {
        Objects.requireNonNull(data, "Data cannot be null");
        attributes.put(ATTRIBUTE_DATA, data.getBytes(StandardCharsets.UTF_8));
        _hasBinaryData = false;
        return this;
    }

    @Override
    public MutableEvent setDataUnsafe(@NotNull byte[] data) {
        Objects.requireNonNull(data, "Data cannot be null");
        attributes.put(ATTRIBUTE_DATA, data);
        _hasBinaryData = true;
        return this;
    }

    @Override
    public @Nullable String getSpecVersion() {
        Object specVersion = attributes.get(ATTRIBUTE_SPEC_VERSION);
        return (specVersion == null) ? null : (String) specVersion;
    }

    @Override
    public @Nullable String getType() {
        Object type = attributes.get(ATTRIBUTE_TYPE);
        return (type == null) ? null : (String) type;
    }

    @Override
    public @Nullable String getSource() {
        Object source = attributes.get(ATTRIBUTE_SOURCE);
        return (source == null) ? null : (String) source;
    }

    @Override
    public @Nullable String getId() {
        Object id = attributes.get(ATTRIBUTE_ID);
        return (id == null) ? null : (String) id;
    }

    @Override
    public @Nullable String getTime() {
        Object time = attributes.get(ATTRIBUTE_TIME);
        return (time == null) ? null : (String) time;
    }

    @Override
    public @Nullable Object getAttribute(String name) {
        return attributes.get(name);
    }

    @Override
    public @Nullable String getStringAttribute(String name) {
        // TODO: Decide whether to throw an exception on class cast error.
        Object obj = attributes.get(name);
        return (obj instanceof String) ? (String) obj : null;
    }

    @Override
    public @Nullable Integer getIntAttribute(String name) {
        // TODO: Decide whether to throw an exception on class cast error.
        Object obj = attributes.get(name);
        return (obj instanceof Integer) ? (Integer) obj : null;
    }

    @Override
    public @Nullable Boolean getBoolAttribute(String name) {
        // TODO: Decide whether to throw an exception on class cast error.
        Object obj = attributes.get(name);
        return (obj instanceof Boolean) ? (Boolean) obj : null;
    }

    @Override
    public @Nullable String getDataContentType() {
        Object contentType = attributes.get(ATTRIBUTE_DATA_CONTENT_TYPE);
        return (contentType == null) ? null : (String) contentType;
    }

    @Override
    public @Nullable String getDataSchema() {
        Object schema = attributes.get(ATTRIBUTE_DATA_SCHEMA);
        return (schema == null) ? null : (String) schema;
    }

    @Override
    public @Nullable String getSubject() {
        Object subject = attributes.get(ATTRIBUTE_SUBJECT);
        return (subject == null) ? null : (String) subject;
    }

    @Override
    public @Nullable byte[] getData() {
        Object data = attributes.get(ATTRIBUTE_DATA);
        return (data == null) ? null : (byte[]) data;
    }

    @Override
    public @Nullable String getDataString() {
        Object data = attributes.get(ATTRIBUTE_DATA);
        return (data == null) ? null : new String((byte[]) data, StandardCharsets.UTF_8);
    }

    @Override
    public String toString() {
        return attributes.toString();
    }
}
