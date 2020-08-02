package com.particlemetrics.events.impl;

import com.particlemetrics.events.Event;
import com.particlemetrics.events.MutableEvent;
import com.particlemetrics.events.ValidationException;
import org.jetbrains.annotations.NotNull;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

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
    public void fromEvent(final Event event) {
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

    @Override
    public MutableEvent setData(@NotNull String data) {
        Objects.requireNonNull(data, "Data cannot be null");
        attributes.put(ATTRIBUTE_DATA, data.getBytes(StandardCharsets.UTF_8));
        _hasBinaryData = false;
        return this;
    }

    @Override
    public MutableEvent setData(@NotNull byte[] data) {
        Objects.requireNonNull(data, "Data cannot be null");
        attributes.put(ATTRIBUTE_DATA, data);
        _hasBinaryData = true;
        return this;
    }

    @Override
    public String getSpecVersion() {
        Object specVersion = attributes.get(ATTRIBUTE_SPEC_VERSION);
        if (specVersion == null) {
            return null;
        }
        return (String) specVersion;
    }

    @Override
    public String getType() {
        Object type = attributes.get(ATTRIBUTE_TYPE);
        if (type == null) {
            return null;
        }
        return (String) type;
    }

    @Override
    public String getSource() {
        Object source = attributes.get(ATTRIBUTE_SOURCE);
        if (source == null) {
            return null;
        }
        return (String) source;
    }

    @Override
    public String getId() {
        Object id = attributes.get(ATTRIBUTE_ID);
        if (id == null) {
            return null;
        }
        return (String) id;
    }

    @Override
    public String getTime() {
        Object time = attributes.get(ATTRIBUTE_TIME);
        if (time == null) {
            return null;
        }
        return (String) time;
    }

    @Override
    public long getTimeLong() {
        return 0;
    }

    @Override
    public String getDataContentType() {
        Object contentType = attributes.get(ATTRIBUTE_DATA_CONTENT_TYPE);
        if (contentType == null) {
            return null;
        }
        return (String) contentType;
    }

    @Override
    public String getDataSchema() {
        Object schema = attributes.get(ATTRIBUTE_DATA_SCHEMA);
        if (schema == null) {
            return null;
        }
        return (String) schema;
    }

    @Override
    public String getSubject() {
        Object subject = attributes.get(ATTRIBUTE_SUBJECT);
        return (subject == null) ? null : (String) subject;
    }

    @Override
    public byte[] getData() {
        Object data = attributes.get(ATTRIBUTE_DATA);
        if (data == null) {
            return null;
        }
        return (byte[]) data;
    }

    @Override
    public String getDataString() {
        Object data = attributes.get(ATTRIBUTE_DATA);
        if (data == null) {
            return null;
        }
        return new String((byte[]) data, StandardCharsets.UTF_8);
    }

    @Override
    public String toString() {
        return attributes.toString();
    }
}
