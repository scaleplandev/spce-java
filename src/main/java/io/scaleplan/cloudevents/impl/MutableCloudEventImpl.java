package io.scaleplan.cloudevents.impl;

import io.scaleplan.cloudevents.CloudEvent;
import io.scaleplan.cloudevents.MutableCloudEvent;
import io.scaleplan.cloudevents.ValidationException;
import io.scaleplan.cloudevents.validators.Validators;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.*;

public class MutableCloudEventImpl implements MutableCloudEvent {
    private static final String SPEC_VERSION = "1.0";

    private final Map<String, Object> attributes;
    private boolean _hasBinaryData;

    public static MutableCloudEvent create(@NotNull String type, @NotNull String source, @NotNull String id) {
        return new MutableCloudEventImpl()
                .setSpecVersion(SPEC_VERSION)
                .setType(Objects.requireNonNull(type, "type is cannot be null"))
                .setSource(Validators.requireValidURIRef(source, "source must be a valid URI reference"))
                .setId(Objects.requireNonNull(id, "id cannot be null"));
    }

    /**
     * Creates a MutableEvent with no required fields set except specversion which is set to 1.0.
     * Warning: Required fields, type and id must always be set.
     *
     * @return a new MutableEvent object
     */
    public static MutableCloudEvent create() {
        return new MutableCloudEventImpl();
    }

    public static MutableCloudEvent wrapUnsafe(@NotNull final Map<String, Object> attributes) {
        Objects.requireNonNull(attributes, "attributes cannot be null");
        boolean hasBinaryData = attributes.containsKey(CloudEvent.ATTRIBUTE_DATA_BASE64);
        return new MutableCloudEventImpl(attributes, hasBinaryData);
    }

    MutableCloudEventImpl() {
        this(new HashMap<>(4), false);
    }

    MutableCloudEventImpl(Map<String, Object> attributes, boolean hasBinaryData) {
        this.attributes = attributes;
        this._hasBinaryData = hasBinaryData;
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
    public MutableCloudEvent reset() {
        attributes.clear();
        return this;
    }

    @Override
    public boolean hasBinaryData() {
        return _hasBinaryData;
    }

    @Override
    public MutableCloudEvent remove(@NotNull String name) {
        Objects.requireNonNull(name, "name cannot be null");
        // Do not allow removing required attributes
        switch (name) {
            case CloudEvent.ATTRIBUTE_SPEC_VERSION:
            case CloudEvent.ATTRIBUTE_TYPE:
            case CloudEvent.ATTRIBUTE_SOURCE:
            case CloudEvent.ATTRIBUTE_ID:
                throw new IllegalArgumentException(
                        String.format("%s is a required attribute. It can't be removed.", name)
                );
            default:
                this.attributes.remove(name);
        }
        return this;
    }

    @Override
    public <T> MutableCloudEvent put(@NotNull String name, @NotNull T value) {
        Objects.requireNonNull(name, "name cannot be null");
        Objects.requireNonNull(value, "value cannot be null");
        // Do not allow putting required or optional attributes
        switch (name) {
            case CloudEvent.ATTRIBUTE_SPEC_VERSION:
            case CloudEvent.ATTRIBUTE_TYPE:
            case CloudEvent.ATTRIBUTE_SOURCE:
            case CloudEvent.ATTRIBUTE_ID:
            case CloudEvent.ATTRIBUTE_TIME:
            case CloudEvent.ATTRIBUTE_SUBJECT:
            case CloudEvent.ATTRIBUTE_DATA_CONTENT_TYPE:
            case CloudEvent.ATTRIBUTE_DATA_SCHEMA:
            case CloudEvent.ATTRIBUTE_DATA:
                throw new IllegalArgumentException(
                        String.format("%s is a required or optional attribute. It can't be replaced.", name)
                );
            default:
                attributes.put(name, value);
        }
        return this;
    }

    @Override
    public <T> MutableCloudEvent putUnsafe(@NotNull String key, @NotNull T value) {
        attributes.put(key, value);
        return this;
    }

    @Override
    public MutableCloudEvent setSpecVersion(@NotNull String specVersion) {
        attributes.put(ATTRIBUTE_SPEC_VERSION, Objects.requireNonNull(specVersion));
        return this;
    }

    @Override
    public MutableCloudEvent setType(@NotNull String type) {
        attributes.put(ATTRIBUTE_TYPE, Objects.requireNonNull(type));
        return this;
    }

    @Override
    public MutableCloudEvent setSource(@NotNull String source) {
        attributes.put(ATTRIBUTE_SOURCE, Validators.requireValidURIRef(source));
        return this;
    }

    @Override
    public MutableCloudEvent setId(@NotNull String id) {
        attributes.put(ATTRIBUTE_ID, Objects.requireNonNull(id));
        return this;
    }

    @Override
    public MutableCloudEvent setTime(@NotNull String time) {
        attributes.put(CloudEvent.ATTRIBUTE_TIME, Validators.requireValidTimestamp(time));
        return this;
    }

    @Override
    public MutableCloudEvent setTime(long milliseconds) {
        // TODO: timezone stuff
        String timeStr = Instant.ofEpochMilli(milliseconds).toString();
        attributes.put(CloudEvent.ATTRIBUTE_TIME, timeStr);
        return this;
    }

    @Override
    public MutableCloudEvent setTimeNow() {
        setTime(System.currentTimeMillis());
        return this;
    }

    @Override
    public MutableCloudEvent setDataContentType(@NotNull String contentType) {
        Objects.requireNonNull(contentType, "DataContentType cannot be null");
        attributes.put(ATTRIBUTE_DATA_CONTENT_TYPE, contentType);
        return this;
    }

    @Override
    public MutableCloudEvent setDataSchema(@NotNull String dataSchema) {
        Validators.requireValidURI(dataSchema, "DataSchema must be valid");
        attributes.put(ATTRIBUTE_DATA_SCHEMA, dataSchema);
        return this;
    }

    @Override
    public MutableCloudEvent setSubject(@NotNull String subject) {
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
    public MutableCloudEvent setData(@NotNull byte[] data) {
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
    public MutableCloudEvent setData(@NotNull String data) {
        Objects.requireNonNull(data, "Data cannot be null");
        attributes.put(ATTRIBUTE_DATA, data.getBytes(StandardCharsets.UTF_8));
        _hasBinaryData = false;
        return this;
    }

    @Override
    public MutableCloudEvent setDataUnsafe(@NotNull byte[] data) {
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
    public String toString() {
        return attributes.toString();
    }
}
