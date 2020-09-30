package io.scaleplan.spce;

import org.jetbrains.annotations.Nullable;

import java.util.Map;

public interface CloudEvent {
    String ATTRIBUTE_DATA = "data";
    String ATTRIBUTE_DATA_BASE64 = "data_base64";
    String ATTRIBUTE_DATA_CONTENT_TYPE = "datacontenttype";
    String ATTRIBUTE_DATA_SCHEMA = "dataschema";
    String ATTRIBUTE_ID = "id";
    String ATTRIBUTE_TIME = "time";
    String ATTRIBUTE_TYPE = "type";
    String ATTRIBUTE_SUBJECT = "subject";
    String ATTRIBUTE_SPEC_VERSION = "specversion";
    String ATTRIBUTE_SOURCE = "source";

//    /**
//     * Returns an immutable map view of the event attributes.
//     *
//     * @return an immutable Map.
//     */
//    Map<String, Object> asMap();

    /**
     * Returns an immutable map view of the event attributes, except data.
     *
     * @return an immutable Map.
     */
    Map<String, Object> getAttributes();

    /**
     * Returns whether the event data is binary.
     *
     * @return true or false
     */
    boolean hasBinaryData();

    // Required attributes

    String getId();

    String getSpecVersion();

    String getSource();

    String getType();

    // Optional attributes

    @Nullable byte[] getData();

    @Nullable String getDataContentType();

    @Nullable String getDataSchema();

    @Nullable String getSubject();

    @Nullable String getTime();

    // Extended attributes

    @Nullable Object getAttribute(String name);

    @Nullable String getStringAttribute(String name);

    @Nullable Integer getIntAttribute(String name);

    @Nullable Boolean getBoolAttribute(String name);
}
