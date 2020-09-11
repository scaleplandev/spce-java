package io.scaleplan.cloudevents;

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

    Map<String, Object> asMap();

    boolean hasBinaryData();

    // Required attributes

    @Nullable String getId();

    @Nullable String getSpecVersion();

    @Nullable String getSource();

    @Nullable String getType();

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
