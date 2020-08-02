package com.particlemetrics.events;

import java.util.Map;

public interface Event {
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

    String MIME_JSON = "application/json";

    Map<String, Object> asMap();

    boolean hasBinaryData();

    // Required attributes
    String getId();

    String getSpecVersion();

    String getSource();

    String getType();

    // Optional attributes
    byte[] getData();

    String getDataString();

    String getDataContentType();

    String getDataSchema();

    String getSubject();

    String getTime();

    long getTimeLong();
}
