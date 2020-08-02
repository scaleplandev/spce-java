package com.particlemetrics.events.codecs.impl;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.particlemetrics.events.Event;
import com.particlemetrics.events.MutableEvent;
import com.particlemetrics.events.codecs.DecodeException;
import com.particlemetrics.events.codecs.Decoder;
import com.particlemetrics.events.impl.MutableEventImpl;
import org.jetbrains.annotations.NotNull;

import java.io.IOException;
import java.util.Base64;

public class JsonDecoder implements Decoder {
    private static final Base64.Decoder base64Decoder = Base64.getDecoder();
    private final JsonFactory jsonFactory = new JsonFactory();

    public static JsonDecoder create() {
        return new JsonDecoder();
    }

    JsonDecoder() {
    }

    @Override
    public @NotNull Event decode(@NotNull byte[] data) {
        return decode(data, true);
    }

    public @NotNull Event decode(@NotNull byte[] data, boolean validate) {
        MutableEvent event = MutableEventImpl.create();
        try (final JsonParser parser = jsonFactory.createParser(data)) {
            boolean started = false;
            String fieldName = "";
            while (!parser.isClosed()) {
                JsonToken token = parser.nextToken();
                if (token == null) {
                    break;
                }
                switch (token) {
                    case FIELD_NAME:
                        fieldName = parser.getValueAsString();
                        break;
                    case VALUE_STRING:
                        updateEventStringAttribute(event, fieldName, parser.getValueAsString());
                        break;
                    case VALUE_NUMBER_INT:
                        updateEventAttribute(event, fieldName, parser.getIntValue());
                        break;
                    case VALUE_TRUE:
                        updateEventAttribute(event, fieldName, true);
                        break;
                    case VALUE_FALSE:
                        updateEventAttribute(event, fieldName, false);
                        break;
                    case START_OBJECT:
                        if (started) throw new DecodeException("Unexpected START_OBJECT");
                        started = true;
                        break;
                    case NOT_AVAILABLE:
                        // pass
                        break;
                    case END_OBJECT:
                        // pass
                        break;
                    case START_ARRAY:
                        throw new DecodeException("Unexpected START_ARRAY");
                    case END_ARRAY:
                        throw new DecodeException("Unexpected END_ARRAY");
                    case VALUE_EMBEDDED_OBJECT:
                        throw new DecodeException("Unexpected VALUE_EMBEDDED_OBJECT");
                    case VALUE_NUMBER_FLOAT:
                        throw new DecodeException("Unexpected VALUE_NUMBER_FLOAT");
                    case VALUE_NULL:
                        throw new DecodeException("Unexpected VALUE_NUMBER_NULL");
                }
            }
        } catch (IOException e) {
            throw new DecodeException("Error decoding JSON", e);
        }
        if (validate) {
            ((MutableEventImpl) event).validate();
        }
        return event;
    }

    private void updateEventStringAttribute(MutableEvent event, String fieldName, String fieldValue) {
        switch (fieldName) {
            // Required attributes
            case Event.ATTRIBUTE_SPEC_VERSION:
                event.setSpecVersion(fieldValue);
                break;
            case Event.ATTRIBUTE_ID:
                event.setId(fieldValue);
                break;
            // TODO: check URI-ref
            case Event.ATTRIBUTE_SOURCE:
                event.setSource(fieldValue);
                break;
            case Event.ATTRIBUTE_TYPE:
                event.setType(fieldValue);
                break;
            // Optional attributes
            // TODO: check timestamp
            case Event.ATTRIBUTE_TIME:
                event.setTime(fieldValue);
                break;
            case Event.ATTRIBUTE_SUBJECT:
                event.setSubject(fieldValue);
                break;
            case Event.ATTRIBUTE_DATA:
                event.setData(fieldValue);
                break;
            case Event.ATTRIBUTE_DATA_BASE64:
                event.setDataUnsafe(base64Decoder.decode(fieldValue));
                break;
            case Event.ATTRIBUTE_DATA_CONTENT_TYPE:
                event.setDataContentType(fieldValue);
                break;
            case Event.ATTRIBUTE_DATA_SCHEMA:
                event.setDataSchema(fieldValue);
                break;
            default:
                event.setAttribute(fieldName, fieldValue);
        }
    }

    private <T> void updateEventAttribute(MutableEvent event, String fieldName, T fieldValue) {
        switch (fieldName) {
            // Required attributes
            case Event.ATTRIBUTE_SPEC_VERSION:
                throw new DecodeException(String.format("%s must be a string", Event.ATTRIBUTE_SPEC_VERSION));
            case Event.ATTRIBUTE_ID:
                throw new DecodeException(String.format("%s must be a string", Event.ATTRIBUTE_ID));
            case Event.ATTRIBUTE_SOURCE:
                throw new DecodeException(String.format("%s must be a string", Event.ATTRIBUTE_SOURCE));
            case Event.ATTRIBUTE_TYPE:
                throw new DecodeException(String.format("%s must be a string", Event.ATTRIBUTE_TYPE));
                // Optional attributes
            case Event.ATTRIBUTE_TIME:
                throw new DecodeException(String.format("%s must be a string", Event.ATTRIBUTE_TIME));
            case Event.ATTRIBUTE_SUBJECT:
                throw new DecodeException(String.format("%s must be a string", Event.ATTRIBUTE_SUBJECT));
            case Event.ATTRIBUTE_DATA:
                throw new DecodeException(String.format("%s must be a string", Event.ATTRIBUTE_DATA));
            case Event.ATTRIBUTE_DATA_BASE64:
                throw new DecodeException(String.format("%s must be a string", Event.ATTRIBUTE_DATA_BASE64));
            case Event.ATTRIBUTE_DATA_CONTENT_TYPE:
                throw new DecodeException(String.format("%s must be a string", Event.ATTRIBUTE_DATA_CONTENT_TYPE));
            case Event.ATTRIBUTE_DATA_SCHEMA:
                throw new DecodeException(String.format("%s must be a string", Event.ATTRIBUTE_DATA_SCHEMA));
            default:
                event.setAttribute(fieldName, fieldValue);
        }
    }
}
