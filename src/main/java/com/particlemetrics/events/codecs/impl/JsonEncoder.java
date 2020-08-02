package com.particlemetrics.events.codecs.impl;

import com.fasterxml.jackson.core.JsonEncoding;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import com.particlemetrics.events.Encoder;
import com.particlemetrics.events.Event;
import com.particlemetrics.events.codecs.EncodeException;
import org.jetbrains.annotations.NotNull;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.Map;

public class JsonEncoder implements Encoder {
    private static final Base64.Encoder base64Encoder = Base64.getEncoder();
    private final JsonFactory jsonFactory = new JsonFactory();

    public static JsonEncoder create() {
        return new JsonEncoder();
    }

    JsonEncoder() {

    }

    @Override
    public @NotNull byte[] encode(final @NotNull Event event) {
        try (ByteArrayOutputStream bos = new ByteArrayOutputStream()) {
            encode(event, bos);
            return bos.toByteArray();
        } catch (IOException e) {
            throw new EncodeException("Error encoding JSON", e);
        }
    }

    @Override
    public void encode(final @NotNull Event event, @NotNull OutputStream outputStream) {
        try (JsonGenerator generator = jsonFactory.createGenerator(outputStream, JsonEncoding.UTF8)) {
            generator.writeStartObject();

            for (Map.Entry<String, Object> attr : event.asMap().entrySet()) {
                String key = attr.getKey();
                Object value = attr.getValue();

                switch (key) {
                    // Write required attributes
                    case Event.ATTRIBUTE_SPEC_VERSION:
                    case Event.ATTRIBUTE_TYPE:
                    case Event.ATTRIBUTE_SOURCE:
                    case Event.ATTRIBUTE_ID:
                        // Write optional attributes
                    case Event.ATTRIBUTE_TIME:
                    case Event.ATTRIBUTE_SUBJECT:
                    case Event.ATTRIBUTE_DATA_CONTENT_TYPE:
                    case Event.ATTRIBUTE_DATA_SCHEMA:
                        generator.writeStringField(key, (String) value);
                        break;
                    case Event.ATTRIBUTE_DATA:
                        writeData(generator, event);
                        break;
                    default:
                        if (value instanceof String) generator.writeStringField(key, (String) value);
                        else if (value instanceof Integer) generator.writeNumberField(key, (Integer) value);
                        else if (value instanceof Boolean) generator.writeBooleanField(key, (Boolean) value);
                        else
                            throw new EncodeException(String.format("Cannot encode fields of type: %s", value.getClass().toString()));
                }
            }

            generator.writeEndObject();
        } catch (IOException e) {
            throw new EncodeException("Error encoding JSON", e);
        }

    }

    private void writeOptionalStringField(
            JsonGenerator generator,
            String fieldName,
            String fieldValue
    ) throws IOException {
        if (fieldValue != null) {
            generator.writeStringField(fieldName, fieldValue);
        }
    }

    private void writeData(final JsonGenerator generator, final Event event) throws IOException {
        byte[] data = event.getData();
        if (data != null) {
            if (event.hasBinaryData()) {
                generator.writeStringField(
                        Event.ATTRIBUTE_DATA_BASE64,
                        base64Encoder.encodeToString(data)
                );
            } else {
                generator.writeStringField(
                        Event.ATTRIBUTE_DATA,
                        new String(data, StandardCharsets.UTF_8)
                );
            }
        }
    }
}
