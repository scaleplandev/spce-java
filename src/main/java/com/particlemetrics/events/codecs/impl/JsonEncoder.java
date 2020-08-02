package com.particlemetrics.events.codecs.impl;

import com.fasterxml.jackson.core.JsonEncoding;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import com.particlemetrics.events.Encoder;
import com.particlemetrics.events.Event;
import com.particlemetrics.events.codecs.EncoderException;
import org.jetbrains.annotations.NotNull;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Base64;

public class JsonEncoder implements Encoder {
    private static final Base64.Encoder base64Encoder = Base64.getEncoder();
    private final JsonFactory jsonFactory = new JsonFactory();

    public static JsonEncoder create() {
        return new JsonEncoder();
    }

    JsonEncoder() {

    }

    @Override
    public byte[] encode(final @NotNull Event event) {
        try (ByteArrayOutputStream bos = new ByteArrayOutputStream()) {
            encode(event, bos);
            return bos.toByteArray();
        } catch (IOException e) {
            throw new EncoderException("Error encoding JSON", e);
        }
    }

    @Override
    public void encode(final @NotNull Event event, @NotNull OutputStream outputStream) {
        try (JsonGenerator generator = jsonFactory.createGenerator(outputStream, JsonEncoding.UTF8)) {
            generator.writeStartObject();

            // Write required attributes
            generator.writeStringField(Event.ATTRIBUTE_SPEC_VERSION, event.getSpecVersion());
            generator.writeStringField(Event.ATTRIBUTE_TYPE, event.getType());
            generator.writeStringField(Event.ATTRIBUTE_SOURCE, event.getSource());
            generator.writeStringField(Event.ATTRIBUTE_ID, event.getId());
            // Write optional attributes
            writeOptionalStringField(generator, Event.ATTRIBUTE_TIME, event.getTime());
            writeOptionalStringField(generator, Event.ATTRIBUTE_SUBJECT, event.getSubject());
            writeOptionalStringField(generator, Event.ATTRIBUTE_DATA_CONTENT_TYPE, event.getDataContentType());
            writeOptionalStringField(generator, Event.ATTRIBUTE_DATA_SCHEMA, event.getDataSchema());
            // Write the data
            writeData(generator, event);

            generator.writeEndObject();
        } catch (IOException e) {
            throw new EncoderException("Error encoding JSON", e);
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
        if (event.hasBinaryData()) {
            byte[] data = event.getData();
            if (data != null) {
                generator.writeStringField(Event.ATTRIBUTE_DATA_BASE64, base64Encoder.encodeToString(data));
            }
        } else {
            String data = event.getDataString();
            if (data != null) {
                generator.writeStringField(Event.ATTRIBUTE_DATA, data);
            }
        }
    }
}
