package io.scaleplan.spce.codecs.impl;

import com.fasterxml.jackson.core.JsonEncoding;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import io.scaleplan.spce.CloudEvent;
import io.scaleplan.spce.codecs.EncodeException;
import io.scaleplan.spce.codecs.Encoder;
import org.jetbrains.annotations.NotNull;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.Collection;
import java.util.Map;
import java.util.Objects;

public class JsonEncoder implements Encoder {
    private static final Base64.Encoder base64Encoder = Base64.getEncoder();
    private final JsonFactory jsonFactory = new JsonFactory();

    public static JsonEncoder create() {
        return new JsonEncoder();
    }

    JsonEncoder() {

    }

    @Override
    public @NotNull byte[] encode(@NotNull final CloudEvent event) {
        try (ByteArrayOutputStream bos = new ByteArrayOutputStream()) {
            encode(event, bos);
            return bos.toByteArray();
        } catch (IOException e) {
            throw new EncodeException("Error encoding JSON", e);
        }
    }

    @Override
    public void encode(@NotNull final CloudEvent event, @NotNull final OutputStream outputStream) {
        Objects.requireNonNull(outputStream);
        try (final JsonGenerator generator = jsonFactory.createGenerator(outputStream, JsonEncoding.UTF8)) {
            encode(event, generator);
        } catch (IOException e) {
            throw new EncodeException("Cannot encode event to JSON", e);
        }
    }

    @Override
    public byte[] encode(@NotNull final Collection<CloudEvent> events) {
        try (ByteArrayOutputStream bos = new ByteArrayOutputStream()) {
            encode(events, bos);
            return bos.toByteArray();
        } catch (IOException e) {
            throw new EncodeException("Error encoding JSON", e);
        }

    }

    @Override
    public void encode(@NotNull final Collection<CloudEvent> events, @NotNull final OutputStream outputStream) {
        Objects.requireNonNull(events);
        Objects.requireNonNull(outputStream);
        try (final JsonGenerator generator = jsonFactory.createGenerator(outputStream, JsonEncoding.UTF8)) {
            generator.writeStartArray(events.size());
            for (CloudEvent event : events) {
                encode(event, generator);
            }
            generator.writeEndArray();
        } catch (IOException e) {
            throw new EncodeException("Cannot encode event to JSON", e);
        }
    }

    protected void encode(
            @NotNull final CloudEvent event,
            @NotNull final JsonGenerator generator
    ) throws IOException {
        Objects.requireNonNull(event);
        Objects.requireNonNull(generator);
        generator.writeStartObject();

        for (Map.Entry<String, Object> attr : event.getAttributes().entrySet()) {
            String key = attr.getKey();
            Object value = attr.getValue();

            switch (key) {
                // Write required attributes
                case CloudEvent.ATTRIBUTE_SPEC_VERSION:
                case CloudEvent.ATTRIBUTE_TYPE:
                case CloudEvent.ATTRIBUTE_SOURCE:
                case CloudEvent.ATTRIBUTE_ID:
                    // Write optional attributes
                case CloudEvent.ATTRIBUTE_TIME:
                case CloudEvent.ATTRIBUTE_SUBJECT:
                case CloudEvent.ATTRIBUTE_DATA_CONTENT_TYPE:
                case CloudEvent.ATTRIBUTE_DATA_SCHEMA:
                    generator.writeStringField(key, (String) value);
                    break;
                default:
                    if (value instanceof String) generator.writeStringField(key, (String) value);
                    else if (value instanceof Integer) generator.writeNumberField(key, (Integer) value);
                    else if (value instanceof Boolean) generator.writeBooleanField(key, (Boolean) value);
                    else
                        throw new EncodeException(String.format("Cannot encode fields of type: %s", value.getClass().toString()));
            }
        }

        byte[] data = event.getData();
        if (data != null) {
            writeData(generator, event);
        }

        generator.writeEndObject();
    }

    private void writeData(final JsonGenerator generator, final CloudEvent event) throws IOException {
        byte[] data = event.getData();
        if (data != null) {
            if (event.hasBinaryData()) {
                generator.writeStringField(
                        CloudEvent.ATTRIBUTE_DATA_BASE64,
                        base64Encoder.encodeToString(data)
                );
            } else {
                generator.writeStringField(
                        CloudEvent.ATTRIBUTE_DATA,
                        new String(data, StandardCharsets.UTF_8)
                );
            }
        }
    }
}
