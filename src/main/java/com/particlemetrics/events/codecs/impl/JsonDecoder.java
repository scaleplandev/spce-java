package com.particlemetrics.events.codecs.impl;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.particlemetrics.events.Event;
import com.particlemetrics.events.MutableEvent;
import com.particlemetrics.events.ValidationException;
import com.particlemetrics.events.codecs.DecodeException;
import com.particlemetrics.events.codecs.DecodeIterator;
import com.particlemetrics.events.codecs.Decoder;
import com.particlemetrics.events.impl.MutableEventImpl;
import org.jetbrains.annotations.NotNull;

import java.io.IOException;
import java.util.*;
import java.util.function.Consumer;

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
        return decode(data, 0, data.length, validate);
    }

    @Override
    public @NotNull List<Event> decodeArray(@NotNull byte[] data) {
        return decodeArray(data, true);
    }

    public @NotNull List<Event> decodeArray(@NotNull byte[] data, boolean validate) {
        final List<Event> events = new ArrayList<>();
        decodeArray(data, validate, events::add);
        return events;
    }

    public void decodeArray(@NotNull byte[] data, boolean validate, @NotNull Consumer<Event> handler) {
        Objects.requireNonNull(handler, "handler cannot be null");
        try (final DecodeIterator iterator = arrayDecoder(data, validate)) {
            while (iterator.hasNext()) {
                handler.accept(iterator.next());
            }
        } catch (ValidationException | DecodeException e) {
            throw e;
        } catch (Exception e) {
            throw new DecodeException("Error decoding JSON", e);
        }
    }

    @Override
    public @NotNull DecodeIterator arrayDecoder(@NotNull byte[] data) {
        return arrayDecoder(data, true);
    }

    public @NotNull DecodeIterator arrayDecoder(@NotNull byte[] data, boolean validate) {
        Objects.requireNonNull(data, "data cannot be null");
        return JsonArrayIterator.create(jsonFactory, data, validate);
    }

    protected @NotNull Event decode(@NotNull byte[] data, int offset, int len, boolean validate) {
        Objects.requireNonNull(data, "data cannot be null");
        try (final JsonParser parser = jsonFactory.createParser(data, offset, len)) {
            Event event = decode(parser, false);
            assert event != null;
            if (validate) {
                ((MutableEventImpl) event).validate();
            }
            return event;
        } catch (IOException e) {
            throw new DecodeException("Error decoding JSON", e);
        }
    }

    private static class JsonArrayIterator implements DecodeIterator {
        final JsonParser parser;
        final boolean validate;
        Event nextEvent = null;

        static JsonArrayIterator create(JsonFactory jsonFactory, byte[] data, boolean validate) {
            try {
                JsonParser parser = createparser(jsonFactory, data);
                return new JsonArrayIterator(parser, validate);
            } catch (IOException e) {
                throw new DecodeException("Error decoding JSON", e);
            }
        }

        JsonArrayIterator(JsonParser parser, boolean validate) {
            this.parser = parser;
            this.validate = validate;
            this.nextEvent = decodeNextEvent();
        }

        @Override
        public boolean hasNext() {
            return nextEvent != null;
        }

        @Override
        public Event next() {
            if (nextEvent == null) {
                throw new NoSuchElementException();
            }
            Event result = nextEvent;
            nextEvent = decodeNextEvent();
            return result;
        }

        private Event decodeNextEvent() {
            if (parser.isClosed()) {
                return null;
            }
            try {
                Event event = decode(parser, true);
                if (event == null) {
                    // No exception was thrown and event was not decoded. Just return.
                    return null;
                }
                if (validate) {
                    ((MutableEventImpl) event).validate();
                }
                return event;
            } catch (IOException e) {
                throw new DecodeException("Error decoding JSON", e);
            }
        }

        @Override
        public void close() {
            try {
                parser.close();
            } catch (IOException e) {
                // pass
            }
        }

        private static JsonParser createparser(JsonFactory jsonFactory, byte[] data) throws IOException {
            JsonParser parser = jsonFactory.createParser(data, 0, data.length);
            JsonToken token = parser.nextToken();
            if (token != JsonToken.START_ARRAY) {
                parser.close();
                throw new DecodeException("Error decoding JSON array");
            }
            return parser;
        }
    }

    private static class EndOfArray extends DecodeException {
        EndOfArray(String msg) {
            super(msg);
        }
    }

    private static Event decode(@NotNull final JsonParser parser, boolean isDecodingArray) throws IOException {
        MutableEvent event = MutableEventImpl.create();
        boolean started = false;
        String fieldName = "";
        boolean finished = false;
        while (!finished) {
            if (parser.isClosed()) {
                break;
            }
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
                    finished = true;
                    break;
                case START_ARRAY:
                    throw new DecodeException("Unexpected START_ARRAY");
                case END_ARRAY:
//                    throw new EndOfArray("Unexpected END_ARRAY");
                    if (isDecodingArray) return null;
                    throw new DecodeException("Unexpected END_ARRAY");
                case VALUE_EMBEDDED_OBJECT:
                    throw new DecodeException("Unexpected VALUE_EMBEDDED_OBJECT");
                case VALUE_NUMBER_FLOAT:
                    throw new DecodeException("Unexpected VALUE_NUMBER_FLOAT");
                case VALUE_NULL:
                    throw new DecodeException("Unexpected VALUE_NUMBER_NULL");
            }
        }
        return event;
    }

    private static void updateEventStringAttribute(MutableEvent event, String fieldName, String fieldValue) {
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
                event.put(fieldName, fieldValue);
        }
    }

    private static <T> void updateEventAttribute(MutableEvent event, String fieldName, T fieldValue) {
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
                event.put(fieldName, fieldValue);
        }
    }
}
