// Copyright 2020 Scale Plan Yazılım A.Ş.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package io.scaleplan.spce.codecs.impl;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import io.scaleplan.spce.CloudEvent;
import io.scaleplan.spce.MutableCloudEvent;
import io.scaleplan.spce.ValidationException;
import io.scaleplan.spce.codecs.DecodeException;
import io.scaleplan.spce.codecs.DecodeIterator;
import io.scaleplan.spce.codecs.Decoder;
import io.scaleplan.spce.impl.MutableCloudEventImpl;
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
    public @NotNull CloudEvent decode(@NotNull byte[] data) {
        return decode(data, true);
    }

    public @NotNull CloudEvent decode(@NotNull byte[] data, boolean validate) {
        return decode(data, 0, data.length, validate);
    }

    @Override
    public @NotNull List<CloudEvent> decodeBatch(@NotNull byte[] data) {
        return decodeBatch(data, true);
    }

    public @NotNull List<CloudEvent> decodeBatch(@NotNull byte[] data, boolean validate) {
        final List<CloudEvent> events = new ArrayList<>();
        decodeBatch(data, validate, events::add);
        return events;
    }

    public void decodeBatch(@NotNull byte[] data, boolean validate, @NotNull Consumer<CloudEvent> handler) {
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
    public @NotNull DecodeIterator batchDecoder(@NotNull byte[] data) {
        return arrayDecoder(data, true);
    }

    public @NotNull DecodeIterator arrayDecoder(@NotNull byte[] data, boolean validate) {
        Objects.requireNonNull(data, "data cannot be null");
        return JsonArrayIterator.create(jsonFactory, data, validate);
    }

    protected @NotNull CloudEvent decode(@NotNull byte[] data, int offset, int len, boolean validate) {
        Objects.requireNonNull(data, "data cannot be null");
        try (final JsonParser parser = jsonFactory.createParser(data, offset, len)) {
            CloudEvent event = decode(parser, false);
            assert event != null;
            if (validate) {
                ((MutableCloudEventImpl) event).validate();
            }
            return event;
        } catch (IOException e) {
            throw new DecodeException("Error decoding JSON", e);
        }
    }

    private static class JsonArrayIterator implements DecodeIterator {
        final JsonParser parser;
        final boolean validate;
        CloudEvent nextEvent;

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
        public CloudEvent next() {
            if (nextEvent == null) {
                throw new NoSuchElementException();
            }
            CloudEvent result = nextEvent;
            nextEvent = decodeNextEvent();
            return result;
        }

        private CloudEvent decodeNextEvent() {
            if (parser.isClosed()) {
                return null;
            }
            try {
                CloudEvent event = decode(parser, true);
                if (event == null) {
                    // No exception was thrown and event was not decoded. Just return.
                    return null;
                }
                if (validate) {
                    ((MutableCloudEventImpl) event).validate();
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

    private static CloudEvent decode(@NotNull final JsonParser parser, boolean isDecodingArray) throws IOException {
        MutableCloudEvent event = MutableCloudEventImpl.create();
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
                    if (started) {
                        throw new DecodeException("Unexpected START_OBJECT");
                    }
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
                    if (isDecodingArray) {
                        return null;
                    }
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

    private static void updateEventStringAttribute(MutableCloudEvent event, String fieldName, String fieldValue) {
        switch (fieldName) {
            // Required attributes
            case CloudEvent.ATTRIBUTE_SPEC_VERSION:
                event.setSpecVersion(fieldValue);
                break;
            case CloudEvent.ATTRIBUTE_ID:
                event.setId(fieldValue);
                break;
            case CloudEvent.ATTRIBUTE_SOURCE:
                event.setSource(fieldValue);
                break;
            case CloudEvent.ATTRIBUTE_TYPE:
                event.setType(fieldValue);
                break;
            // Optional attributes
            case CloudEvent.ATTRIBUTE_TIME:
                event.setTime(fieldValue);
                break;
            case CloudEvent.ATTRIBUTE_SUBJECT:
                event.setSubject(fieldValue);
                break;
            case CloudEvent.ATTRIBUTE_DATA:
                event.setData(fieldValue);
                break;
            case CloudEvent.ATTRIBUTE_DATA_BASE64:
                event.setDataUnsafe(base64Decoder.decode(fieldValue));
                break;
            case CloudEvent.ATTRIBUTE_DATA_CONTENT_TYPE:
                event.setDataContentType(fieldValue);
                break;
            case CloudEvent.ATTRIBUTE_DATA_SCHEMA:
                event.setDataSchema(fieldValue);
                break;
            default:
                event.put(fieldName, fieldValue);
        }
    }

    private static <T> void updateEventAttribute(MutableCloudEvent event, String fieldName, T fieldValue) {
        // This method runs when the field value is not a string.
        // Throw an exception if the field name is a required or optional attribute.
        switch (fieldName) {
            // Required attributes
            case CloudEvent.ATTRIBUTE_SPEC_VERSION:
                throw new DecodeException(String.format("%s must be a string", CloudEvent.ATTRIBUTE_SPEC_VERSION));
            case CloudEvent.ATTRIBUTE_ID:
                throw new DecodeException(String.format("%s must be a string", CloudEvent.ATTRIBUTE_ID));
            case CloudEvent.ATTRIBUTE_SOURCE:
                throw new DecodeException(String.format("%s must be a string", CloudEvent.ATTRIBUTE_SOURCE));
            case CloudEvent.ATTRIBUTE_TYPE:
                throw new DecodeException(String.format("%s must be a string", CloudEvent.ATTRIBUTE_TYPE));
                // Optional attributes
            case CloudEvent.ATTRIBUTE_TIME:
                throw new DecodeException(String.format("%s must be a string", CloudEvent.ATTRIBUTE_TIME));
            case CloudEvent.ATTRIBUTE_SUBJECT:
                throw new DecodeException(String.format("%s must be a string", CloudEvent.ATTRIBUTE_SUBJECT));
            case CloudEvent.ATTRIBUTE_DATA:
                throw new DecodeException(String.format("%s must be a string", CloudEvent.ATTRIBUTE_DATA));
            case CloudEvent.ATTRIBUTE_DATA_BASE64:
                throw new DecodeException(String.format("%s must be a string", CloudEvent.ATTRIBUTE_DATA_BASE64));
            case CloudEvent.ATTRIBUTE_DATA_CONTENT_TYPE:
                throw new DecodeException(String.format("%s must be a string", CloudEvent.ATTRIBUTE_DATA_CONTENT_TYPE));
            case CloudEvent.ATTRIBUTE_DATA_SCHEMA:
                throw new DecodeException(String.format("%s must be a string", CloudEvent.ATTRIBUTE_DATA_SCHEMA));
            default:
                event.put(fieldName, fieldValue);
        }
    }
}
