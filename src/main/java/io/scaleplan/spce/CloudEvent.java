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

package io.scaleplan.spce;

import io.scaleplan.spce.impl.MutableCloudEventImpl;
import org.jetbrains.annotations.NotNull;
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

    String SPEC_VERSION = "1.0";

    final class Builder {
        private final MutableCloudEventImpl event = (MutableCloudEventImpl) MutableCloudEventImpl.create()
                .setSpecVersion(SPEC_VERSION);

        public CloudEvent build() {
            event.validate();
            return event;
        }

        public Builder setSpecVersion(@NotNull String specVersion) {
            event.setSpecVersion(specVersion);
            return this;
        }

        public Builder setId(@NotNull String id) {
            event.setId(id);
            return this;
        }

        public Builder setSource(@NotNull String source) {
            event.setSource(source);
            return this;
        }

        public Builder setType(@NotNull String type) {
            event.setType(type);
            return this;
        }

        // Optional attributes

        public Builder setData(@NotNull String data) {
            event.setData(data);
            return this;
        }

        public Builder setData(@NotNull byte[] data) {
            event.setData(data);
            return this;
        }

        public Builder setDataUnsafe(@NotNull byte[] data) {
            event.setDataUnsafe(data);
            return this;
        }

        public Builder setDataContentType(@NotNull String contentType) {
            event.setDataContentType(contentType);
            return this;
        }

        public Builder setDataSchema(@NotNull String dataSchema) {
            event.setDataSchema(dataSchema);
            return this;
        }

        public Builder setSubject(@NotNull String subject) {
            event.setSubject(subject);
            return this;
        }

        public Builder setTime(@NotNull String time) {
            event.setTime(time);
            return this;
        }

        public Builder setTime(long milliseconds) {
            event.setTime(milliseconds);
            return this;
        }

        public Builder setTimeNow() {
            event.setTimeNow();
            return this;
        }

        // Extended attributes

        /**
         * Adds or replaces an extended attribute.
         * Cannot add or replace a required or optional attribute.
         *
         * @param name  Extended attribute name
         * @param value Extended attribute value
         * @return a MutableEvent
         * @throws IllegalArgumentException if name is a required or optional attribute.
         * @throws NullPointerException     if either name or value is null.
         */
        public <T> Builder put(@NotNull String name, @NotNull T value) {
            event.put(name, value);
            return this;
        }

        /**
         * Adds or replaces an extended attribute.
         * Can add or replace a required or optional attribute.
         *
         * @param name  Extended attribute name
         * @param value Extended attribute value
         * @return a MutableEvent
         * @throws NullPointerException if either name or value is null.
         */
        public <T> Builder putUnsafe(@NotNull String name, @NotNull T value) {
            event.put(name, value);
            return this;
        }
    }

    static Builder builder() {
        return new Builder();
    }

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
