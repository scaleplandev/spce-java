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

import org.jetbrains.annotations.NotNull;

public interface MutableCloudEvent extends CloudEvent {
    MutableCloudEvent reset();

    // Required attributes

    MutableCloudEvent setSpecVersion(@NotNull String specVersion);

    MutableCloudEvent setId(@NotNull String id);

    MutableCloudEvent setSource(@NotNull String source);

    MutableCloudEvent setType(@NotNull String type);

    // Optional attributes

    MutableCloudEvent setData(@NotNull String data);

    MutableCloudEvent setData(@NotNull byte[] data);

    MutableCloudEvent setDataUnsafe(@NotNull byte[] data);

    MutableCloudEvent setDataContentType(@NotNull String contentType);

    MutableCloudEvent setDataSchema(@NotNull String dataSchema);

    MutableCloudEvent setSubject(@NotNull String subject);

    MutableCloudEvent setTime(@NotNull String time);

    MutableCloudEvent setTime(long milliseconds);

    MutableCloudEvent setTimeNow();

    // Extended attributes

    /**
     * Adds or replaces an extended attribute.
     * Cannot add or replace a required or optional attribute.
     *
     * @param name  Extended attribute name
     * @param value Extended attribute value
     * @param <T>   type for attribute value
     * @return a MutableEvent
     * @throws IllegalArgumentException if name is a required or optional attribute.
     * @throws NullPointerException     if either name or value is null.
     */
    <T> MutableCloudEvent put(@NotNull String name, @NotNull T value);

    /**
     * Adds or replaces an extended attribute.
     * Can add or replace a required or optional attribute.
     *
     * @param name  Extended attribute name
     * @param value Extended attribute value
     * @param <T> type for attribute value
     * @return a MutableEvent
     * @throws NullPointerException if either name or value is null.
     */
    <T> MutableCloudEvent putUnsafe(@NotNull String name, @NotNull T value);

    MutableCloudEvent remove(@NotNull String name);

}
