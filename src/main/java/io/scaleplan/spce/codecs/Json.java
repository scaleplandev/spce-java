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

package io.scaleplan.spce.codecs;

import io.scaleplan.spce.CloudEvent;
import io.scaleplan.spce.codecs.impl.JsonDecoder;
import io.scaleplan.spce.codecs.impl.JsonEncoder;
import org.jetbrains.annotations.NotNull;

import java.io.OutputStream;
import java.util.Collection;
import java.util.List;

public class Json {
    public static @NotNull byte[] encode(final @NotNull CloudEvent event) {
        return EncoderHolder.instance.encode(event);
    }

    public static void encode(@NotNull final CloudEvent event, @NotNull final OutputStream outputStream) {
        EncoderHolder.instance.encode(event, outputStream);
    }

    public static @NotNull byte[] encode(@NotNull final Collection<CloudEvent> events) {
        return EncoderHolder.instance.encode(events);
    }

    public static void encode(@NotNull final Collection<CloudEvent> events, @NotNull final OutputStream outputStream) {
        EncoderHolder.instance.encode(events, outputStream);
    }

    public static @NotNull CloudEvent decode(@NotNull byte[] data) {
        return DecoderHolder.instance.decode(data);
    }

    public static @NotNull List<CloudEvent> decodeBatch(@NotNull byte[] data) {
        return DecoderHolder.instance.decodeBatch(data);
    }

    public static @NotNull DecodeIterator batchDecoder(@NotNull byte[] data) {
        return DecoderHolder.instance.arrayDecoder(data);
    }

    private static class DecoderHolder {
        private static final JsonDecoder instance = JsonDecoder.create();
    }

    private static class EncoderHolder {
        private static final JsonEncoder instance = JsonEncoder.create();
    }

    private Json() {
    }
}
