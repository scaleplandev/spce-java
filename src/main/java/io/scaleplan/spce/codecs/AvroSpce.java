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
import io.scaleplan.spce.codecs.avro.AvroSpceDecoder;
import io.scaleplan.spce.codecs.avro.AvroSpceEncoder;
import org.jetbrains.annotations.NotNull;

import java.util.Collection;
import java.util.List;

public class AvroSpce {
    public static @NotNull byte[] encode(final @NotNull CloudEvent event) {
        return AvroSpce.EncoderHolder.instance.encode(event);
    }

    public static @NotNull CloudEvent decode(@NotNull byte[] data) {
        return AvroSpce.DecoderHolder.instance.decode(data);
    }

    public static @NotNull byte[] encode(@NotNull final Collection<CloudEvent> events) {
        return EncoderHolder.instance.encode(events);
    }

    public static @NotNull List<CloudEvent> decodeBatch(@NotNull byte[] data) {
        return DecoderHolder.instance.decodeBatch(data);
    }

    private static class EncoderHolder {
        private static final AvroSpceEncoder instance = AvroSpceEncoder.create();
    }

    private static class DecoderHolder {
        private static final AvroSpceDecoder instance = AvroSpceDecoder.create();
    }

}
