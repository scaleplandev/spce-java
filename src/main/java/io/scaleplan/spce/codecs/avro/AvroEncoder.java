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

package io.scaleplan.spce.codecs.avro;

import io.scaleplan.spce.CloudEvent;
import io.scaleplan.spce.codecs.EncodeException;
import io.scaleplan.spce.codecs.Encoder;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.avro.util.Utf8;
import org.jetbrains.annotations.NotNull;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

public class AvroEncoder implements Encoder {
    private final static DatumWriter<io.cloudevents.avro.CloudEvent> datumWriter =
            new SpecificDatumWriter<>(io.cloudevents.avro.CloudEvent.class);

    public static AvroEncoder create() {
        return new AvroEncoder();
    }

    AvroEncoder() {
    }

    @Override
    public @NotNull byte[] encode(@NotNull CloudEvent event) {
        io.cloudevents.avro.CloudEvent avroCe = new io.cloudevents.avro.CloudEvent();
        Map<String, Object> attrs = event.getAttributes();
        Map<CharSequence, Object> avroAttrs = new HashMap<>(attrs.size());
        for (Map.Entry<String, Object> kv : attrs.entrySet()) {
            avroAttrs.put(kv.getKey(), kv.getValue());
        }
        avroCe.setAttribute(avroAttrs);
        byte[] data = event.getData();
        if (data != null) {
            if (event.hasBinaryData()) {
                avroCe.setData(ByteBuffer.wrap(data));
            } else {
                // TODO: prevent new String creation
                avroCe.setData(new Utf8(data));
            }
        }
        try (ByteArrayOutputStream bos = new ByteArrayOutputStream()) {
            // TODO: reuse
            BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(bos, null);
            datumWriter.write(avroCe, encoder);
            encoder.flush();
            return bos.toByteArray();
        } catch (IOException e) {
            throw new EncodeException("Error while retrieving Avro byte array", e);
        }
    }

    @Override
    public void encode(@NotNull CloudEvent event, @NotNull OutputStream outputStream) {
        throw new EncodeException("Not implemented yet.");
    }

    @Override
    public @NotNull byte[] encode(@NotNull Collection<CloudEvent> events) {
        throw new EncodeException("CloudEvents Avro spec does not specify batch events.");
    }

    @Override
    public void encode(@NotNull Collection<CloudEvent> events, @NotNull OutputStream outputStream) {
        throw new EncodeException("CloudEvents Avro spec does not specify batch events.");
    }
}
