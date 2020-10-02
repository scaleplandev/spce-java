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
import org.jetbrains.annotations.NotNull;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

public class AvroSpceEncoder implements Encoder {
    private final static DatumWriter<AvroCloudEvent> datumWriter =
            new SpecificDatumWriter<>(AvroCloudEvent.class);

    public static AvroSpceEncoder create() {
        return new AvroSpceEncoder();
    }

    AvroSpceEncoder() {

    }

    @Override
    public @NotNull byte[] encode(@NotNull CloudEvent event) {
        try (ByteArrayOutputStream bos = new ByteArrayOutputStream()) {
            encode(event, bos);
            return bos.toByteArray();
        } catch (IOException e) {
            throw new EncodeException("Error while retrieving Avro byte array", e);
        }
    }

    @Override
    public void encode(@NotNull CloudEvent event, @NotNull OutputStream outputStream) {
        AvroCloudEvent avroCe = new AvroCloudEvent();
        avroCe.setSpecversion(event.getSpecVersion());
        avroCe.setType(event.getType());
        avroCe.setSource(event.getSource());
        avroCe.setId(event.getId());
        avroCe.setSubject(event.getSubject());
        avroCe.setTime(event.getTime());
        avroCe.setDataschema(event.getDataSchema());
        avroCe.setDatacontenttype(event.getDataContentType());

        byte[] data = event.getData();
        if (data != null) {
            if (event.hasBinaryData()) avroCe.setData(ByteBuffer.wrap(data));
            else avroCe.setData(new String(data));
        }

        Map<String, Object> attrs = event.getAttributes();
        Map<String, Object> avroAttrs = new HashMap<>(attrs.size());
        for (Map.Entry<String, Object> kv : attrs.entrySet()) {
            switch (kv.getKey()) {
                case CloudEvent.ATTRIBUTE_SPEC_VERSION:
                case CloudEvent.ATTRIBUTE_TYPE:
                case CloudEvent.ATTRIBUTE_SOURCE:
                case CloudEvent.ATTRIBUTE_ID:
                case CloudEvent.ATTRIBUTE_SUBJECT:
                case CloudEvent.ATTRIBUTE_TIME:
                case CloudEvent.ATTRIBUTE_DATA_SCHEMA:
                case CloudEvent.ATTRIBUTE_DATA_CONTENT_TYPE:
                case CloudEvent.ATTRIBUTE_DATA:
                    continue;
                default:
                    avroAttrs.put(kv.getKey(), kv.getValue());
            }
        }
        avroCe.setAttribute(avroAttrs);
        // TODO: reuse
        BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(outputStream, null);
        try {
            datumWriter.write(avroCe, encoder);
            encoder.flush(); // XXX:
        } catch (IOException e) {
            throw new EncodeException("Error while writing Avro encoded event", e);
        }
    }

    @Override
    public @NotNull byte[] encode(@NotNull Collection<CloudEvent> events) {
        try (ByteArrayOutputStream bos = new ByteArrayOutputStream()) {
            encode(events, bos);
            return bos.toByteArray();
        } catch (IOException e) {
            throw new EncodeException("Error while retrieving Avro byte array", e);
        }
    }

    @Override
    public void encode(@NotNull Collection<CloudEvent> events, @NotNull OutputStream outputStream) {
        for (CloudEvent event : events) {
            encode(event, outputStream);
        }
    }
}
