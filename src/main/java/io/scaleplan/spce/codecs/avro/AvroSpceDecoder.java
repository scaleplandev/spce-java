package io.scaleplan.spce.codecs.avro;

import io.scaleplan.spce.CloudEvent;
import io.scaleplan.spce.MutableCloudEvent;
import io.scaleplan.spce.codecs.DecodeException;
import io.scaleplan.spce.codecs.DecodeIterator;
import io.scaleplan.spce.codecs.Decoder;
import io.scaleplan.spce.impl.MutableCloudEventImpl;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.util.Utf8;
import org.jetbrains.annotations.NotNull;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;

public class AvroSpceDecoder implements Decoder {
    private static final DatumReader<AvroCloudEvent> datumReader =
            new SpecificDatumReader<>(AvroCloudEvent.class);

    public static AvroSpceDecoder create() {
        return new AvroSpceDecoder();
    }

    AvroSpceDecoder() {

    }

    @Override
    public @NotNull CloudEvent decode(@NotNull byte[] encodedEvent) {
        BinaryDecoder binaryDecoder = DecoderFactory.get().binaryDecoder(encodedEvent, null);
        try {
            AvroCloudEvent avroCe = datumReader.read(null, binaryDecoder);
            MutableCloudEvent event = MutableCloudEventImpl.create()
                    .setSpecVersion(avroCe.getSpecversion().toString())
                    .setType(avroCe.getType().toString())
                    .setSource(avroCe.getSource().toString())
                    .setId(avroCe.getId().toString());
            if (avroCe.getSubject() != null) event.setSubject(avroCe.getSubject().toString());
            if (avroCe.getTime() != null) event.setTime(avroCe.getTime().toString());
            if (avroCe.getDataschema() != null) event.setDataSchema(avroCe.getDataschema().toString());
            if (avroCe.getDatacontenttype() != null) event.setDataContentType(avroCe.getDatacontenttype().toString());

            Object data = avroCe.getData();
            if (data != null) {
                if (data instanceof Utf8) event.setData(((Utf8) data).toString());
                else event.setData(((ByteBuffer) data).array());
            }

            for (Map.Entry<CharSequence, Object> kv : avroCe.getAttribute().entrySet()) {
                Object value = kv.getValue();
                if (value instanceof Utf8) {
                    event.put(kv.getKey().toString(), value.toString());
                } else {
                    event.put(kv.getKey().toString(), value);
                }
            }

            return event;
        } catch (IOException e) {
            throw new DecodeException("Error while decoding Avro data.", e);
        }
    }

    @Override
    public @NotNull List<CloudEvent> decodeBatch(@NotNull byte[] data) {
        throw new DecodeException("CloudEvents Avro spec does not specify batch events.");
    }

    @Override
    public @NotNull DecodeIterator batchDecoder(@NotNull byte[] data) {
        throw new DecodeException("CloudEvents Avro spec does not specify batch events.");
    }
}
