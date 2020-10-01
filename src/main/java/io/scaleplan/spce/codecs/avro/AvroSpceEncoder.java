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
