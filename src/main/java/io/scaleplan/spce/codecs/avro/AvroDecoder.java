package io.scaleplan.spce.codecs.avro;

import io.scaleplan.spce.CloudEvent;
import io.scaleplan.spce.codecs.DecodeException;
import io.scaleplan.spce.codecs.DecodeIterator;
import io.scaleplan.spce.codecs.Decoder;
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

public class AvroDecoder implements Decoder {
    private final io.cloudevents.CloudEvent reusedCloudEvent = new io.cloudevents.CloudEvent();

    private static final DatumReader<io.cloudevents.CloudEvent> datumReader =
            new SpecificDatumReader<>(io.cloudevents.CloudEvent.class);

    public static AvroDecoder create() {
        return new AvroDecoder();
    }

    AvroDecoder() {

    }

    @Override
    public @NotNull CloudEvent decode(@NotNull byte[] data) {
        return decode(data, null);
    }

    /**
     * Decodes with reuse
     * <p>
     * Warning: Not thread-safe.
     *
     * @param data
     * @return
     */
    public @NotNull CloudEvent decodeFast(@NotNull byte[] data) {
        return decode(data, reusedCloudEvent);
    }

    private @NotNull CloudEvent decode(@NotNull byte[] data, io.cloudevents.CloudEvent reuse) {
        try {
            BinaryDecoder binaryDecoder = DecoderFactory.get().binaryDecoder(data, null);
            io.cloudevents.CloudEvent avroCe = datumReader.read(reuse, binaryDecoder);
            CloudEvent.Builder builder = CloudEvent.builder();
            Object avroData = avroCe.getData();
            if (avroData != null) {
                if (avroData instanceof Utf8) {
                    builder.setData(((Utf8) avroData).toString());
                } else if (avroData instanceof ByteBuffer) {
                    builder.setData(((ByteBuffer) avroData).array());
                } else {
                    throw new DecodeException(String.format("CloudEvent data must be a string or ByteBuffer, but got: %s", avroData.getClass()));
                }
            }
            Map<CharSequence, Object> avroAttrs = avroCe.getAttribute();
            if (avroAttrs != null) {
                for (Map.Entry<CharSequence, Object> kv : avroAttrs.entrySet()) {
                    String attrName = kv.getKey().toString();
                    Object attrValue = kv.getValue();
                    switch (attrName) {
                        case CloudEvent.ATTRIBUTE_SPEC_VERSION:
                            builder.setSpecVersion(((Utf8) attrValue).toString());
                            break;
                        case CloudEvent.ATTRIBUTE_ID:
                            builder.setId(((Utf8) attrValue).toString());
                            break;
                        case CloudEvent.ATTRIBUTE_SOURCE:
                            builder.setSource(((Utf8) attrValue).toString());
                            break;
                        case CloudEvent.ATTRIBUTE_TYPE:
                            builder.setType(((Utf8) attrValue).toString());
                            break;
                        // Optional attributes
                        case CloudEvent.ATTRIBUTE_TIME:
                            builder.setTime(((Utf8) attrValue).toString());
                            break;
                        case CloudEvent.ATTRIBUTE_SUBJECT:
                            builder.setSubject(((Utf8) attrValue).toString());
                            break;
                        case CloudEvent.ATTRIBUTE_DATA_CONTENT_TYPE:
                            builder.setDataContentType(((Utf8) attrValue).toString());
                            break;
                        case CloudEvent.ATTRIBUTE_DATA_SCHEMA:
                            builder.setDataSchema(((Utf8) attrValue).toString());
                            break;
                        default:
                            if (attrValue instanceof Utf8) {
                                builder.put(attrName, ((Utf8) attrValue).toString());
                            } else {
                                builder.put(attrName, attrValue);
                            }
                    }
                }
            }
            return builder.build();
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
