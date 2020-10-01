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
import org.jetbrains.annotations.NotNull;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
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
        AvroCloudEvent avroCe = new AvroCloudEvent();
        return decode(binaryDecoder, avroCe);
    }

    @NotNull CloudEvent decode(BinaryDecoder binaryDecoder, AvroCloudEvent avroCe) {
        try {
            avroCe = datumReader.read(avroCe, binaryDecoder);
            MutableCloudEvent event = MutableCloudEventImpl.create()
                    .setSpecVersion(avroCe.getSpecversion())
                    .setType(avroCe.getType())
                    .setSource(avroCe.getSource())
                    .setId(avroCe.getId());
            if (avroCe.getSubject() != null) event.setSubject(avroCe.getSubject());
            if (avroCe.getTime() != null) event.setTime(avroCe.getTime());
            if (avroCe.getDataschema() != null) event.setDataSchema(avroCe.getDataschema());
            if (avroCe.getDatacontenttype() != null) event.setDataContentType(avroCe.getDatacontenttype());

            Object data = avroCe.getData();
            if (data != null) {
                if (data instanceof String) event.setData((String) data);
                else event.setData(((ByteBuffer) data).array());
            }

            for (Map.Entry<String, Object> kv : avroCe.getAttribute().entrySet()) {
                event.put(kv.getKey(), kv.getValue());
            }

            return event;
        } catch (IOException e) {
            throw new DecodeException("Error while decoding Avro data.", e);
        }
    }

    @Override
    public @NotNull List<CloudEvent> decodeBatch(@NotNull byte[] encodedEvents) {
        BinaryDecoder binaryDecoder = DecoderFactory.get().binaryDecoder(encodedEvents, null);
        AvroCloudEvent avroCe = new AvroCloudEvent();
        List<CloudEvent> decodedEvents = new ArrayList<>();
        while (true) {
            try {
                if (binaryDecoder.isEnd()) break;
                decodedEvents.add(decode(binaryDecoder, avroCe));
            } catch (IOException e) {
                throw new DecodeException("Error while decoding batch Avro data.", e);
            }
        }
        return decodedEvents;
    }

    @Override
    public @NotNull DecodeIterator batchDecoder(@NotNull byte[] data) {
        throw new DecodeException("CloudEvents Avro spec does not specify batch events.");
    }
}
