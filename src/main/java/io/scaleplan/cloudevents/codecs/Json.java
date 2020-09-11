package io.scaleplan.cloudevents.codecs;

import io.scaleplan.cloudevents.codecs.impl.JsonDecoder;
import io.scaleplan.cloudevents.codecs.impl.JsonEncoder;

public class Json {
    public static JsonDecoder getDecoder() {
        return DecoderHolder.instance;
    }

    public static JsonEncoder getEncoder() {
        return EncoderHolder.instance;
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
