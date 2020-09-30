package io.scaleplan.spce.codecs;

import io.scaleplan.spce.codecs.impl.JsonDecoder;
import io.scaleplan.spce.codecs.impl.JsonEncoder;

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
