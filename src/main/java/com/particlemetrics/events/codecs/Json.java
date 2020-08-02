package com.particlemetrics.events.codecs;

import com.particlemetrics.events.codecs.impl.JsonDecoder;
import com.particlemetrics.events.codecs.impl.JsonEncoder;

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
