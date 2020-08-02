package com.particlemetrics.events.codecs;

public class EncoderException extends RuntimeException {
    public EncoderException() {
        super();
    }

    public EncoderException(String msg) {
        super(msg);
    }

    public EncoderException(Throwable t) {
        super(t);
    }

    public EncoderException(String msg, Throwable t) {
        super(msg, t);
    }

}
