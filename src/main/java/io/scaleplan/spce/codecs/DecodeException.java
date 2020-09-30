package io.scaleplan.spce.codecs;

public class DecodeException extends RuntimeException {
    public DecodeException() {
        super();
    }

    public DecodeException(Throwable t) {
        super(t);
    }

    public DecodeException(String msg) {
        super(msg);
    }

    public DecodeException(String msg, Throwable t) {
        super(msg, t);
    }
}
