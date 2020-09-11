package io.scaleplan.cloudevents.codecs;

public class EncodeException extends RuntimeException {
    public EncodeException() {
        super();
    }

    public EncodeException(String msg) {
        super(msg);
    }

    public EncodeException(Throwable t) {
        super(t);
    }

    public EncodeException(String msg, Throwable t) {
        super(msg, t);
    }

}
