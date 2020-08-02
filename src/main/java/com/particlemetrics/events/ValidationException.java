package com.particlemetrics.events;

public class ValidationException extends RuntimeException {
    public ValidationException() {
        super();
    }

    public ValidationException(String msg) {
        super(msg);
    }

    public ValidationException(Throwable t) {
        super(t);
    }

    public ValidationException(String msg, Throwable t) {
        super(msg, t);
    }
}
