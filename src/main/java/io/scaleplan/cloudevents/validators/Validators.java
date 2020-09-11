package io.scaleplan.cloudevents.validators;

import com.ethlo.time.ITU;
import io.scaleplan.cloudevents.ValidationException;
import org.jetbrains.annotations.NotNull;

import java.util.Objects;

public class Validators {
    public static boolean isValidURI(@NotNull final String uri) {
        Objects.requireNonNull(uri);
        return Rfc3986Validator.isValid(uri);
    }

    public static boolean isValidURIRef(@NotNull final String uriRef) {
        Objects.requireNonNull(uriRef);
        return Rfc3986Validator.isValid(uriRef) || Rfc3986Validator.isValidReference(uriRef);
    }

    public static boolean isValidTimestamp(@NotNull final String timestamp) {
        Objects.requireNonNull(timestamp);
        return ITU.isValid(timestamp);
    }

    public static String requireValidURI(@NotNull final String uri) {
        return requireValidURI(uri, null);
    }

    public static String requireValidURI(@NotNull final String uri, final String exceptionMessage) {
        if (!isValidURI(uri)) {
            throw new ValidationException(exceptionMessage);
        }
        return uri;
    }

    public static String requireValidURIRef(@NotNull final String uriRef) {
        return requireValidURIRef(uriRef, null);
    }

    public static String requireValidURIRef(@NotNull final String uriRef, final String exceptionMessage) {
        if (!isValidURIRef(uriRef)) {
            throw new ValidationException(exceptionMessage);
        }
        return uriRef;
    }
}
