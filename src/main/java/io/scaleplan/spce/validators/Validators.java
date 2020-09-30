package io.scaleplan.spce.validators;

import com.ethlo.time.ITU;
import io.scaleplan.spce.ValidationException;
import org.jetbrains.annotations.NotNull;

import java.net.URISyntaxException;
import java.util.Objects;

public class Validators {
    public static boolean isValidURI(@NotNull final String uri) {
        Objects.requireNonNull(uri);
        try {
            new Parser(uri).parse(true);
            return true;
        } catch (URISyntaxException e) {
            return false;
        }
    }

    public static boolean isValidURIRef(@NotNull final String uriRef) {
        Objects.requireNonNull(uriRef);
//        return Rfc3986Validator.isValidReference(uriRef) || Rfc3986Validator.isValid(uriRef);
        try {
            new Parser(uriRef).parse(false);
            return true;
        } catch (URISyntaxException e) {
            return false;
        }
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

    public static String requireValidTimestamp(@NotNull final String timestamp) {
        return requireValidTimestamp(timestamp, null);
    }

    public static String requireValidTimestamp(@NotNull final String timestamp, final String exceptionMessage) {
        if (!isValidTimestamp(timestamp)) {
            throw new ValidationException(exceptionMessage);
        }
        return timestamp;
    }
}
