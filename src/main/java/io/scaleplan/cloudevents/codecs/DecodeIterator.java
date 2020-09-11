package io.scaleplan.cloudevents.codecs;

import io.scaleplan.cloudevents.CloudEvent;

import java.util.Iterator;

public interface DecodeIterator extends Iterator<CloudEvent>, AutoCloseable {
}
