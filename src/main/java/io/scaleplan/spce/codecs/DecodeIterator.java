package io.scaleplan.spce.codecs;

import io.scaleplan.spce.CloudEvent;

import java.util.Iterator;

public interface DecodeIterator extends Iterator<CloudEvent>, AutoCloseable {
}
