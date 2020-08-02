package com.particlemetrics.events.codecs;

import com.particlemetrics.events.Event;

import java.util.Iterator;

public interface DecodeIterator extends Iterator<Event>, AutoCloseable {
}
