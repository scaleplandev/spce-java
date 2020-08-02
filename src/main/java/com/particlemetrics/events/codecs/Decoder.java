package com.particlemetrics.events.codecs;

import com.particlemetrics.events.Event;

public interface Decoder {
    Event decode(byte[] data);
}
