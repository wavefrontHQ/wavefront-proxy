package com.wavefront.agent.queueing;

import java.io.ByteArrayOutputStream;

/** Enables direct access to the internal array. Avoids unnecessary copying. */
public final class DirectByteArrayOutputStream extends ByteArrayOutputStream {

  /**
   * Gets a reference to the internal byte array. The {@link #size()} method indicates how many
   * bytes contain actual data added since the last {@link #reset()} call.
   */
  byte[] getArray() {
    return buf;
  }
}
