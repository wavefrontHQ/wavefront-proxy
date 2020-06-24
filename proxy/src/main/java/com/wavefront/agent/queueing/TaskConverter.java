package com.wavefront.agent.queueing;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.IOException;
import java.io.OutputStream;

/**
 * Proxy-specific interface for converting data into and from queues,
 * this potentially allows us to support other converting mechanisms in the future.
 *
 * @param <T> type of objects stored.
 *
 * @author mike@wavefront.com
 */
public interface TaskConverter<T> {

  /**
   * De-serializes an object from a byte array.
   *
   * @return de-serialized object.
   **/
  T fromBytes(@Nonnull byte[] bytes) throws IOException;

  /**
   * Serializes {@code value} to bytes written to the specified stream.
   *
   * @param value value to serialize.
   * @param bytes output stream to write a {@code byte[]} to.
   *
   **/
  void serializeToStream(@Nonnull T value, @Nonnull OutputStream bytes) throws IOException;

  /**
   * Attempts to retrieve task weight from a {@code byte[]}, without de-serializing
   * the object, if at all possible.
   *
   * @return task weight or null if not applicable.
   **/
  @Nullable
  Integer getWeight(@Nonnull byte[] bytes);

  /**
   * Supported compression schemas
   **/
  enum CompressionType {
    NONE, GZIP, LZ4
  }
}
