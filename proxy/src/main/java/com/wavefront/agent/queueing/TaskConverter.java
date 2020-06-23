package com.wavefront.agent.queueing;

import javax.annotation.Nonnull;
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

  /** Converts bytes to an Object **/
  T from(@Nonnull byte[] bytes);

  /** Converts {@code value} to bytes written to the specified stream. **/
  void toStream(@Nonnull T t, @Nonnull OutputStream bytes) throws IOException;

  /** Supported compression schemas **/
  enum CompressionType { NONE, GZIP, LZ4 }
}
