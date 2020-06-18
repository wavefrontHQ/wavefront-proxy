package com.wavefront.agent.queueing;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.io.OutputStream;

public interface TaskConverter<T> {

  T from(@Nonnull byte[] bytes);
  void toStream(@Nonnull T t, @Nonnull OutputStream bytes) throws IOException;
  enum CompressionType { NONE, GZIP, LZ4 }
}
