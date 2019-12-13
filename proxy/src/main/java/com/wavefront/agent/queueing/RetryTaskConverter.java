package com.wavefront.agent.queueing;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.squareup.tape2.ObjectQueue;
import com.wavefront.agent.data.DataSubmissionTask;
import com.wavefront.common.TaggedMetricName;
import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.Counter;
import net.jpountz.lz4.LZ4BlockInputStream;
import net.jpountz.lz4.LZ4BlockOutputStream;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.logging.Logger;

/**
 * A serializer + deserializer of {@link DataSubmissionTask} objects for storage.
 *
 * @param <T> task type
 * @author vasily@wavefront.com
 */
public class RetryTaskConverter<T extends DataSubmissionTask<T>>
    implements ObjectQueue.Converter<T> {
  private static final Logger logger = Logger.getLogger(
      RetryTaskConverter.class.getCanonicalName());

  private final ObjectMapper objectMapper = new ObjectMapper();

  private final String handle;
  private final boolean disableCompression;
  private final Counter errorCounter;

  /**
   * TODO (VV): javadoc
   *
   * @param handle
   * @param disableCompression
   */
  RetryTaskConverter(String handle, boolean disableCompression) {
    this.handle = handle;
    objectMapper.enableDefaultTyping();
    //this.objectReader = objectMapper.readerFor(DataSubmissionTask.class);
    //this.objectWriter = objectMapper.writerFor(DataSubmissionTask.class);
    this.disableCompression = disableCompression;
    this.errorCounter = Metrics.newCounter(new TaggedMetricName("buffer", "read-errors",
        "port", handle));
  }

  @SuppressWarnings("unchecked")
  @Nullable
  @Override
  public T from(@Nonnull byte[] bytes) throws IOException {
    try {
      if (disableCompression) {
        return (T) objectMapper.readValue(bytes, DataSubmissionTask.class);
      } else {
        return (T) objectMapper.readValue(new LZ4BlockInputStream(new ByteArrayInputStream(bytes)),
            DataSubmissionTask.class);
      }
    } catch (Throwable t) {
      logger.warning("Failed to read a single retry submission from buffer, ignoring: " + t);
      errorCounter.inc();
      return null;
    }
  }

  @Override
  public void toStream(@Nonnull T t, @Nonnull OutputStream bytes) throws IOException {
    if (disableCompression) {
      objectMapper.writeValue(bytes, t);
    } else {
      LZ4BlockOutputStream lz4BlockOutputStream = new LZ4BlockOutputStream(bytes);
      objectMapper.writeValue(lz4BlockOutputStream, t);
      lz4BlockOutputStream.close();
    }
  }
}
