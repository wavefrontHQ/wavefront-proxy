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
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

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

  private final CompressionType compressionType;
  private final Counter errorCounter;

  /**
   * @param handle          Handle (usually port number) of the pipeline where the data came from.
   * @param compressionType compression type to use for storing tasks.
   */
  RetryTaskConverter(String handle, CompressionType compressionType) {
    objectMapper.enableDefaultTyping();
    this.compressionType = compressionType;
    this.errorCounter = Metrics.newCounter(new TaggedMetricName("buffer", "read-errors",
        "port", handle));
  }

  @SuppressWarnings("unchecked")
  @Nullable
  @Override
  public T from(@Nonnull byte[] bytes) {
    if (bytes.length < 3) return null;
    try {
      if (bytes[0] == 'L' && bytes[1] == 'Z' && bytes[2] == '4') { // LZ4 compression
        return (T) objectMapper.readValue(new LZ4BlockInputStream(new ByteArrayInputStream(bytes)),
            DataSubmissionTask.class);
      } else if (bytes[0] == (byte)0x1f && bytes[1] == (byte)0x8b) { // GZIP compression
        return (T) objectMapper.readValue(new GZIPInputStream(new ByteArrayInputStream(bytes)),
            DataSubmissionTask.class);
      } else {
        return (T) objectMapper.readValue(bytes, DataSubmissionTask.class);
      }
    } catch (Throwable t) {
      logger.warning("Failed to read a single retry submission from buffer, ignoring: " + t);
      errorCounter.inc();
      return null;
    }
  }

  @Override
  public void toStream(@Nonnull T t, @Nonnull OutputStream bytes) throws IOException {
    switch (compressionType) {
      case LZ4:
        LZ4BlockOutputStream lz4BlockOutputStream = new LZ4BlockOutputStream(bytes);
        objectMapper.writeValue(lz4BlockOutputStream, t);
        lz4BlockOutputStream.close();
        return;
      case GZIP:
        GZIPOutputStream gzipOutputStream = new GZIPOutputStream(bytes);
        objectMapper.writeValue(gzipOutputStream, t);
        gzipOutputStream.close();
      case NONE:
        objectMapper.writeValue(bytes, t);
    }
  }

  public enum CompressionType { NONE, GZIP, LZ4 }
}
