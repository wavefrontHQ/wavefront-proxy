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
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Arrays;
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

  static final byte[] TASK_HEADER = new byte[] { 'W', 'F' };
  static final byte[] FORMAT_RAW = new byte[] { 1 };
  static final byte[] FORMAT_GZIP = new byte[] { 2 };
  static final byte[] FORMAT_LZ4 = new byte[] { 3 };

  private final ObjectMapper objectMapper = new ObjectMapper();

  private final CompressionType compressionType;
  private final Counter errorCounter;

  /**
   * @param handle          Handle (usually port number) of the pipeline where the data came from.
   * @param compressionType compression type to use for storing tasks.
   */
  public RetryTaskConverter(String handle, CompressionType compressionType) {
    objectMapper.enableDefaultTyping();
    this.compressionType = compressionType;
    this.errorCounter = Metrics.newCounter(new TaggedMetricName("buffer", "read-errors",
        "port", handle));
  }

  @SuppressWarnings("unchecked")
  @Nullable
  @Override
  public T from(@Nonnull byte[] bytes) {
    ByteArrayInputStream input = new ByteArrayInputStream(bytes);
    int len = TASK_HEADER.length;
    byte[] prefix = new byte[len];
    if (input.read(prefix, 0, len) == len && Arrays.equals(prefix, TASK_HEADER)) {
      int bytesToRead = input.read();
      if (bytesToRead > 0) {
        byte[] format = new byte[bytesToRead];
        if (input.read(format, 0, bytesToRead) == bytesToRead) {
          try {
            InputStream stream;
            if (Arrays.equals(format, FORMAT_LZ4)) { // LZ4 compression
              stream = new LZ4BlockInputStream(input);
            } else if (Arrays.equals(format, FORMAT_GZIP)) { // GZIP compression
              stream = new GZIPInputStream(input);
            } else if (Arrays.equals(format, FORMAT_RAW)) { // no compression
              stream = input;
            } else {
              logger.warning("Unable to restore persisted task - unsupported data format " +
                  "header detected: " + Arrays.toString(format));
              return null;
            }
            return (T) objectMapper.readValue(stream, DataSubmissionTask.class);
          } catch (Throwable t) {
            logger.warning("Unable to restore persisted task: " + t);
          }
        } else {
          logger.warning("Unable to restore persisted task - corrupted header, ignoring");
        }
      } else {
        logger.warning("Unable to restore persisted task - missing header, ignoring");
      }
    } else {
      logger.warning("Unable to restore persisted task - invalid or missing header, ignoring");
    }
    errorCounter.inc();
    return null;
  }

  @Override
  public void toStream(@Nonnull T t, @Nonnull OutputStream bytes) throws IOException {
    bytes.write(TASK_HEADER);
    switch (compressionType) {
      case LZ4:
        bytes.write((byte) FORMAT_LZ4.length);
        bytes.write(FORMAT_LZ4);
        LZ4BlockOutputStream lz4BlockOutputStream = new LZ4BlockOutputStream(bytes);
        objectMapper.writeValue(lz4BlockOutputStream, t);
        lz4BlockOutputStream.close();
        return;
      case GZIP:
        bytes.write((byte) FORMAT_GZIP.length);
        bytes.write(FORMAT_GZIP);
        GZIPOutputStream gzipOutputStream = new GZIPOutputStream(bytes);
        objectMapper.writeValue(gzipOutputStream, t);
        gzipOutputStream.close();
        return;
      case NONE:
        bytes.write((byte) FORMAT_RAW.length);
        bytes.write(FORMAT_RAW);
        objectMapper.writeValue(bytes, t);
    }
  }

  public enum CompressionType { NONE, GZIP, LZ4 }
}
