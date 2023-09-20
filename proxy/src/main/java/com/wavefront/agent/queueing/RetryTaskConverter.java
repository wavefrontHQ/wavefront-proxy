package com.wavefront.agent.queueing;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.json.JsonMapper;
import com.fasterxml.jackson.databind.jsontype.impl.LaissezFaireSubTypeValidator;
import com.wavefront.agent.data.DataSubmissionTask;
import com.wavefront.common.TaggedMetricName;
import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.Counter;
import org.apache.commons.compress.compressors.lz4.BlockLZ4CompressorInputStream;
import org.apache.commons.compress.compressors.lz4.BlockLZ4CompressorOutputStream;
import org.apache.commons.io.IOUtils;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
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
public class RetryTaskConverter<T extends DataSubmissionTask<T>> implements TaskConverter<T> {
  private static final Logger logger =
      Logger.getLogger(RetryTaskConverter.class.getCanonicalName());

  static final byte[] TASK_HEADER = new byte[] {'W', 'F'};
  static final byte FORMAT_RAW = 1;
  static final byte FORMAT_GZIP = 2;
  static final byte FORMAT_LZ4_OLD = 3;
  static final byte WRAPPED = 4;
  static final byte FORMAT_LZ4 = 5;
  static final byte[] PREFIX = {'W', 'F', 6, 4};

  private final ObjectMapper objectMapper =
      JsonMapper.builder().activateDefaultTyping(LaissezFaireSubTypeValidator.instance).build();

  private final CompressionType compressionType;
  private final Counter errorCounter;

  /**
   * @param handle Handle (usually port number) of the pipeline where the data came from.
   * @param compressionType compression type to use for storing tasks.
   */
  public RetryTaskConverter(String handle, CompressionType compressionType) {
    this.compressionType = compressionType;
    this.errorCounter =
        Metrics.newCounter(new TaggedMetricName("buffer", "read-errors", "port", handle));
  }

  @SuppressWarnings("unchecked")
  @Nullable
  @Override
  public T fromBytes(@Nonnull byte[] bytes) {
    ByteArrayInputStream input = new ByteArrayInputStream(bytes);
    int len = TASK_HEADER.length;
    byte[] prefix = new byte[len];
    if (input.read(prefix, 0, len) == len && Arrays.equals(prefix, TASK_HEADER)) {
      int bytesToRead = input.read();
      if (bytesToRead > 0) {
        byte[] header = new byte[bytesToRead];
        if (input.read(header, 0, bytesToRead) == bytesToRead) {
          InputStream stream = null;
          byte compression = header[0] == WRAPPED && bytesToRead > 1 ? header[1] : header[0];
          try {
            switch (compression) {
              case FORMAT_LZ4_OLD:
                input.skip(21); // old lz4 header, not need with the apache commons implementation
                stream = new BlockLZ4CompressorInputStream(input);
                break;
              case FORMAT_LZ4:
                stream = new BlockLZ4CompressorInputStream(input);
                break;
              case FORMAT_GZIP:
                stream = new GZIPInputStream(input);
                break;
              case FORMAT_RAW:
                stream = input;
                break;
              default:
                logger.warning(
                    "Unable to restore persisted task - unsupported data format "
                        + "header detected: "
                        + Arrays.toString(header));
                return null;
            }
            return (T) objectMapper.readValue(stream, DataSubmissionTask.class);
          } catch (Throwable t) {
            logger.warning("Unable to restore persisted task: " + t);
          } finally {
            IOUtils.closeQuietly(stream);
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
  public void serializeToStream(@Nonnull T t, @Nonnull OutputStream bytes) throws IOException {
    bytes.write(TASK_HEADER);
    // 6 bytes: 1 for WRAPPED, 1 for compression method, 4 for task weight (integer)
    bytes.write(6);
    bytes.write(WRAPPED);
    switch (compressionType) {
      case LZ4:
        bytes.write(FORMAT_LZ4);
        bytes.write(ByteBuffer.allocate(4).putInt(t.weight()).array());
        BlockLZ4CompressorOutputStream lz4BlockOutputStream =
            new BlockLZ4CompressorOutputStream(bytes);
        objectMapper.writeValue(lz4BlockOutputStream, t);
        lz4BlockOutputStream.close();
        return;
      case GZIP:
        bytes.write(FORMAT_GZIP);
        bytes.write(ByteBuffer.allocate(4).putInt(t.weight()).array());
        GZIPOutputStream gzipOutputStream = new GZIPOutputStream(bytes);
        objectMapper.writeValue(gzipOutputStream, t);
        gzipOutputStream.close();
        return;
      case NONE:
        bytes.write(FORMAT_RAW);
        bytes.write(ByteBuffer.allocate(4).putInt(t.weight()).array());
        objectMapper.writeValue(bytes, t);
    }
  }

  @Nullable
  @Override
  public Integer getWeight(@Nonnull byte[] bytes) {
    if (bytes.length > 8 && Arrays.equals(Arrays.copyOf(bytes, PREFIX.length), PREFIX)) {
      // take a shortcut - reconstruct an integer from bytes 5 thru 7
      return bytes[5] << 24 | (bytes[6] & 0xFF) << 16 | (bytes[7] & 0xFF) << 8 | (bytes[8] & 0xFF);
    } else {
      T t = fromBytes(bytes);
      if (t == null) return null;
      return t.weight();
    }
  }
}
