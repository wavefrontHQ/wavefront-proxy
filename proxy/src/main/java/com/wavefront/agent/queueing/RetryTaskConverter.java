package com.wavefront.agent.queueing;

import com.squareup.tape2.ObjectQueue;
import com.wavefront.agent.data.DataSubmissionTask;
import com.wavefront.common.TaggedMetricName;
import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.Counter;

import net.jpountz.lz4.LZ4BlockInputStream;
import net.jpountz.lz4.LZ4BlockOutputStream;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.util.logging.Logger;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 *
 * @param <T>
 *
 * @author vasily@wavefront.com
 */
public class RetryTaskConverter<T extends DataSubmissionTask> implements ObjectQueue.Converter<T> {
  private static final Logger logger = Logger.getLogger(
      RetryTaskConverter.class.getCanonicalName());

  private final String handle;
  private final boolean disableCompression;
  private final Counter errorCounter;

  RetryTaskConverter(String handle, boolean disableCompression) {
    this.handle = handle;
    this.disableCompression = disableCompression;
    this.errorCounter = Metrics.newCounter(new TaggedMetricName("buffer", "read-errors",
        "port", handle));
  }

  @Nullable
  @Override
  public T from(@Nonnull byte[] bytes) throws IOException {
    try {
      ObjectInputStream ois;
      if (disableCompression) {
        ois = new ObjectInputStream(new ByteArrayInputStream(bytes));
      } else {
        ois = new ObjectInputStream(new LZ4BlockInputStream(new ByteArrayInputStream(bytes)));
      }
      //noinspection unchecked
      return (T) ois.readObject();
    } catch (Throwable t) {
      logger.warning("Failed to read a single retry submission from buffer, ignoring: " + t);
      errorCounter.inc();
      return null;
    }
  }

  @Override
  public void toStream(@Nonnull T t, @Nonnull OutputStream bytes) throws IOException {
    if (disableCompression) {
      ObjectOutputStream oos = new ObjectOutputStream(bytes);
      oos.writeObject(t);
      oos.close();
    } else {
      LZ4BlockOutputStream lz4BlockOutputStream = new LZ4BlockOutputStream(bytes);
      ObjectOutputStream oos = new ObjectOutputStream(lz4BlockOutputStream);
      oos.writeObject(t);
      oos.close();
      lz4BlockOutputStream.close();
    }
  }
}
