package com.wavefront.agent.queueing;

import com.wavefront.common.TimeProvider;
import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Iterator;
import java.util.function.BiConsumer;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * A {@link com.squareup.tape2.QueueFile} to {@link QueueFile} adapter.
 *
 * @author vasily@wavefront.com
 */
public class TapeQueueFile implements QueueFile {
  private static final Method usedBytes;
  private static final Field fileLength;

  static {
    try {
      Class<?> classQueueFile = Class.forName("com.squareup.tape2.QueueFile");
      usedBytes = classQueueFile.getDeclaredMethod("usedBytes");
      usedBytes.setAccessible(true);
      fileLength = classQueueFile.getDeclaredField("fileLength");
      fileLength.setAccessible(true);
    } catch (ClassNotFoundException | NoSuchMethodException | NoSuchFieldException e) {
      throw new AssertionError(e);
    }
  }

  private final com.squareup.tape2.QueueFile delegate;
  @Nullable private final BiConsumer<Integer, Long> writeStatsConsumer;
  private final TimeProvider clock;

  /** @param delegate tape queue file */
  public TapeQueueFile(com.squareup.tape2.QueueFile delegate) {
    this(delegate, null, null);
  }

  /**
   * @param delegate tape queue file
   * @param writeStatsConsumer consumer for statistics on writes (bytes written and millis taken)
   */
  public TapeQueueFile(
      com.squareup.tape2.QueueFile delegate,
      @Nullable BiConsumer<Integer, Long> writeStatsConsumer) {
    this(delegate, writeStatsConsumer, null);
  }

  /**
   * @param delegate tape queue file
   * @param writeStatsConsumer consumer for statistics on writes (bytes written and millis taken)
   * @param clock time provider (in millis)
   */
  public TapeQueueFile(
      com.squareup.tape2.QueueFile delegate,
      @Nullable BiConsumer<Integer, Long> writeStatsConsumer,
      @Nullable TimeProvider clock) {
    this.delegate = delegate;
    this.writeStatsConsumer = writeStatsConsumer;
    this.clock = clock == null ? System::currentTimeMillis : clock;
  }

  @Override
  public void add(byte[] data, int offset, int count) throws IOException {
    long startTime = clock.currentTimeMillis();
    delegate.add(data, offset, count);
    if (writeStatsConsumer != null) {
      writeStatsConsumer.accept(count, clock.currentTimeMillis() - startTime);
    }
  }

  @Override
  public boolean isEmpty() {
    return delegate.isEmpty();
  }

  @Override
  @Nullable
  public byte[] peek() throws IOException {
    return delegate.peek();
  }

  @Nonnull
  @Override
  public Iterator<byte[]> iterator() {
    return delegate.iterator();
  }

  @Override
  public int size() {
    return delegate.size();
  }

  @Override
  public long storageBytes() {
    try {
      return (long) fileLength.get(delegate);
    } catch (IllegalAccessException e) {
      return 0;
    }
  }

  @Override
  public long usedBytes() {
    try {
      return (long) usedBytes.invoke(delegate);
    } catch (InvocationTargetException | IllegalAccessException e) {
      return 0;
    }
  }

  @Override
  public long availableBytes() {
    return storageBytes() - usedBytes();
  }

  @Override
  public void remove() throws IOException {
    delegate.remove();
  }

  @Override
  public void clear() throws IOException {
    delegate.clear();
  }

  @Override
  public void close() throws IOException {
    delegate.close();
  }
}
