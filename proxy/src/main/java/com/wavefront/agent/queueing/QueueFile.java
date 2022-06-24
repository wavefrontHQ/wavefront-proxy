package com.wavefront.agent.queueing;

import java.io.Closeable;
import java.io.IOException;
import java.util.NoSuchElementException;
import javax.annotation.Nullable;

/**
 * Proxy-specific FIFO queue interface for storing {@code byte[]}. This allows us to potentially
 * support multiple backing storages in the future.
 *
 * @author vasily@wavefront.com
 */
public interface QueueFile extends Closeable, Iterable<byte[]> {
  /**
   * Adds an element to the end of the queue.
   *
   * @param data to copy bytes from
   */
  default void add(byte[] data) throws IOException {
    add(data, 0, data.length);
  }

  /**
   * Adds an element to the end of the queue.
   *
   * @param data to copy bytes from
   * @param offset to start from in buffer
   * @param count number of bytes to copy
   * @throws IndexOutOfBoundsException if {@code offset < 0} or {@code count < 0}, or if {@code
   *     offset + count} is bigger than the length of {@code buffer}.
   */
  void add(byte[] data, int offset, int count) throws IOException;

  /** Clears this queue. Truncates the file to the initial size. */
  void clear() throws IOException;

  /**
   * Checks whether this queue is empty.
   *
   * @return true if this queue contains no entries
   */
  default boolean isEmpty() {
    return size() == 0;
  }

  /**
   * Reads the eldest element. Returns null if the queue is empty.
   *
   * @return the eldest element.
   */
  @Nullable
  byte[] peek() throws IOException;

  /**
   * Removes the eldest element.
   *
   * @throws NoSuchElementException if the queue is empty
   */
  void remove() throws IOException;

  /** Returns the number of elements in this queue. */
  int size();

  /**
   * Returns the storage size (on-disk file size) in bytes.
   *
   * @return file size in bytes.
   */
  long storageBytes();

  /**
   * Returns the number of bytes used for data.
   *
   * @return bytes used.
   */
  long usedBytes();

  /**
   * Returns the number of bytes available for adding new tasks without growing the file.
   *
   * @return bytes available.
   */
  long availableBytes();
}
