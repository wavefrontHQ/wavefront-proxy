package com.wavefront.common;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * A basic ring buffer with an ability to evict values on overflow.
 *
 * @param <T> type of objects stored
 *
 * @author vasily@wavefont.com
 */
public class EvictingRingBuffer<T> {
  private final List<T> buffer;
  private final int bufferSize;
  private int headPtr;
  private int tailPtr;
  private final boolean strict;

  /**
   * @param capacity desired capacity
   */
  public EvictingRingBuffer(int capacity) {
    this(capacity, false, null, false);
  }

  /**
   * @param capacity desired capacity
   * @param strict   Disables auto-eviction on overflow. When full capacity is
   *                 reached, all subsequent append() operations would throw
   *                 {@link IllegalStateException} if this parameter is true,
   *                 or evict the oldest value if this parameter is false.
   */
  public EvictingRingBuffer(int capacity, boolean strict) {
    this(capacity, strict, null, false);
  }

  /**
   * @param capacity     desired capacity
   * @param strict       disables auto-eviction on overflow. When full capacity is
   *                     reached, all subsequent append() operations would throw
   *                     {@link IllegalStateException} if this parameter is true,
   *                     or evict the oldest value if this parameter is false.
   * @param defaultValue pre-fill the buffer with this default value
   */
  public EvictingRingBuffer(int capacity, boolean strict, @Nullable T defaultValue) {
    this(capacity, strict, defaultValue, true);
  }

  private EvictingRingBuffer(int capacity, boolean strict, @Nullable T defaultValue,
                             boolean preFill) {
    this.buffer = new ArrayList<>(Collections.nCopies(capacity + 1, defaultValue));
    this.buffer.set(0, null);
    this.bufferSize = capacity + 1;
    this.strict = strict;
    this.headPtr = 0;
    this.tailPtr = preFill ? capacity : 0;
  }

  /**
   * Returns buffer capacity (i.e. max number of elements this buffer can hold).
   *
   * @return buffer capacity
   */
  public int capacity() {
    return bufferSize - 1;
  }

  /**
   * Returns number of elements in the buffer.
   *
   * @return number of elements
   */
  public int size() {
    return tailPtr - headPtr + (tailPtr < headPtr ? bufferSize : 0);
  }

  /**
   * Return the element at the specified position in the buffer.
   *
   * @param index index of the element to return
   * @return the element at the specified position in the buffer
   * @throws IndexOutOfBoundsException if the index is out of range
   *         ({@code index < 0 || index >= size()})
   */
  public T get(int index) {
    if (index < 0 || index >= size()) {
      throw new IndexOutOfBoundsException("Index out of bounds: " + index +
          ", expected: [0; " + size() + ")");
    }
    return buffer.get(wrap(headPtr + index + 1));
  }

  /**
   * Add a value at the end of the ring buffer.
   *
   * @param value element to be appended to the end of the buffer
   */
  public void append(T value) {
    if (size() == bufferSize - 1) {
      if (strict) {
        throw new IllegalStateException("Buffer capacity exceeded: " + (bufferSize - 1));
      } else {
        // evict oldest value
        headPtr = wrap(headPtr + 1);
        buffer.set(headPtr, null); // to allow evicted value to be GC'd
      }
    }
    tailPtr = wrap(tailPtr + 1);
    buffer.set(tailPtr, value);
  }

  /**
   * Returns a {@code List<T>} containing all the elements in the buffer
   * in proper sequence (first to last element).
   *
   * @return a {@code List<T>} containing all the elements in the buffer
   *         in proper sequence
   */
  public List<T> toList() {
    if (tailPtr == headPtr) {
      return Collections.emptyList();
    } else if (tailPtr > headPtr) {
      return Collections.unmodifiableList(buffer.subList(headPtr + 1, tailPtr + 1));
    } else {
      return Collections.unmodifiableList(Stream.concat(
          buffer.subList(headPtr + 1, bufferSize).stream(),
          buffer.subList(0, tailPtr + 1).stream()).collect(Collectors.toList()));
    }
  }

  /**
   * Retrieves and removes an item from the head of the buffer.
   *
   * @return removed element
   * @throws NoSuchElementException if buffer is empty
   */
  public T remove() {
    if (size() == 0) throw new NoSuchElementException("No elements available");
    T t = get(0);
    headPtr = wrap(headPtr + 1);
    buffer.set(headPtr, null); // to allow removed value to be GC'd
    return t;
  }

  private int wrap(int index) {
    int m = index % bufferSize;
    return m < 0 ? m + bufferSize : m;
  }
}