package com.wavefront.agent.common;

import com.wavefront.common.EvictingRingBuffer;
import org.junit.Test;

import java.util.NoSuchElementException;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

/**
 * Tests for EvictingRingBuffer
 *
 * @author vasily@wavefront.com
 */
public class EvictingRingBufferTest {

  @Test
  public void testRingBufferWithAutoEvict() {
    EvictingRingBuffer<Integer> buf = new EvictingRingBuffer<>(5, false);
    assertEquals(5, buf.capacity());
    assertEquals(0, buf.size());
    assertArrayEquals(new Integer[0], buf.toList().toArray());
    buf.append(1);
    assertEquals(1, buf.size());
    assertArrayEquals(new Integer[] {1}, buf.toList().toArray());
    buf.append(2);
    assertEquals(2, buf.size());
    assertArrayEquals(new Integer[] {1, 2}, buf.toList().toArray());
    buf.append(3);
    assertEquals(3, buf.size());
    assertArrayEquals(new Integer[] {1, 2, 3}, buf.toList().toArray());
    // remove 1 element from head
    assertEquals(1, buf.remove().intValue());
    assertEquals(2, buf.size());
    assertArrayEquals(new Integer[] {2, 3}, buf.toList().toArray());
    buf.append(4);
    assertEquals(3, buf.size());
    assertArrayEquals(new Integer[] {2, 3, 4}, buf.toList().toArray());
    buf.append(5);
    assertEquals(4, buf.size());
    assertArrayEquals(new Integer[] {2, 3, 4, 5}, buf.toList().toArray());
    buf.append(6);
    assertEquals(5, buf.size());
    assertArrayEquals(new Integer[] {2, 3, 4, 5, 6}, buf.toList().toArray());
    // capacity reached, next append should evict the oldest value
    buf.append(7);
    assertEquals(5, buf.size());
    assertArrayEquals(new Integer[] {3, 4, 5, 6, 7}, buf.toList().toArray());
    assertEquals(3, buf.remove().intValue());
    assertEquals(4, buf.size());
    assertArrayEquals(new Integer[] {4, 5, 6, 7}, buf.toList().toArray());
    assertEquals(4, buf.remove().intValue());
    assertEquals(3, buf.size());
    assertArrayEquals(new Integer[] {5, 6, 7}, buf.toList().toArray());
    assertEquals(5, buf.remove().intValue());
    assertEquals(2, buf.size());
    assertArrayEquals(new Integer[] {6, 7}, buf.toList().toArray());
    buf.append(8);
    assertEquals(3, buf.size());
    assertArrayEquals(new Integer[] {6, 7, 8}, buf.toList().toArray());
    buf.append(9);
    assertEquals(4, buf.size());
    assertArrayEquals(new Integer[] {6, 7, 8, 9}, buf.toList().toArray());
    buf.append(10);
    assertEquals(5, buf.size());
    assertArrayEquals(new Integer[] {6, 7, 8, 9, 10}, buf.toList().toArray());
    buf.append(11);
    assertEquals(5, buf.size());
    assertArrayEquals(new Integer[] {7, 8, 9, 10, 11}, buf.toList().toArray());
    buf.append(12);
    assertEquals(5, buf.size());
    assertArrayEquals(new Integer[] {8, 9, 10, 11, 12}, buf.toList().toArray());
    buf.append(13);
    assertEquals(5, buf.size());
    assertArrayEquals(new Integer[] {9, 10, 11, 12, 13}, buf.toList().toArray());
    buf.append(14);
    assertEquals(5, buf.size());
    assertArrayEquals(new Integer[] {10, 11, 12, 13, 14}, buf.toList().toArray());
    buf.append(15);
    assertEquals(5, buf.size());
    assertArrayEquals(new Integer[] {11, 12, 13, 14, 15}, buf.toList().toArray());
    assertEquals(11, buf.remove().intValue());
    assertEquals(4, buf.size());
    assertArrayEquals(new Integer[] {12, 13, 14, 15}, buf.toList().toArray());
    assertEquals(12, buf.remove().intValue());
    assertEquals(3, buf.size());
    assertArrayEquals(new Integer[] {13, 14, 15}, buf.toList().toArray());
    assertEquals(13, buf.remove().intValue());
    assertEquals(2, buf.size());
    assertArrayEquals(new Integer[] {14, 15}, buf.toList().toArray());
    assertEquals(14, buf.remove().intValue());
    assertEquals(1, buf.size());
    assertArrayEquals(new Integer[] {15}, buf.toList().toArray());
    assertEquals(15, buf.remove().intValue());
    assertEquals(0, buf.size());
  }

  @Test
  public void testRingBufferWithDefaultValue() {
    EvictingRingBuffer<Integer> buf = new EvictingRingBuffer<>(5, false, 777);
    assertEquals(5, buf.capacity());
    assertEquals(5, buf.size());
    assertArrayEquals(new Integer[]{777, 777, 777, 777, 777}, buf.toList().toArray());
    buf.append(888);
    assertEquals(5, buf.size());
    assertArrayEquals(new Integer[]{777, 777, 777, 777, 888}, buf.toList().toArray());
    buf.append(999);
    assertEquals(5, buf.size());
    assertArrayEquals(new Integer[]{777, 777, 777, 888, 999}, buf.toList().toArray());
    assertEquals(777, buf.remove().intValue());
    assertEquals(777, buf.remove().intValue());
    assertEquals(777, buf.remove().intValue());
    assertArrayEquals(new Integer[]{888, 999}, buf.toList().toArray());
  }

  @Test
  public void testRingBufferWithStrictOverflowCheckingThrowsOnOverflow() {
    EvictingRingBuffer<Integer> buf = new EvictingRingBuffer<>(5, true);
    assertEquals(5, buf.capacity());
    assertEquals(0, buf.size());
    buf.append(1);
    buf.append(2);
    buf.append(3);
    buf.append(4);
    buf.append(5);
    try {
      buf.append(6); // should throw here
      fail();
    } catch (IllegalStateException e) {
      // ignore
    }
    assertEquals(1, buf.remove().intValue());
    buf.append(7);
    assertEquals(2, buf.remove().intValue());
    assertEquals(3, buf.remove().intValue());
    assertEquals(4, buf.remove().intValue());
    assertEquals(5, buf.remove().intValue());
    assertEquals(7, buf.remove().intValue());
  }

  @Test
  public void testRingBufferRemoveOnEmptyThrows() {
    EvictingRingBuffer<Integer> buf = new EvictingRingBuffer<>(5, true);
    assertEquals(0, buf.size());
    assertEquals(5, buf.capacity());
    buf.append(1);
    buf.remove();
    try {
      buf.remove(); // should throw here
      fail();
    } catch (NoSuchElementException e) {
      // ignore
    }
  }

  @Test
  public void testRingBufferAccessByIndex() {
    EvictingRingBuffer<Integer> buf = new EvictingRingBuffer<>(3);
    assertEquals(0, buf.size());
    assertEquals(3, buf.capacity());
    try {
      buf.get(0);
      fail();
    } catch (IndexOutOfBoundsException e) {
      // ignore
    }
    buf.append(1);
    buf.get(0);
    try {
      buf.get(1);
      fail();
    } catch (IndexOutOfBoundsException e) {
      // ignore
    }
    buf.append(2);
    buf.get(0);
    buf.get(1);
    try {
      buf.get(2);
      fail();
    } catch (IndexOutOfBoundsException e) {
      // ignore
    }
    buf.remove();
    buf.get(0);
    try {
      buf.get(1);
      fail();
    } catch (IndexOutOfBoundsException e) {
      // ignore
    }
    buf.append(3);
    buf.get(0);
    buf.get(1);
    try {
      buf.get(2);
      fail();
    } catch (IndexOutOfBoundsException e) {
      // ignore
    }
    buf.append(4);
    buf.get(0);
    buf.get(1);
    buf.get(2);
    try {
      buf.get(3);
      fail();
    } catch (IndexOutOfBoundsException e) {
      // ignore
    }
    buf.append(5);
    buf.get(0);
    buf.get(1);
    buf.get(2);
    try {
      buf.get(3);
      fail();
    } catch (IndexOutOfBoundsException e) {
      // ignore
    }
  }
}
