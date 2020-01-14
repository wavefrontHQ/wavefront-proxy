package com.wavefront.agent.data;

import com.google.common.collect.ImmutableList;
import com.wavefront.data.ReportableEntityType;
import org.junit.Test;

import java.util.List;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;

/**
 * @author vasily@wavefront.com
 */
public class LineDelimitedDataSubmissionTaskTest {
  @Test
  public void testSplitTask() {
    LineDelimitedDataSubmissionTask task = new LineDelimitedDataSubmissionTask(null, null, null,
        null, "graphite_v2", ReportableEntityType.POINT, "2878",
        ImmutableList.of("A", "B", "C", "D", "E", "F", "G", "H", "I", "J", "K"), null);

    List<LineDelimitedDataSubmissionTask> split;

    // don't split if task is smaller than min split size
    split = task.splitTask(11, 4);
    assertEquals(1, split.size());
    assertSame(split.get(0), task);

    // split in 2
    split = task.splitTask(10, 11);
    assertEquals(2, split.size());
    assertArrayEquals(new String[] {"A", "B", "C", "D", "E", "F"}, split.get(0).payload.toArray());
    assertArrayEquals(new String[] {"G", "H", "I", "J", "K"}, split.get(1).payload.toArray());

    split = task.splitTask(10, 6);
    assertEquals(2, split.size());
    assertArrayEquals(new String[] {"A", "B", "C", "D", "E", "F"}, split.get(0).payload.toArray());
    assertArrayEquals(new String[] {"G", "H", "I", "J", "K"}, split.get(1).payload.toArray());

    // split in 3
    split = task.splitTask(10, 5);
    assertEquals(3, split.size());
    assertArrayEquals(new String[] {"A", "B", "C", "D", "E"}, split.get(0).payload.toArray());
    assertArrayEquals(new String[] {"F", "G", "H", "I", "J"}, split.get(1).payload.toArray());
    assertArrayEquals(new String[] {"K"}, split.get(2).payload.toArray());

    split = task.splitTask(7, 4);
    assertEquals(3, split.size());
    assertArrayEquals(new String[] {"A", "B", "C", "D"}, split.get(0).payload.toArray());
    assertArrayEquals(new String[] {"E", "F", "G", "H"}, split.get(1).payload.toArray());
    assertArrayEquals(new String[] {"I", "J", "K"}, split.get(2).payload.toArray());

    // split in 4
    split = task.splitTask(7, 3);
    assertEquals(4, split.size());
    assertArrayEquals(new String[] {"A", "B", "C"}, split.get(0).payload.toArray());
    assertArrayEquals(new String[] {"D", "E", "F"}, split.get(1).payload.toArray());
    assertArrayEquals(new String[] {"G", "H", "I"}, split.get(2).payload.toArray());
    assertArrayEquals(new String[] {"J", "K"}, split.get(3).payload.toArray());

    // split in 6
    split = task.splitTask(7, 2);
    assertEquals(6, split.size());
    assertArrayEquals(new String[] {"A", "B"}, split.get(0).payload.toArray());
    assertArrayEquals(new String[] {"C", "D"}, split.get(1).payload.toArray());
    assertArrayEquals(new String[] {"E", "F"}, split.get(2).payload.toArray());
    assertArrayEquals(new String[] {"G", "H"}, split.get(3).payload.toArray());
    assertArrayEquals(new String[] {"I", "J"}, split.get(4).payload.toArray());
    assertArrayEquals(new String[] {"K"}, split.get(5).payload.toArray());
  }
}
