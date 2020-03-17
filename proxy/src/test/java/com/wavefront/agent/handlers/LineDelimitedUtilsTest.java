package com.wavefront.agent.handlers;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.junit.Test;

import static org.junit.Assert.*;

/**
 * @author vasily@wavefront.com
 */
public class LineDelimitedUtilsTest {

  @Test
  public void testSplitStringIterator() {
    assertArrayEquals(new String[] {"str1"},
        toArray(LineDelimitedUtils.splitStringIterator("str1", '\n')));
    assertArrayEquals(new String[] {"str1", "str2", "str3"},
        toArray(LineDelimitedUtils.splitStringIterator("str1\nstr2\nstr3", '\n')));
    assertArrayEquals(new String[] {"str1", "str2", "str3"},
        toArray(LineDelimitedUtils.splitStringIterator("\nstr1\nstr2\n\nstr3\n\n", '\n')));
  }

  private String[] toArray(Iterator<String> iterator) {
    List<String> list = new ArrayList<>();
    iterator.forEachRemaining(list::add);
    return list.toArray(new String[list.size()]);
  }
}