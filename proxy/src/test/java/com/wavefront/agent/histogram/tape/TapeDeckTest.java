package com.wavefront.agent.histogram.tape;


import com.google.common.collect.ImmutableList;

import com.squareup.tape.ObjectQueue;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static com.google.common.truth.Truth.assertThat;

/**
 * Unit tests around {@link TapeDeck}
 *
 * @author Tim Schmidt (tim@wavefront.com).
 */
public class TapeDeckTest {
  private TapeDeck<List<String>> deck;
  private File file;

  @Before
  public void setup() {
    try {
      file = File.createTempFile("test-file", ".tmp");
      file.deleteOnExit();
    } catch (IOException e) {
      e.printStackTrace();
    }
    deck = new TapeDeck<>(TapeStringListConverter.getDefaultInstance(), true);
  }

  @After
  public void cleanup() {
    file.delete();
  }

  private void testTape(ObjectQueue<List<String>> tape) {
    final AtomicInteger addCalls = new AtomicInteger(0);
    final AtomicInteger removeCalls = new AtomicInteger(0);

    ObjectQueue.Listener<List<String>> listener = new ObjectQueue.Listener<List<String>>() {
      @Override
      public void onAdd(ObjectQueue<List<String>> objectQueue, List<String> strings) {
        addCalls.incrementAndGet();
      }

      @Override
      public void onRemove(ObjectQueue<List<String>> objectQueue) {
        removeCalls.incrementAndGet();
      }
    };

    tape.setListener(listener);
    assertThat(tape).isNotNull();
    tape.add(ImmutableList.of("test"));
    assertThat(tape.size()).isEqualTo(1);
    assertThat(tape.peek()).containsExactly("test");
    assertThat(addCalls.get()).isEqualTo(1);
    assertThat(removeCalls.get()).isEqualTo(0);
    tape.remove();
    assertThat(addCalls.get()).isEqualTo(1);
    assertThat(removeCalls.get()).isEqualTo(1);
  }

  @Test
  public void testFileDoNotPersist() throws IOException {
    file.delete();
    deck = new TapeDeck<>(TapeStringListConverter.getDefaultInstance(), false);

    ObjectQueue<List<String>> q = deck.getTape(file);
    testTape(q);
  }

  @Test
  public void testFileDoesNotExist() throws IOException {
    file.delete();
    ObjectQueue<List<String>> q = deck.getTape(file);
    testTape(q);
  }
}
