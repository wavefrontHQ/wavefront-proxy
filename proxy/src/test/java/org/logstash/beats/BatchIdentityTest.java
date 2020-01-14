package org.logstash.beats;

import com.google.common.collect.ImmutableMap;

import org.junit.Test;

import javax.annotation.Nonnull;
import java.util.Collections;
import java.util.Iterator;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

/**
 * @author vasily@wavefront.com.
 */
public class BatchIdentityTest {

  @Test
  public void testEquals() {
    assertEquals(new BatchIdentity("2019-09-17T01:02:03.123Z", 6, 5, null, null),
        new BatchIdentity("2019-09-17T01:02:03.123Z", 6, 5, null, null));
    assertEquals(new BatchIdentity("2019-09-17T01:02:03.123Z", 1, 5, "test.log", 123),
        new BatchIdentity("2019-09-17T01:02:03.123Z", 1, 5, "test.log", 123));
    assertNotEquals(new BatchIdentity("2019-09-17T01:02:03.123Z", 1, 5, "test.log", 123),
        new BatchIdentity("2019-09-16T01:02:03.123Z", 1, 5, "test.log", 123));
    assertNotEquals(new BatchIdentity("2019-09-17T01:02:03.123Z", 1, 5, "test.log", 123),
        new BatchIdentity("2019-09-17T01:02:03.123Z", 2, 5, "test.log", 123));
    assertNotEquals(new BatchIdentity("2019-09-17T01:02:03.123Z", 1, 5, "test.log", 123),
        new BatchIdentity("2019-09-17T01:02:03.123Z", 1, 4, "test.log", 123));
    assertNotEquals(new BatchIdentity("2019-09-17T01:02:03.123Z", 1, 5, null, 123),
        new BatchIdentity("2019-09-17T01:02:03.123Z", 1, 5, "test.log", 123));
    assertNotEquals(new BatchIdentity("2019-09-17T01:02:03.123Z", 1, 5, null, null),
        new BatchIdentity("2019-09-17T01:02:03.123Z", 1, 5, null, 123));
  }

  @Test
  public void testCreateFromMessage() {
    Message message = new Message(101, ImmutableMap.builder().
        put("@metadata", ImmutableMap.of("beat", "filebeat", "type", "_doc", "version", "7.3.1")).
        put("@timestamp", "2019-09-17T01:02:03.123Z").
        put("input", ImmutableMap.of("type", "log")).
        put("message", "This is a log line #1").
        put("host", ImmutableMap.of("name", "host1.acme.corp", "hostname", "host1.acme.corp",
            "id", "6DF46E56-37A3-54F8-9541-74EC4DE13483")).
        put("log", ImmutableMap.of("offset", 6599, "file", ImmutableMap.of("path", "test.log"))).
        put("agent", ImmutableMap.of("id", "30ff3498-ae71-41e3-bbcb-4a39352da0fe",
            "version", "7.3.1", "type", "filebeat", "hostname", "host1.acme.corp",
            "ephemeral_id", "fcb2e75f-859f-4706-9f14-a16dc1965ff1")).build());
    Message message2 = new Message(102, ImmutableMap.builder().
        put("@metadata", ImmutableMap.of("beat", "filebeat", "type", "_doc", "version", "7.3.1")).
        put("@timestamp", "2019-09-17T01:02:04.123Z").
        put("input", ImmutableMap.of("type", "log")).
        put("message", "This is a log line #2").
        put("host", ImmutableMap.of("name", "host1.acme.corp", "hostname", "host1.acme.corp",
            "id", "6DF46E56-37A3-54F8-9541-74EC4DE13483")).
        put("log", ImmutableMap.of("offset", 6799, "file", ImmutableMap.of("path", "test.log"))).
        put("agent", ImmutableMap.of("id", "30ff3498-ae71-41e3-bbcb-4a39352da0fe",
            "version", "7.3.1", "type", "filebeat", "hostname", "host1.acme.corp",
            "ephemeral_id", "fcb2e75f-859f-4706-9f14-a16dc1965ff1")).build());
    Batch batch = new Batch() {
      @Override
      public byte getProtocol() {
        return 0;
      }

      @Override
      public int getBatchSize() {
        return 2;
      }

      @Override
      public void setBatchSize(int batchSize) {
      }

      @Override
      public int getHighestSequence() {
        return 102;
      }

      @Override
      public int size() {
        return 2;
      }

      @Override
      public boolean isEmpty() {
        return false;
      }

      @Override
      public boolean isComplete() {
        return true;
      }

      @Override
      public void release() {
      }

      @Nonnull
      @Override
      public Iterator<Message> iterator() {
        return Collections.emptyIterator();
      }
    };
    message.setBatch(batch);
    message2.setBatch(batch);
    String key = BatchIdentity.keyFrom(message);
    BatchIdentity identity = BatchIdentity.valueFrom(message);
    assertEquals("30ff3498-ae71-41e3-bbcb-4a39352da0fe", key);
    assertEquals(new BatchIdentity("2019-09-17T01:02:03.123Z", 102, 2, "test.log", 6599),
        identity);
  }
}