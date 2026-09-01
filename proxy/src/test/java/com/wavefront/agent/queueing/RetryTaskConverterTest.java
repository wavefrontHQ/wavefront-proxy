package com.wavefront.agent.queueing;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import com.google.common.collect.ImmutableList;
import com.wavefront.agent.data.DefaultEntityPropertiesForTesting;
import com.wavefront.agent.data.LineDelimitedDataSubmissionTask;
import com.wavefront.data.ReportableEntityType;
import java.io.ByteArrayOutputStream;
import java.io.UnsupportedEncodingException;
import java.util.UUID;
import org.junit.Test;

public class RetryTaskConverterTest {

  @Test
  public void testTaskSerialize() {
    UUID proxyId = UUID.randomUUID();
    LineDelimitedDataSubmissionTask task =
        new LineDelimitedDataSubmissionTask(
            null,
            proxyId,
            new DefaultEntityPropertiesForTesting(),
            null,
            "wavefront",
            ReportableEntityType.POINT,
            "2878",
            ImmutableList.of("item1", "item2", "item3"),
            () -> 12345L);
    RetryTaskConverter<LineDelimitedDataSubmissionTask> converter =
        new RetryTaskConverter<>("2878", RetryTaskConverter.CompressionType.NONE);

    assertNull(converter.fromBytes(new byte[] {0, 0, 0}));
    assertNull(converter.fromBytes(new byte[] {'W', 'F', 0}));
    assertNull(converter.fromBytes(new byte[] {'W', 'F', 1}));
    assertNull(converter.fromBytes(new byte[] {'W', 'F', 1, 0}));
  }

  @Test
  public void testTaskRoundTrip() throws Exception {
    LineDelimitedDataSubmissionTask task =
        new LineDelimitedDataSubmissionTask(
            null,
            UUID.randomUUID(),
            new DefaultEntityPropertiesForTesting(),
            null,
            "wavefront",
            ReportableEntityType.POINT,
            "2878",
            ImmutableList.of("item1", "item2", "item3"),
            () -> 12345L);
    RetryTaskConverter<LineDelimitedDataSubmissionTask> converter =
        new RetryTaskConverter<>("2878", RetryTaskConverter.CompressionType.NONE);

    ByteArrayOutputStream out = new ByteArrayOutputStream();
    converter.serializeToStream(task, out);
    LineDelimitedDataSubmissionTask restored = converter.fromBytes(out.toByteArray());
    assertEquals(task.payload(), restored.payload());
  }

  @Test
  public void testMaliciousClassIdIsRejected() throws UnsupportedEncodingException {
    // a persisted/queued blob whose "__CLASS" points at a type that isn't a DataSubmissionTask
    // must be rejected rather than instantiated, however innocuous-looking the named class is.
    String json = "{\"__CLASS\":\"java.util.HashMap\"}";
    byte[] jsonBytes = json.getBytes("UTF-8");
    byte[] bytes = new byte[4 + jsonBytes.length];
    bytes[0] = 'W';
    bytes[1] = 'F';
    bytes[2] = 1; // header length
    bytes[3] = RetryTaskConverter.FORMAT_RAW;
    System.arraycopy(jsonBytes, 0, bytes, 4, jsonBytes.length);

    RetryTaskConverter<LineDelimitedDataSubmissionTask> converter =
        new RetryTaskConverter<>("2878", RetryTaskConverter.CompressionType.NONE);
    assertNull(converter.fromBytes(bytes));
  }
}
