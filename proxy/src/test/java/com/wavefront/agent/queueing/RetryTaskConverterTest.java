package com.wavefront.agent.queueing;

import static org.junit.Assert.assertNull;

import com.google.common.collect.ImmutableList;
import com.wavefront.agent.data.DefaultEntityPropertiesForTesting;
import com.wavefront.agent.data.LineDelimitedDataSubmissionTask;
import com.wavefront.agent.handlers.HandlerKey;
import com.wavefront.data.ReportableEntityType;
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
            new HandlerKey(ReportableEntityType.POINT, "2878"),
            ImmutableList.of("item1", "item2", "item3"),
            () -> 12345L);
    RetryTaskConverter<LineDelimitedDataSubmissionTask> converter =
        new RetryTaskConverter<>("2878", RetryTaskConverter.CompressionType.NONE);

    assertNull(converter.fromBytes(new byte[] {0, 0, 0}));
    assertNull(converter.fromBytes(new byte[] {'W', 'F', 0}));
    assertNull(converter.fromBytes(new byte[] {'W', 'F', 1}));
    assertNull(converter.fromBytes(new byte[] {'W', 'F', 1, 0}));
  }
}
