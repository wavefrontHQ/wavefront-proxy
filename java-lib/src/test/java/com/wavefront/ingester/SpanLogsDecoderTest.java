package com.wavefront.ingester;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.Test;
import wavefront.report.SpanLogs;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

/**
 * Tests for SpanLogsDecoder
 *
 * @author zhanghan@vmware.com
 */
public class SpanLogsDecoderTest {

  private final SpanLogsDecoder decoder = new SpanLogsDecoder();
  private final ObjectMapper jsonParser = new ObjectMapper();

  @Test
  public void testDecodeWithoutSpanSecondaryId() throws IOException {
    List<SpanLogs> out = new ArrayList<>();
    String msg = "{" +
        "\"customer\":\"default\"," +
        "\"traceId\":\"7b3bf470-9456-11e8-9eb6-529269fb1459\"," +
        "\"spanId\":\"0313bafe-9457-11e8-9eb6-529269fb1459\"," +
        "\"logs\":[{\"timestamp\":1554363517965,\"fields\":{\"event\":\"error\",\"error.kind\":\"exception\"}}]}";

    decoder.decode(jsonParser.readTree(msg), out, "testCustomer");
    assertEquals(1, out.size());
    assertEquals("testCustomer", out.get(0).getCustomer());
    assertEquals("7b3bf470-9456-11e8-9eb6-529269fb1459", out.get(0).getTraceId());
    assertEquals("0313bafe-9457-11e8-9eb6-529269fb1459", out.get(0).getSpanId());
    assertNull(out.get(0).getSpanSecondaryId());
    assertEquals(1, out.get(0).getLogs().size());
    assertEquals(1554363517965L, out.get(0).getLogs().get(0).getTimestamp().longValue());
    assertEquals(2, out.get(0).getLogs().get(0).getFields().size());
    assertEquals("error", out.get(0).getLogs().get(0).getFields().get("event"));
    assertEquals("exception", out.get(0).getLogs().get(0).getFields().get("error.kind"));
  }

  @Test
  public void testDecodeWithSpanSecondaryId() throws IOException {
    List<SpanLogs> out = new ArrayList<>();
    String msg = "{" +
        "\"customer\":\"default\"," +
        "\"traceId\":\"7b3bf470-9456-11e8-9eb6-529269fb1459\"," +
        "\"spanId\":\"0313bafe-9457-11e8-9eb6-529269fb1459\"," +
        "\"spanSecondaryId\":\"server\"," +
        "\"logs\":[{\"timestamp\":1554363517965,\"fields\":{\"event\":\"error\",\"error.kind\":\"exception\"}}]}";

    decoder.decode(jsonParser.readTree(msg), out, "testCustomer");
    assertEquals(1, out.size());
    assertEquals("testCustomer", out.get(0).getCustomer());
    assertEquals("7b3bf470-9456-11e8-9eb6-529269fb1459", out.get(0).getTraceId());
    assertEquals("0313bafe-9457-11e8-9eb6-529269fb1459", out.get(0).getSpanId());
    assertEquals("server", out.get(0).getSpanSecondaryId());
    assertEquals(1, out.get(0).getLogs().size());
    assertEquals(1554363517965L, out.get(0).getLogs().get(0).getTimestamp().longValue());
    assertEquals(2, out.get(0).getLogs().get(0).getFields().size());
    assertEquals("error", out.get(0).getLogs().get(0).getFields().get("event"));
    assertEquals("exception", out.get(0).getLogs().get(0).getFields().get("error.kind"));
  }
}
