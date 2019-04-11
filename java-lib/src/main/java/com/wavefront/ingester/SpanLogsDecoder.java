package com.wavefront.ingester;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import wavefront.report.SpanLog;
import wavefront.report.SpanLogs;

/**
 * Span logs decoder that converts a JSON object in the following format:
 *
 * {
 *   "traceId": "...",
 *   "spanId": "...",
 *   "logs": [
 *     {
 *       "timestamp": 1234567890000000,
 *       "fields": {
 *         "key": "value",
 *         "key2": "value2"
 *       }
 *     }
 *   ]
 * }
 *
 * @author vasily@wavefront.com
 */
public class SpanLogsDecoder implements ReportableEntityDecoder<JsonNode, SpanLogs> {

  private static final ObjectMapper JSON_PARSER = new ObjectMapper();

  public SpanLogsDecoder() {
  }

  @Override
  public void decode(JsonNode msg, List<SpanLogs> out, String customerId) {
    Iterable<JsonNode> iterable = () -> msg.get("logs").elements();
    //noinspection unchecked
    SpanLogs spanLogs = SpanLogs.newBuilder().
        setCustomer("default").
        setTraceId(msg.get("traceId") == null ? null : msg.get("traceId").textValue()).
        setSpanId(msg.get("spanId") == null ? null : msg.get("spanId").textValue()).
        setLogs(StreamSupport.stream(iterable.spliterator(), false).
            map(x -> SpanLog.newBuilder().
                setTimestamp(x.get("timestamp").asLong()).
                setFields(JSON_PARSER.convertValue(x.get("fields"), Map.class)).
                build()
            ).collect(Collectors.toList())).
        build();
    if (out != null) {
      out.add(spanLogs);
    }
  }
}
