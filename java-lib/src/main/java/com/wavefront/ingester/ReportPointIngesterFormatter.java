package com.wavefront.ingester;

import com.wavefront.common.Clock;

import org.antlr.v4.runtime.Token;

import java.util.List;
import java.util.Map;
import java.util.Queue;

import javax.annotation.Nullable;

import wavefront.report.ReportPoint;

/**
 * Builder pattern for creating new ingestion formats. Inspired by the date time formatters in
 * Joda.
 *
 * @author Clement Pang (clement@wavefront.com).
 */
public class ReportPointIngesterFormatter extends AbstractIngesterFormatter<ReportPoint> {

  private ReportPointIngesterFormatter(List<FormatterElement> elements) {
    super(elements);
  }

  /**
   * A builder pattern to create a format for the report point parse.
   */
  public static class ReportPointIngesterFormatBuilder extends IngesterFormatBuilder<ReportPoint> {

    @Override
    public ReportPointIngesterFormatter build() {
      return new ReportPointIngesterFormatter(elements);
    }
  }

  public static IngesterFormatBuilder<ReportPoint> newBuilder() {
    return new ReportPointIngesterFormatBuilder();
  }

  @Override
  public ReportPoint drive(String input, String defaultHostName, String customerId,
                           @Nullable List<String> customSourceTags) {
    Queue<Token> queue = getQueue(input);

    ReportPoint point = new ReportPoint();
    point.setTable(customerId);
    // if the point has a timestamp, this would be overriden
    point.setTimestamp(Clock.now());
    AbstractWrapper wrapper = new ReportPointWrapper(point);
    try {
      for (FormatterElement element : elements) {
        element.consume(queue, wrapper);
      }
    } catch (Exception ex) {
      throw new RuntimeException("Could not parse: " + input, ex);
    }
    if (!queue.isEmpty()) {
      throw new RuntimeException("Could not parse: " + input);
    }

    String host = null;
    Map<String, String> annotations = point.getAnnotations();
    if (annotations != null) {
      host = annotations.remove("source");
      if (host == null) {
        host = annotations.remove("host");
      } else if (annotations.containsKey("host")) {
        // we have to move this elsewhere since during querying,
        // host= would be interpreted as host and not a point tag
        annotations.put("_host", annotations.remove("host"));
      }
      if (annotations.containsKey("tag")) {
        annotations.put("_tag", annotations.remove("tag"));
      }
      if (host == null && customSourceTags != null) {
        // iterate over the set of custom tags, breaking when one is found
        for (String tag : customSourceTags) {
          host = annotations.get(tag);
          if (host != null) {
            break;
          }
        }
      }
    }
    if (host == null) {
      host = defaultHostName;
    }
    point.setHost(host);
    return ReportPoint.newBuilder(point).build();
  }
}
