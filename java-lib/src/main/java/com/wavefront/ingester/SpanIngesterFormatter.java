package com.wavefront.ingester;

import org.antlr.v4.runtime.Token;

import java.util.Iterator;
import java.util.List;
import java.util.Queue;

import javax.annotation.Nullable;

import wavefront.report.Annotation;
import wavefront.report.Span;

/**
 * Builder for Span formatter.
 *
 * @author vasily@wavefront.com
 */
public class SpanIngesterFormatter extends AbstractIngesterFormatter<Span> {

  private SpanIngesterFormatter(List<FormatterElement> elements) {
    super(elements);
  }

  /**
   * A builder pattern to create a format for span parsing.
   */
  public static class SpanIngesterFormatBuilder extends IngesterFormatBuilder<Span> {

    @Override
    public SpanIngesterFormatter build() {
      return new SpanIngesterFormatter(elements);
    }
  }

  public static IngesterFormatBuilder<Span> newBuilder() {
    return new SpanIngesterFormatBuilder();
  }

  @Override
  public Span drive(String input, String defaultHostName, String customerId,
                           @Nullable List<String> customSourceTags) {
    Queue<Token> queue = getQueue(input);

    Span span = new Span();
    span.setCustomer(customerId);
    if (defaultHostName != null) {
      span.setSource(defaultHostName);
    }
    AbstractWrapper wrapper = new SpanWrapper(span);
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

    List<Annotation> annotations = span.getAnnotations();
    if (annotations != null) {
      boolean hasTrueSource = false;
      Iterator<Annotation> iterator = annotations.iterator();
      while (iterator.hasNext()) {
        final Annotation annotation = iterator.next();
        if (customSourceTags != null && !hasTrueSource && customSourceTags.contains(annotation.getKey())) {
          span.setSource(annotation.getValue());
        }
        switch (annotation.getKey()) {
          case "source":
          case "host":
            span.setSource(annotation.getValue());
            iterator.remove();
            hasTrueSource = true;
            break;
          case "spanId":
            span.setSpanId(annotation.getValue());
            iterator.remove();
            break;
          case "traceId":
            span.setTraceId(annotation.getValue());
            iterator.remove();
            break;
          default:
            break;
        }
      }
    }

    if (span.getSource() == null) {
      throw new RuntimeException("source can't be null: " + input);
    }
    if (span.getSpanId() == null) {
      throw new RuntimeException("spanId can't be null: " + input);
    }
    if (span.getTraceId() == null) {
      throw new RuntimeException("traceId can't be null: " + input);
    }
    return Span.newBuilder(span).build();
  }
}
