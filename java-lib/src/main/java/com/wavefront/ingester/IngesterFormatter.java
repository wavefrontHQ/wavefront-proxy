package com.wavefront.ingester;

import com.google.common.base.Predicate;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import com.wavefront.common.Clock;

import org.antlr.v4.runtime.ANTLRInputStream;
import org.antlr.v4.runtime.BaseErrorListener;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.Lexer;
import org.antlr.v4.runtime.RecognitionException;
import org.antlr.v4.runtime.Recognizer;
import org.antlr.v4.runtime.Token;

import java.util.ArrayDeque;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import queryserver.parser.DSWrapperLexer;
import sunnylabs.report.ReportPoint;

/**
 * Builder pattern for creating new ingestion formats. Inspired by the date time formatters in
 * Joda.
 *
 * @author Clement Pang (clement@wavefront.com).
 */
public class IngesterFormatter {

  private static final FormatterElement WHITESPACE_ELEMENT = new Whitespace();
  private static final Pattern SINGLE_QUOTE_PATTERN = Pattern.compile("\\'", Pattern.LITERAL);
  private static final Pattern DOUBLE_QUOTE_PATTERN = Pattern.compile("\\\"", Pattern.LITERAL);
  private static final String DOUBLE_QUOTE_REPLACEMENT = Matcher.quoteReplacement("\"");
  private static final String SINGLE_QUOTE_REPLACEMENT = Matcher.quoteReplacement("'");

  private static final BaseErrorListener THROWING_ERROR_LISTENER = new BaseErrorListener() {
    @Override
    public void syntaxError(Recognizer<?, ?> recognizer, Object offendingSymbol, int line,
                            int charPositionInLine, String msg, RecognitionException e) {
      throw new RuntimeException(msg, e);
    }
  };

  private final List<FormatterElement> elements;
  private final ThreadLocal<DSWrapperLexer> dsWrapperLexerThreadLocal =
      new ThreadLocal<DSWrapperLexer>() {
        @Override
        protected DSWrapperLexer initialValue() {
          final DSWrapperLexer lexer = new DSWrapperLexer(new ANTLRInputStream(""));
          // note that other errors are not thrown by the lexer and hence we only need to handle the
          // syntaxError case.
          lexer.removeErrorListeners();
          lexer.addErrorListener(THROWING_ERROR_LISTENER);
          return lexer;
        }
      };

  private IngesterFormatter(List<FormatterElement> elements) {
    this.elements = elements;
  }

  public ReportPoint drive(String input, String defaultHostName, String customerId,
                           List<String> customSourceTags) {
    DSWrapperLexer lexer = dsWrapperLexerThreadLocal.get();
    lexer.setInputStream(new ANTLRInputStream(input));
    CommonTokenStream commonTokenStream = new CommonTokenStream(lexer);
    commonTokenStream.getText();
    List<Token> tokens = commonTokenStream.getTokens();
    if (tokens.isEmpty()) {
      throw new RuntimeException("Could not parse: " + input);
    }
    // this is sensitive to the grammar in DSQuery.g4. We could just use the visitor but doing so
    // means we need to be creating the AST and instead we could just use the lexer. in any case,
    // we don't expect the graphite format to change anytime soon.

    // filter all EOF tokens first.
    Queue<Token> queue = new ArrayDeque<>(Lists.newArrayList(Iterables.filter(tokens,
        new Predicate<Token>() {
          @Override
          public boolean apply(Token input) {
            return input.getType() != Lexer.EOF;
          }
        })));
    ReportPoint point = new ReportPoint();
    point.setTable(customerId);
    // if the point has a timestamp, this would be overriden
    point.setTimestamp(Clock.now());
    try {
      for (FormatterElement element : elements) {
        element.consume(queue, point);
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
      if (host == null) {
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
    return point;
  }

  public static IngesterFormatBuilder newBuilder() {
    return new IngesterFormatBuilder();
  }

  public static class IngesterFormatBuilder {
    private final List<FormatterElement> elements = Lists.newArrayList();

    public IngesterFormatBuilder appendCaseSensitiveLiteral(String literal) {
      elements.add(new Literal(literal, true));
      return this;
    }

    public IngesterFormatBuilder appendCaseInsensitiveLiteral(String literal) {
      elements.add(new Literal(literal, false));
      return this;
    }

    public IngesterFormatBuilder appendMetricName() {
      elements.add(new Metric());
      return this;
    }

    public IngesterFormatBuilder appendValue() {
      elements.add(new Value());
      return this;
    }

    public IngesterFormatBuilder appendTimestamp() {
      elements.add(new AdaptiveTimestamp(false));
      return this;
    }

    public IngesterFormatBuilder appendOptionalTimestamp() {
      elements.add(new AdaptiveTimestamp(true));
      return this;
    }

    public IngesterFormatBuilder appendTimestamp(TimeUnit timeUnit) {
      elements.add(new Timestamp(timeUnit, false));
      return this;
    }

    public IngesterFormatBuilder appendOptionalTimestamp(TimeUnit timeUnit) {
      elements.add(new Timestamp(timeUnit, true));
      return this;
    }

    public IngesterFormatBuilder appendAnnotationsConsumer() {
      elements.add(new Loop(new Tag()));
      return this;
    }

    public IngesterFormatBuilder whiteSpace() {
      elements.add(new Whitespace());
      return this;
    }

    public IngesterFormatter build() {
      return new IngesterFormatter(elements);
    }
  }

  private interface FormatterElement {
    /**
     * Consume tokens from the queue.
     */
    void consume(Queue<Token> tokenQueue, ReportPoint point);
  }

  public static class Loop implements FormatterElement {

    private final FormatterElement element;

    public Loop(FormatterElement element) {
      this.element = element;
    }

    @Override
    public void consume(Queue<Token> tokenQueue, ReportPoint point) {
      while (!tokenQueue.isEmpty()) {
        WHITESPACE_ELEMENT.consume(tokenQueue, point);
        if (tokenQueue.isEmpty()) return;
        element.consume(tokenQueue, point);
      }
    }
  }

  public static class Tag implements FormatterElement {

    @Override
    public void consume(Queue<Token> queue, ReportPoint point) {
      // extract tags.
      String tagk;
      tagk = getLiteral(queue);
      if (tagk.length() == 0) {
        throw new RuntimeException("Invalid tag name");
      }
      WHITESPACE_ELEMENT.consume(queue, point);
      Token current = queue.poll();
      if (current == null || current.getType() != DSWrapperLexer.EQ) {
        throw new RuntimeException("Tag keys and values must be separated by '='" +
            (current != null ? ", " + "found: " + current.getText() : ", found EOF"));
      }
      WHITESPACE_ELEMENT.consume(queue, point);
      String tagv = getLiteral(queue);
      if (tagv.length() == 0) throw new RuntimeException("Invalid tag value for: " + tagk);
      if (point.getAnnotations() == null) {
        point.setAnnotations(Maps.<String, String>newHashMap());
      }
      point.getAnnotations().put(tagk, tagv);
    }
  }

  public static class Value implements FormatterElement {

    @Override
    public void consume(Queue<Token> tokenQueue, ReportPoint point) {
      String value = "";
      Token current = tokenQueue.poll();
      if (current == null) throw new RuntimeException("Invalid metric value, found EOF");
      if (current.getType() == DSWrapperLexer.MinusSign) {
        current = tokenQueue.poll();
        value = "-";
      }
      if (current == null) throw new RuntimeException("Invalid metric value, found EOF");
      if (current.getType() == DSWrapperLexer.Quoted) {
        if (!value.equals("")) {
          throw new RuntimeException("invalid metric value: " + value + current.getText());
        }
        value += unquote(current.getText());
      } else if (current.getType() == DSWrapperLexer.Letters ||
          current.getType() == DSWrapperLexer.Literal ||
          current.getType() == DSWrapperLexer.Number) {
        value += current.getText();
      } else {
        throw new RuntimeException("invalid metric value: " + current.getText());
      }
      try {
        point.setValue(Double.parseDouble(value));
      } catch (NumberFormatException nef) {
        throw new RuntimeException("invalid metric value: " + value);
      }
    }
  }

  public static class AdaptiveTimestamp implements FormatterElement {

    private final boolean optional;

    public AdaptiveTimestamp(boolean optional) {
      this.optional = optional;
    }

    @Override
    public void consume(Queue<Token> tokenQueue, ReportPoint point) {
      Token peek = tokenQueue.peek();
      if (peek == null) {
        if (optional) return;
        else throw new RuntimeException("Expecting timestamp, found EOF");
      }
      if (peek.getType() == DSWrapperLexer.Number) {
        try {
          Double timestamp = Double.parseDouble(tokenQueue.poll().getText());
          Long timestampLong = timestamp.longValue();
          // see if it has 13 digits.
          if (timestampLong.toString().length() == 13) {
            // milliseconds.
            point.setTimestamp(timestamp.longValue());
          } else {
            // treat it as seconds.
            point.setTimestamp((long) (1000.0 * timestamp));
          }
        } catch (NumberFormatException nfe) {
          throw new RuntimeException("Invalid timestamp value: " + peek.getText());
        }
      } else if (!optional) {
        throw new RuntimeException("Expecting timestamp, found: " + peek.getText());
      }
    }
  }

  public static class Timestamp implements FormatterElement {

    private final TimeUnit timeUnit;
    private final boolean optional;

    public Timestamp(TimeUnit timeUnit, boolean optional) {
      this.timeUnit = timeUnit;
      this.optional = optional;
    }

    @Override
    public void consume(Queue<Token> tokenQueue, ReportPoint point) {
      Token peek = tokenQueue.peek();
      if (peek == null) {
        if (optional) return;
        else throw new RuntimeException("Expecting timestamp, found EOF");
      }
      if (peek.getType() == DSWrapperLexer.Number) {
        try {
          // we need to handle the conversion outselves.
          long multiplier = timeUnit.toMillis(1);
          if (multiplier < 1) {
            point.setTimestamp(timeUnit.toMillis(
                (long) Double.parseDouble(tokenQueue.poll().getText())));
          } else {
            point.setTimestamp((long)
                (multiplier * Double.parseDouble(tokenQueue.poll().getText())));
          }
        } catch (NumberFormatException nfe) {
          throw new RuntimeException("Invalid timestamp value: " + peek.getText());
        }
      } else if (!optional) {
        throw new RuntimeException("Expecting timestamp, found: " + peek.getText());
      }
    }
  }

  public static class Metric implements FormatterElement {

    @Override
    public void consume(Queue<Token> tokenQueue, ReportPoint point) {
      // extract the metric name.
      String metric = getLiteral(tokenQueue);
      if (metric.length() == 0) throw new RuntimeException("Invalid metric name");
      point.setMetric(metric);
    }
  }

  public static class Whitespace implements FormatterElement {
    @Override
    public void consume(Queue<Token> tokens, ReportPoint point) {
      while (!tokens.isEmpty() && tokens.peek().getType() == DSWrapperLexer.WS) {
        tokens.poll();
      }
    }
  }

  public static class Literal implements FormatterElement {

    private final String literal;
    private final boolean caseSensitive;

    public Literal(String literal, boolean caseSensitive) {
      this.literal = literal;
      this.caseSensitive = caseSensitive;
    }

    @Override
    public void consume(Queue<Token> tokenQueue, ReportPoint point) {
      if (tokenQueue.isEmpty()) {
        throw new RuntimeException("Expecting a literal string: " + literal + " but found EOF");
      }
      String literal = getLiteral(tokenQueue);
      if (caseSensitive) {
        if (!literal.equals(this.literal)) {
          throw new RuntimeException("Expecting a literal string: " + this.literal + " but found:" +
              " " + literal);
        }
      } else {
        if (!literal.equalsIgnoreCase(this.literal)) {
          throw new RuntimeException("Expecting a literal string: " + this.literal + " but found:" +
              " " + literal);
        }
      }
    }
  }

  private static String getLiteral(Queue<Token> tokens) {
    String toReturn = "";
    Token next = tokens.peek();
    if (next == null) return "";
    if (next.getType() == DSWrapperLexer.Quoted) {
      return unquote(tokens.poll().getText());
    }
    while (next != null && (next.getType() == DSWrapperLexer.Letters ||
        next.getType() == DSWrapperLexer.Number ||
        next.getType() == DSWrapperLexer.SLASH ||
        next.getType() == DSWrapperLexer.AT ||
        next.getType() == DSWrapperLexer.Literal ||
        next.getType() == DSWrapperLexer.IpV4Address ||
        next.getType() == DSWrapperLexer.MinusSign ||
        next.getType() == DSWrapperLexer.IpV6Address)) {
      toReturn += tokens.poll().getText();
      next = tokens.peek();
    }
    return toReturn;
  }

  public static String unquote(String text) {
    if (text.startsWith("\"")) {
      text = DOUBLE_QUOTE_PATTERN.matcher(text.substring(1, text.length() - 1)).
          replaceAll(DOUBLE_QUOTE_REPLACEMENT);
    } else if (text.startsWith("'")) {
      text = SINGLE_QUOTE_PATTERN.matcher(text.substring(1, text.length() - 1)).
          replaceAll(SINGLE_QUOTE_REPLACEMENT);
    }
    return text;
  }
}
