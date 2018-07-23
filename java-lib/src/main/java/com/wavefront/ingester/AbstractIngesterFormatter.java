package com.wavefront.ingester;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableSortedSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import org.antlr.v4.runtime.ANTLRInputStream;
import org.antlr.v4.runtime.BaseErrorListener;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.Lexer;
import org.antlr.v4.runtime.RecognitionException;
import org.antlr.v4.runtime.Recognizer;
import org.antlr.v4.runtime.Token;
import org.apache.commons.lang.time.DateUtils;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import javax.annotation.Nullable;
import javax.validation.constraints.NotNull;

import queryserver.parser.DSWrapperLexer;
import wavefront.report.Annotation;
import wavefront.report.Histogram;
import wavefront.report.HistogramType;
import wavefront.report.ReportPoint;
import wavefront.report.ReportSourceTag;
import wavefront.report.Span;

/**
 * This is the base class for formatting the content.
 *
 * @author Suranjan Pramanik (suranjan@wavefront.com)
 */
public abstract class AbstractIngesterFormatter<T> {

  protected static final FormatterElement WHITESPACE_ELEMENT = new
      ReportPointIngesterFormatter.Whitespace();
  protected static final Pattern SINGLE_QUOTE_PATTERN = Pattern.compile("\\'", Pattern.LITERAL);
  protected static final Pattern DOUBLE_QUOTE_PATTERN = Pattern.compile("\\\"", Pattern.LITERAL);
  protected static final String DOUBLE_QUOTE_REPLACEMENT = Matcher.quoteReplacement("\"");
  protected static final String SINGLE_QUOTE_REPLACEMENT = Matcher.quoteReplacement("'");

  private static final BaseErrorListener THROWING_ERROR_LISTENER = new BaseErrorListener() {
    @Override
    public void syntaxError(Recognizer<?, ?> recognizer, Object offendingSymbol, int line,
                            int charPositionInLine, String msg, RecognitionException e) {
      throw new RuntimeException("Syntax error at line " + line + ", position " + charPositionInLine + ": " + msg, e);
    }
  };

  protected final List<FormatterElement> elements;

  protected static final ThreadLocal<DSWrapperLexer> dsWrapperLexerThreadLocal =
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

  AbstractIngesterFormatter(List<FormatterElement> elements) {
    this.elements = elements;
  }

  protected Queue<Token> getQueue(String input) {
    DSWrapperLexer lexer = dsWrapperLexerThreadLocal.get();
    lexer.setInputStream(new ANTLRInputStream(input));
    CommonTokenStream commonTokenStream = new CommonTokenStream(lexer);
    commonTokenStream.fill();
    List<Token> tokens = commonTokenStream.getTokens();
    if (tokens.isEmpty()) {
      throw new RuntimeException("Could not parse: " + input);
    }
    // this is sensitive to the grammar in DSQuery.g4. We could just use the visitor but doing so
    // means we need to be creating the AST and instead we could just use the lexer. in any case,
    // we don't expect the graphite format to change anytime soon.

    // filter all EOF tokens first.
    Queue<Token> queue = tokens.stream().filter(t -> t.getType() != Lexer.EOF).collect(
        Collectors.toCollection(ArrayDeque::new));
    return queue;
  }

  /**
   * This class is a wrapper/proxy around ReportPoint and ReportSourceTag. It has a default
   * implementation of all the methods of these two classes. The default implementation is to
   * throw exception and the base classes will override them.
   */
  protected abstract static class AbstractWrapper {

    Object getValue() {
      throw new UnsupportedOperationException("Should not be invoked.");
    }

    void setValue(Histogram value) {
      throw new UnsupportedOperationException("Should not be invoked.");
    }

    void setValue(double value) {
      throw new UnsupportedOperationException("Should not be invoked.");
    }

    void setValue(String value) {
      throw new UnsupportedOperationException("Should not be invoked.");
    }

    void setValue(Long value) {
      throw new UnsupportedOperationException("Should not be invoked.");
    }

    Long getTimestamp() {
      throw new UnsupportedOperationException("Should not be invoked.");
    }

    void setTimestamp(Long value) {
      throw new UnsupportedOperationException("Should not be invoked.");
    }

    void setDuration(Long value) {
      throw new UnsupportedOperationException("Should not be invoked.");
    }

    void addAnnotation(String key, String value) {
      throw new UnsupportedOperationException("Should not be invoked.");
    }

    void addAnnotation(String value) {
      throw new UnsupportedOperationException("Should not be invoked.");
    }

    void setMetric(String value) {
      throw new UnsupportedOperationException("Should not be invoked.");
    }

    String getLiteral() {
      throw new UnsupportedOperationException("Should not be invoked.");
    }

    void setLiteral(String literal) {
      throw new UnsupportedOperationException("Should not be invoked.");
    }

    void setCustomer(String name) {
      throw new UnsupportedOperationException("Should not be invoked.");
    }

    void setName(String value) {
      throw new UnsupportedOperationException("Should not be invoked.");
    }
  }

  /**
   * This class provides a wrapper around ReportPoint.
   */
  protected static class ReportPointWrapper extends AbstractWrapper {
    ReportPoint reportPoint;

    ReportPointWrapper(ReportPoint reportPoint) {
      this.reportPoint = reportPoint;
    }

    @Override
    Object getValue() {
      return reportPoint.getValue();
    }

    @Override
    void setValue(Histogram value) {
      reportPoint.setValue(value);
    }

    @Override
    void setValue(double value) {
      reportPoint.setValue(value);
    }

    @Override
    void setValue(String value) {
      reportPoint.setValue(value);
    }

    @Override
    void setValue(Long value) {
      reportPoint.setValue(value);
    }

    @Override
    Long getTimestamp() {
      return reportPoint.getTimestamp();
    }

    @Override
    void setTimestamp(Long value) {
      reportPoint.setTimestamp(value);
    }

    @Override
    void addAnnotation(String key, String value) {
      if (reportPoint.getAnnotations() == null) {
        reportPoint.setAnnotations(new HashMap<>());
      }
      reportPoint.getAnnotations().put(key, value);
    }

    @Override
    void setMetric(String value) {
      reportPoint.setMetric(value);
    }
  }

  /**
   * This class provides a wrapper around ReportSourceTag
   */
  protected static class ReportSourceTagWrapper extends AbstractWrapper {
    final ReportSourceTag reportSourceTag;
    final Map<String, String> annotations;

    ReportSourceTagWrapper(ReportSourceTag reportSourceTag) {
      this.reportSourceTag = reportSourceTag;
      this.annotations = Maps.newHashMap();
    }

    @Override
    String getLiteral() {
      return reportSourceTag.getSourceTagLiteral();
    }

    @Override
    void setLiteral(String literal) {
      reportSourceTag.setSourceTagLiteral(literal);
    }

    @Override
    void addAnnotation(String key, String value) {
      annotations.put(key, value);
    }

    @Override
    void addAnnotation(String value) {
      if (reportSourceTag.getAnnotations() == null)
        reportSourceTag.setAnnotations(Lists.<String>newArrayList());
      reportSourceTag.getAnnotations().add(value);
    }

    @NotNull
    Map<String, String> getAnnotationMap() {
      return annotations;
    }
  }

  /**
   * This class provides a wrapper around ReportPoint.
   */
  protected static class SpanWrapper extends AbstractWrapper {
    Span span;

    SpanWrapper(Span span) {
      this.span = span;
    }

    @Override
    void setDuration(Long value) {
      span.setDuration(value);
    }

    @Override
    Long getTimestamp() {
      return span.getStartMillis();
    }

    @Override
    void setTimestamp(Long value) {
      span.setStartMillis(value);
    }

    @Override
    void setName(String name) { //
      span.setName(name);
    }

    @Override
    void addAnnotation(String key, String value) {
      if (span.getAnnotations() == null) {
        span.setAnnotations(Lists.newArrayList());
      }
      span.getAnnotations().add(new Annotation(key, value));
    }
  }

  protected interface FormatterElement {
    /**
     * Consume tokens from the queue.
     */
    void consume(Queue<Token> tokenQueue, AbstractWrapper point);
  }

  /**
   * This class can be used to create a parser for a content that the proxy receives - e.g.,
   * ReportPoint and ReportSourceTag.
   */
  public abstract static class IngesterFormatBuilder<T> {

    final List<FormatterElement> elements = Lists.newArrayList();

    public IngesterFormatBuilder<T> appendCaseSensitiveLiteral(String literal) {
      elements.add(new Literal(literal, true));
      return this;
    }

    public IngesterFormatBuilder<T> appendCaseSensitiveLiterals(String[] literals) {
      elements.add(new Literals(literals, true));
      return this;
    }

    public IngesterFormatBuilder<T> appendCaseInsensitiveLiteral(String literal) {
      elements.add(new Literal(literal, false));
      return this;
    }

    public IngesterFormatBuilder<T> appendMetricName() {
      elements.add(new Metric());
      return this;
    }

    public IngesterFormatBuilder<T> appendValue() {
      elements.add(new Value());
      return this;
    }

    public IngesterFormatBuilder<T> appendTimestamp() {
      elements.add(new AdaptiveTimestamp(false));
      return this;
    }

    public IngesterFormatBuilder<T> appendOptionalTimestamp() {
      elements.add(new AdaptiveTimestamp(true));
      return this;
    }

    public IngesterFormatBuilder<T> appendTimestamp(TimeUnit timeUnit) {
      elements.add(new Timestamp(timeUnit, false));
      return this;
    }

    public IngesterFormatBuilder<T> appendOptionalTimestamp(TimeUnit timeUnit) {
      elements.add(new Timestamp(timeUnit, true));
      return this;
    }

    public IngesterFormatBuilder<T> appendRawTimestamp() {
      elements.add(new AdaptiveTimestamp(false, false));
      return this;
    }

    public IngesterFormatBuilder<T> appendDuration() {
      elements.add(new Duration(false));
      return this;
    }

    public IngesterFormatBuilder<T> appendName() {
      elements.add(new Name());
      return this;
    }

    public IngesterFormatBuilder<T> appendBoundedAnnotationsConsumer() {
      elements.add(new GuardedLoop(new Tag(), ImmutableSortedSet.of(DSWrapperLexer.Literal, DSWrapperLexer.Letters,
          DSWrapperLexer.Quoted), false));
      return this;
    }

    public IngesterFormatBuilder<T> appendAnnotationsConsumer() {
      elements.add(new Loop(new Tag()));
      return this;
    }

    public IngesterFormatBuilder<T> whiteSpace() {
      elements.add(new Whitespace());
      return this;
    }

    public IngesterFormatBuilder<T> binType() {
      elements.add(new BinType());
      return this;
    }

    public IngesterFormatBuilder<T> centroids() {
      elements.add(new GuardedLoop(new Centroid(), Centroid.expectedToken(), false));
      return this;
    }

    public IngesterFormatBuilder<T> adjustTimestamp() {
      elements.add(new TimestampAdjuster());
      return this;
    }

    public IngesterFormatBuilder<T> appendLoopOfKeywords() {
      elements.add(new LoopOfKeywords());
      return this;
    }

    public IngesterFormatBuilder<T> appendLoopOfValues() {
      elements.add(new Loop(new AlphaNumericValue()));
      return this;
    }

    /**
     * Subclasses will provide concrete implementation for this method.
     */
    public abstract AbstractIngesterFormatter<T> build();
  }

  public static class Loop implements FormatterElement {

    private final FormatterElement element;

    public Loop(FormatterElement element) {
      this.element = element;
    }

    @Override
    public void consume(Queue<Token> tokenQueue, AbstractWrapper point) {
      while (!tokenQueue.isEmpty()) {
        WHITESPACE_ELEMENT.consume(tokenQueue, point);
        if (tokenQueue.isEmpty()) return;
        element.consume(tokenQueue, point);
      }
    }
  }

  public static class BinType implements FormatterElement {


    @Override
    public void consume(Queue<Token> tokenQueue, AbstractWrapper point) {
      {
        Token peek = tokenQueue.peek();

        if (peek == null) {
          throw new RuntimeException("Expected BinType, found EOF");
        }
        if (peek.getType() != DSWrapperLexer.BinType) {
          throw new RuntimeException("Expected BinType, found " + peek.getText());
        }
      }

      int durationMillis = 0;
      String binType = tokenQueue.poll().getText();

      switch (binType) {
        case "!M":
          durationMillis = (int) DateUtils.MILLIS_PER_MINUTE;
          break;
        case "!H":
          durationMillis = (int) DateUtils.MILLIS_PER_HOUR;
          break;
        case "!D":
          durationMillis = (int) DateUtils.MILLIS_PER_DAY;
          break;
        default:
          throw new RuntimeException("Unknown BinType " + binType);
      }

      Histogram h = computeIfNull((Histogram) point.getValue(), Histogram::new);
      h.setDuration(durationMillis);
      h.setType(HistogramType.TDIGEST);
      point.setValue(h);
    }
  }

  public static class Centroid implements FormatterElement {

    public static int expectedToken() {
      return DSWrapperLexer.Weight;
    }

    @Override
    public void consume(Queue<Token> tokenQueue, AbstractWrapper point) {
      {
        Token peek = tokenQueue.peek();

        Preconditions.checkNotNull(peek, "Expected Count, got EOF");
        Preconditions.checkArgument(peek.getType() == DSWrapperLexer.Weight, "Expected Count, got " + peek.getText());
      }

      String countStr = tokenQueue.poll().getText();
      int count;
      try {
        count = Integer.parseInt(countStr.substring(1));
      } catch (NumberFormatException e) {
        throw new RuntimeException("Could not parse count " + countStr);
      }

      WHITESPACE_ELEMENT.consume(tokenQueue, point);

      // Mean
      double mean = parseValue(tokenQueue, "centroid mean");

      Histogram h = computeIfNull((Histogram) point.getValue(), Histogram::new);
      List<Double> bins = computeIfNull(h.getBins(), ArrayList::new);
      bins.add(mean);
      h.setBins(bins);

      List<Integer> counts = computeIfNull(h.getCounts(), ArrayList::new);
      counts.add(count);
      h.setCounts(counts);
      point.setValue(h);
    }
  }

  /**
   * Similar to {@link Loop}, but expects a configurable non-whitespace {@link Token}
   */
  public static class GuardedLoop implements FormatterElement {
    private final FormatterElement element;
    private final Set<Integer> acceptedTokens;
    private final boolean optional;

    public GuardedLoop(FormatterElement element, int acceptedToken, boolean optional) {
      this.element = element;
      this.acceptedTokens = ImmutableSortedSet.of(acceptedToken);
      this.optional = optional;
    }

    public GuardedLoop(FormatterElement element, Set<Integer> acceptedTokens, boolean optional) {
      this.element = element;
      this.acceptedTokens = acceptedTokens;
      this.optional = optional;
    }


    @Override
    public void consume(Queue<Token> tokenQueue, AbstractWrapper point) {
      boolean satisfied = optional;
      while (!tokenQueue.isEmpty()) {
        WHITESPACE_ELEMENT.consume(tokenQueue, point);
        if (tokenQueue.peek() == null || !acceptedTokens.contains(tokenQueue.peek().getType())) {
          break;
        }
        satisfied = true;
        element.consume(tokenQueue, point);
      }

      if (!satisfied) {
        throw new RuntimeException("Expected at least one element, got none");
      }
    }
  }

  /**
   * Pins the point's timestamp to the beginning of the respective interval
   */
  public static class TimestampAdjuster implements FormatterElement {

    @Override
    public void consume(Queue<Token> tokenQueue, AbstractWrapper point) {
      Preconditions.checkArgument(point.getValue() != null
              && point.getTimestamp() != null
              && point.getValue() instanceof Histogram
              && ((Histogram) point.getValue()).getDuration() != null,
          "Expected a histogram point with timestamp and histogram duration");

      long duration = ((Histogram) point.getValue()).getDuration();
      point.setTimestamp((point.getTimestamp() / duration) * duration);
    }
  }


  public static class Tag implements FormatterElement {

    @Override
    public void consume(Queue<Token> queue, AbstractWrapper point) {
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
      point.addAnnotation(tagk, tagv);
    }
  }

  private static double parseValue(Queue<Token> tokenQueue, String name) {
    String value = "";
    Token current = tokenQueue.poll();
    if (current == null) throw new RuntimeException("Invalid " + name + ", found EOF");
    if (current.getType() == DSWrapperLexer.MinusSign) {
      current = tokenQueue.poll();
      value = "-";
    }
    if (current == null) throw new RuntimeException("Invalid " + name + ", found EOF");
    if (current.getType() == DSWrapperLexer.Quoted) {
      if (!value.equals("")) {
        throw new RuntimeException("Invalid " + name + ": " + value + current.getText());
      }
      value += unquote(current.getText());
    } else if (current.getType() == DSWrapperLexer.Letters ||
        current.getType() == DSWrapperLexer.Literal ||
        current.getType() == DSWrapperLexer.Number) {
      value += current.getText();
    } else {
      throw new RuntimeException("Invalid " + name + ": " + current.getText());
    }
    try {
      return Double.parseDouble(value);
    } catch (NumberFormatException nef) {
      throw new RuntimeException("Invalid " + name + ": " + value);
    }
  }

  public static class AlphaNumericValue implements FormatterElement {

    @Override
    public void consume(Queue<Token> tokenQueue, AbstractWrapper sourceTag) {
      WHITESPACE_ELEMENT.consume(tokenQueue, sourceTag);
      String value = "";
      Token current = tokenQueue.poll();
      if (current == null) throw new RuntimeException("Invalid value, found EOF");

      if (current == null) throw new RuntimeException("Invalid value, found EOF");
      if (current.getType() == DSWrapperLexer.Quoted) {
        if (!value.equals("")) {
          throw new RuntimeException("invalid metric value: " + value + current.getText());
        }
        value += ReportPointIngesterFormatter.unquote(current.getText());
      } else if (current.getType() == DSWrapperLexer.Letters ||
          current.getType() == DSWrapperLexer.Literal ||
          current.getType() == DSWrapperLexer.Number) {
        value += current.getText();
      } else {
        throw new RuntimeException("invalid value: " + current.getText());
      }
      sourceTag.addAnnotation(value);
    }
  }

  public static class Value implements FormatterElement {

    @Override
    public void consume(Queue<Token> tokenQueue, AbstractWrapper point) {
      point.setValue(parseValue(tokenQueue, "metric value"));
    }
  }

  private static Long parseTimestamp(Queue<Token> tokenQueue, boolean optional, boolean convertToMillis) {
    Token peek = tokenQueue.peek();
    if (peek == null || peek.getType() != DSWrapperLexer.Number) {
      if (optional) return null;
      else
        throw new RuntimeException("Expected timestamp, found " + (peek == null ? "EOF" : peek.getText()));
    }
    try {
      Double timestamp = Double.parseDouble(tokenQueue.poll().getText());
      Long timestampLong = timestamp.longValue();
      if (!convertToMillis) {
        // as-is
        return timestampLong;
      }
      int timestampDigits = timestampLong.toString().length();
      if (timestampDigits == 19) {
        // nanoseconds.
        return timestampLong / 1000000;
      } else if (timestampDigits == 16) {
        // microseconds
        return timestampLong / 1000;
      } else if (timestampDigits == 13) {
        // milliseconds.
        return timestampLong;
      } else {
        // treat it as seconds.
        return (long) (1000.0 * timestamp);
      }
    } catch (NumberFormatException nfe) {
      throw new RuntimeException("Invalid timestamp value: " + peek.getText());
    }
  }

  public static class Duration implements FormatterElement {

    private final boolean optional;

    public Duration(boolean optional) {
      this.optional = optional;
    }

    @Override
    public void consume(Queue<Token> tokenQueue, AbstractWrapper wrapper) {
      Long timestamp = parseTimestamp(tokenQueue, optional, false);

      Long startTs = wrapper.getTimestamp();
      if (timestamp != null && startTs != null) {
        long duration = (timestamp - startTs >= 0) ? timestamp - startTs : timestamp;
        // convert both timestamps to micros
        if (startTs > 999999999999999999L) {
          // 19 digits == nanoseconds,
          wrapper.setTimestamp(startTs / 1000);
          wrapper.setDuration(duration / 1000);
        } else if (startTs > 999999999999999L) {
          // 16 digits == microseconds
          wrapper.setDuration(duration);
        } else if (startTs > 999999999999L) {
          // 13 digits == milliseconds
          wrapper.setTimestamp(startTs * 1000);
          wrapper.setDuration(duration * 1000);
        } else {
          // seconds
          wrapper.setTimestamp(startTs * 1000000);
          wrapper.setDuration(duration * 1000000);
        }
      } else {
        throw new RuntimeException("Both timestamp and duration expected");
      }
    }
  }

  public static class Name implements FormatterElement {
    @Override
    public void consume(Queue<Token> tokenQueue, AbstractWrapper point) {
      String name = getLiteral(tokenQueue);
      if (name.length() == 0) throw new RuntimeException("Invalid name");
      point.setName(name);
    }
  }

  public static class AdaptiveTimestamp implements FormatterElement {

    private final boolean optional;
    private final boolean convertToMillis;

    public AdaptiveTimestamp(boolean optional) {
      this(optional, true);
    }

    public AdaptiveTimestamp(boolean optional, boolean convertToMillis) {
      this.optional = optional;
      this.convertToMillis = convertToMillis;
    }

    @Override
    public void consume(Queue<Token> tokenQueue, AbstractWrapper point) {
      Long timestamp = parseTimestamp(tokenQueue, optional, convertToMillis);

      // Do not override with null on satisfied
      if (timestamp != null) {
        point.setTimestamp(timestamp);
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
    public void consume(Queue<Token> tokenQueue, AbstractWrapper point) {
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
    public void consume(Queue<Token> tokenQueue, AbstractWrapper point) {
      // extract the metric name.
      String metric = getLiteral(tokenQueue);
      if (metric.length() == 0) throw new RuntimeException("Invalid metric name");
      point.setMetric(metric);
    }
  }

  public static class Whitespace implements FormatterElement {
    @Override
    public void consume(Queue<Token> tokens, AbstractWrapper point) {
      while (!tokens.isEmpty() && tokens.peek().getType() == DSWrapperLexer.WS) {
        tokens.poll();
      }
    }
  }

  /**
   * This class handles a sequence of key value pairs. Currently it works for source tag related
   * inputs only.
   */
  public static class LoopOfKeywords implements FormatterElement {

    private final FormatterElement tagElement = new Tag();

    @Override
    public void consume(Queue<Token> tokenQueue, AbstractWrapper sourceTag) {
      if (sourceTag.getLiteral() == null) {
        // throw an exception since we expected that field to be populated
        throw new RuntimeException("Expected either @SourceTag or @SourceDescription in the " +
            "message");
      } else if (sourceTag.getLiteral().equals("SourceTag")) {
        // process it as a sourceTag -- 2 tag elements; action="add" source="aSource"
        int count = 0, max = 2;
        while (count < max) {
          WHITESPACE_ELEMENT.consume(tokenQueue, sourceTag);
          tagElement.consume(tokenQueue, sourceTag);
          count++;
        }
      } else if (sourceTag.getLiteral().equals("SourceDescription")) {
        // process it as a description -- all the remaining should be tags
        while (!tokenQueue.isEmpty()) {
          WHITESPACE_ELEMENT.consume(tokenQueue, sourceTag);
          tagElement.consume(tokenQueue, sourceTag);
        }
      } else {
        // throw exception, since it should be one of the above
        throw new RuntimeException("Expected either @SourceTag or @SourceDescription in the " +
            "message");
      }
    }
  }

  public static class Literals implements FormatterElement {
    private final String[] literals;
    private final boolean caseSensitive;

    public Literals(String[] literals, boolean caseSensitive) {
      this.literals = literals;
      this.caseSensitive = caseSensitive;
    }

    @Override
    public void consume(Queue<Token> tokenQueue, AbstractWrapper point) {
      if (literals == null || literals.length != 2)
        throw new RuntimeException("Sourcetag metadata parser is not properly initialized.");

      if (tokenQueue.isEmpty()) {
        throw new RuntimeException("Expecting a literal string: " + literals[0] + " or " +
            literals[1] + " but found EOF");
      }
      String literal = getLiteral(tokenQueue);
      if (caseSensitive) {
        for (String specLiteral : literals) {
          if (literal.equals(specLiteral)) {
            point.setLiteral(literal.substring(1));
            return;
          }
        }
        throw new RuntimeException("Expecting a literal string: " + literals[0] + " or " +
            literals[1] + " but found: " + literal);
      } else {
        for (String specLiteral : literals) {
          if (literal.equalsIgnoreCase(specLiteral)) {
            point.setLiteral(literal.substring(1));
            return;
          }
        }
        throw new RuntimeException("Expecting a literal string: " + literals[0] + " or " +
            literals[1] + " but found: " + literal);
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
    public void consume(Queue<Token> tokenQueue, AbstractWrapper point) {
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


  protected static String getLiteral(Queue<Token> tokens) {
    StringBuilder toReturn = new StringBuilder();
    Token next = tokens.peek();
    if (next == null) return "";
    if (next.getType() == DSWrapperLexer.Quoted) {
      return unquote(tokens.poll().getText());
    }

    while (next != null &&
        (next.getType() == DSWrapperLexer.Letters ||
            next.getType() == DSWrapperLexer.RelaxedLiteral ||
            next.getType() == DSWrapperLexer.Number ||
            next.getType() == DSWrapperLexer.SLASH ||
            next.getType() == DSWrapperLexer.AT ||
            next.getType() == DSWrapperLexer.Literal ||
            next.getType() == DSWrapperLexer.IpV4Address ||
            next.getType() == DSWrapperLexer.MinusSign ||
            next.getType() == DSWrapperLexer.IpV6Address ||
            next.getType() == DSWrapperLexer.DELTA)) {
      toReturn.append(tokens.poll().getText());
      next = tokens.peek();
    }
    return toReturn.toString();
  }

  /**
   * @param text Text to unquote.
   * @return Extracted value from inside a quoted string.
   */
  @SuppressWarnings("WeakerAccess")  // Has users.
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

  public T drive(String input, String defaultHostName, String customerId) {
    return drive(input, defaultHostName, customerId, null);
  }

  public abstract T drive(String input, String defaultHostName, String customerId,
                          @Nullable List<String> customerSourceTags);

  static <T> T computeIfNull(@Nullable T input, Supplier<T> supplier) {
    if (input == null) return supplier.get();
    return input;
  }
}
