package com.wavefront.ingester.graphite;

import com.google.common.base.Joiner;
import com.google.common.base.Predicate;
import com.google.common.base.Splitter;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToMessageDecoder;
import org.antlr.v4.runtime.*;
import queryserver.parser.DSWrapperLexer;
import sunnylabs.report.ReportPoint;

import java.util.ArrayDeque;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.regex.Pattern;

/**
 * Take a line describing a metric defined as:
 * <p/>
 * [metric name] [metric value] [timestamp]? [tags]
 * <p/>
 * <p/>
 * User: sam
 * Date: 7/12/13
 * Time: 8:10 PM
 */
public class GraphiteDecoder extends MessageToMessageDecoder<String> {

  private static final Pattern CUSTOMERID = Pattern.compile("[a-z]+");
  private final String hostName;

  public GraphiteDecoder() {
    this.hostName = null;
  }

  public GraphiteDecoder(String hostName) {
    this.hostName = hostName;
  }

  /**
   * Decode graphite points and dump them into an output array. The supplied customer id will be set and no customer id
   * extraction will be attempted.
   *
   * @param msg        Message to parse.
   * @param out        List to output the parsed point.
   * @param customerId The customer id to use as the table for the result ReportPoint.
   */
  public void decodeReportPoints(String msg, List<ReportPoint> out, String customerId) {
    DSWrapperLexer lexer = new DSWrapperLexer(new ANTLRInputStream(msg));
    // note that other errors are not thrown by the lexer and hence we only need to handle the syntaxError case.
    lexer.addErrorListener(new BaseErrorListener() {
      @Override
      public void syntaxError(Recognizer<?, ?> recognizer, Object offendingSymbol, int line, int charPositionInLine,
                              String msg, RecognitionException e) {
        throw new RuntimeException(msg, e);
      }
    });
    lexer.setTokenFactory(new CommonTokenFactory(true));
    CommonTokenStream commonTokenStream = new CommonTokenStream(lexer);
    commonTokenStream.getText();
    List<Token> tokens = commonTokenStream.getTokens();
    if (tokens.isEmpty()) {
      throw new RuntimeException("Could not parse: " + msg);
    }
    // this is sensitive to the grammar in DSQuery.g4. We could just use the visitor but doing so means we need
    // to be creating the AST and instead we could just use the lexer. in any case, we don't expect
    // the graphite format to change anytime soon.

    // filter all whitespace tokens first.
    Queue<Token> queue = new ArrayDeque<Token>(Lists.newArrayList(Iterables.filter(tokens, new Predicate<Token>() {
      @Override
      public boolean apply(Token input) {
        return input.getType() != Lexer.EOF;
      }
    })));
    skipWhiteSpace(queue);
    // extract the metric name.
    String metric = getLiteral(queue);
    if (metric.length() == 0) throw new RuntimeException("Invalid metric name in line: " + msg);
    skipWhiteSpace(queue);
    // extract the value.
    Token current = queue.poll();
    String value = "";
    if (current.getType() == DSWrapperLexer.MinusSign) {
      current = queue.poll();
      value = "-";
    }
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
    skipWhiteSpace(queue);
    // extract the timestamp.
    current = queue.peek();
    Map<String, String> annotations = Maps.newHashMap();
    String timestamp = null;
    if (current != null) {
      if (current.getType() == DSWrapperLexer.Number) {
        timestamp = queue.poll().getText();
        skipWhiteSpace(queue);
      }
      // extract tags.
      String tagk = null;
      while (!queue.isEmpty()) {
        if (tagk == null) {
          tagk = getLiteral(queue);
          if (tagk.length() == 0) {
            throw new RuntimeException("Invalid tag name: " + msg);
          }
          skipWhiteSpace(queue);
          current = queue.poll();
          if (current == null || current.getType() != DSWrapperLexer.EQ) {
            throw new RuntimeException("Tag keys and values must be separated by '='" + (current != null ? ", " +
                "found: " + current.getText() : ", found EOF"));
          }
        } else {
          String tagv = getLiteral(queue);
          if (tagv.length() == 0) throw new RuntimeException("Invalid tag value: " + msg);
          annotations.put(tagk, tagv);
          tagk = null;
        }
        skipWhiteSpace(queue);
      }
    }

    ReportPoint.Builder builder = ReportPoint.newBuilder();
    try {
      // It is reported as either a double or a string depending on if it can be parsed
      builder.setValue(Double.parseDouble(value));
    } catch (NumberFormatException e) {
      builder.setValue(value);
    }
    String host = annotations.remove("source");
    if (host == null) {
      host = annotations.remove("host");
    } else if (annotations.containsKey("host")) {
      // we have to move this elsewhere since during querying, host= would be interpreted as host and not a point tag
      annotations.put("_host", annotations.remove("host"));
    }
    if (host == null) {
      host = hostName;
    }
    if (annotations.containsKey("tag")) {
      String tagV = annotations.remove("tag");
      annotations.put("_tag", tagV);
    }
    ReportPoint point = builder
        .setMetric(metric)
        .setTable(customerId)
        .setAnnotations(annotations)
        .setHost(host)
        .setTimestamp(timestamp == null ? System.currentTimeMillis() : (long) (1000.0 * Double.parseDouble(timestamp)
        )).build();

    if (out != null) {
      // Can allow null outs if you just want to test decoding but don't care about the ReportPoint
      out.add(point);
    }
  }

  private String getLiteral(Queue<Token> tokens) {
    String toReturn = "";
    Token next = tokens.peek();
    if (next == null) return "";
    if (next.getType() == DSWrapperLexer.Quoted) {
      return unquote(tokens.poll().getText());
    }
    while (next != null && (next.getType() == DSWrapperLexer.Letters ||
        next.getType() == DSWrapperLexer.Number ||
        next.getType() == DSWrapperLexer.Literal ||
        next.getType() == DSWrapperLexer.IpV4Address ||
        next.getType() == DSWrapperLexer.MinusSign ||
        next.getType() == DSWrapperLexer.IpV6Address)) {
      toReturn += tokens.poll().getText();
      next = tokens.peek();
    }
    return toReturn;
  }

  private void skipWhiteSpace(Queue<Token> tokens) {
    while (!tokens.isEmpty() && tokens.peek().getType() == DSWrapperLexer.WS) {
      tokens.poll();
    }
  }

  /**
   * Decode graphite points and dump them into an output array. The customer id would be parsed out of the metric name.
   *
   * @param msg Message to parse.
   * @param out List to output the parsed point.
   */
  public void decodeReportPoints(String msg, List<ReportPoint> out) {
    List<ReportPoint> output = Lists.newArrayList();
    decodeReportPoints(msg, output, "dummy");
    if (!output.isEmpty()) {
      for (ReportPoint rp : output) {
        String metricName = rp.getMetric();
        List<String> metricParts = Lists.newArrayList(Splitter.on(".").split(metricName));
        if (metricParts.size() <= 1) {
          throw new RuntimeException("Metric name does not contain a customer id: " + metricName);
        }
        String customerId = metricParts.get(0);
        if (CUSTOMERID.matcher(customerId).matches()) {
          metricName = Joiner.on(".").join(metricParts.subList(1, metricParts.size()));
        }
        out.add(ReportPoint.newBuilder(rp).setMetric(metricName).setTable(customerId).build());
      }
    }
  }

  @Override
  public void decode(ChannelHandlerContext ctx, String msg, List<Object> out) throws Exception {
    List<ReportPoint> points = Lists.newArrayList();
    decodeReportPoints(msg, points);
    out.addAll(points);
  }

  public static String unquote(String text) {
    if (text.startsWith("\"")) {
      text = text.substring(1, text.length() - 1).replace("\\\"", "\"");
    } else if (text.startsWith("'")) {
      text = text.substring(1, text.length() - 1).replace("\\'", "'");
    }
    return text;
  }
}
