package com.wavefront.agent.preprocessor;

import javax.annotation.Nullable;
import java.util.Calendar;
import java.util.Map;
import java.util.TimeZone;
import java.util.function.Predicate;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.CommonTokenStream;

import com.mdimension.jchronic.Chronic;
import com.mdimension.jchronic.Options;
import com.wavefront.agent.preprocessor.predicate.ConditionLexer;
import com.wavefront.agent.preprocessor.predicate.ConditionParser;
import com.wavefront.agent.preprocessor.predicate.ConditionVisitorImpl;
import com.wavefront.agent.preprocessor.predicate.ErrorListener;
import com.wavefront.agent.preprocessor.predicate.EvalExpression;
import com.wavefront.agent.preprocessor.predicate.ExpressionPredicate;

import wavefront.report.Annotation;
import wavefront.report.ReportPoint;
import wavefront.report.Span;

import static com.wavefront.ingester.AbstractIngesterFormatter.unquote;
import static org.apache.commons.lang3.ObjectUtils.firstNonNull;

/**
 * Utility class for methods used by preprocessors.
 *
 * @author vasily@wavefront.com
 */
public abstract class PreprocessorUtil {

  private static final Pattern PLACEHOLDERS = Pattern.compile("\\{\\{(.*?)}}");

  /**
   * Substitute {{...}} placeholders with corresponding components of the point
   * {{metricName}} {{sourceName}} are replaced with the metric name and source respectively
   * {{anyTagK}} is replaced with the value of the anyTagK point tag
   *
   * @param input        input string with {{...}} placeholders
   * @param reportPoint  ReportPoint object to extract components from
   * @return string with substituted placeholders
   */
  public static String expandPlaceholders(String input, ReportPoint reportPoint) {
    if (reportPoint != null && input.contains("{{")) {
      StringBuffer result = new StringBuffer();
      Matcher placeholders = PLACEHOLDERS.matcher(input);
      while (placeholders.find()) {
        if (placeholders.group(1).isEmpty()) {
          placeholders.appendReplacement(result, placeholders.group(0));
        } else {
          String substitution;
          switch (placeholders.group(1)) {
            case "metricName":
              substitution = reportPoint.getMetric();
              break;
            case "sourceName":
              substitution = reportPoint.getHost();
              break;
            default:
              substitution = reportPoint.getAnnotations().get(placeholders.group(1));
          }
          placeholders.appendReplacement(result, firstNonNull(substitution, placeholders.group(0)));
        }
      }
      placeholders.appendTail(result);
      return result.toString();
    }
    return input;
  }

  /**
   * Substitute {{...}} placeholders with corresponding components of a Span
   * {{spanName}} {{sourceName}} are replaced with the span name and source respectively
   * {{anyKey}} is replaced with the value of an annotation with anyKey key
   *
   * @param input input string with {{...}} placeholders
   * @param span  Span object to extract components from
   * @return string with substituted placeholders
   */
  public static String expandPlaceholders(String input, Span span) {
    if (span != null && input.contains("{{")) {
      StringBuffer result = new StringBuffer();
      Matcher placeholders = PLACEHOLDERS.matcher(input);
      while (placeholders.find()) {
        if (placeholders.group(1).isEmpty()) {
          placeholders.appendReplacement(result, placeholders.group(0));
        } else {
          String substitution;
          switch (placeholders.group(1)) {
            case "spanName":
              substitution = span.getName();
              break;
            case "sourceName":
              substitution = span.getSource();
              break;
            default:
              substitution = span.getAnnotations().stream().
                  filter(a -> a.getKey().equals(placeholders.group(1))).
                  map(Annotation::getValue).findFirst().orElse(null);
          }
          placeholders.appendReplacement(result, firstNonNull(substitution, placeholders.group(0)));
        }
      }
      placeholders.appendTail(result);
      return result.toString();
    }
    return input;
  }

  /**
   * Enforce string max length limit - either truncate or truncate with "..." at the end.
   *
   * @param input         Input string to truncate.
   * @param maxLength     Truncate string at this length.
   * @param actionSubtype TRUNCATE or TRUNCATE_WITH_ELLIPSIS.
   * @return truncated string
   */
  public static String truncate(String input, int maxLength, LengthLimitActionType actionSubtype) {
    switch (actionSubtype) {
      case TRUNCATE:
        return input.substring(0, maxLength);
      case TRUNCATE_WITH_ELLIPSIS:
        return input.substring(0, maxLength - 3) + "...";
      default:
        throw new IllegalArgumentException(actionSubtype + " action is not allowed!");
    }
  }

  @Nullable
  public static String getString(Map<String, Object> ruleMap, String key) {
    Object value = ruleMap.get(key);
    if (value == null) return null;
    if (value instanceof String) return (String) value;
    if (value instanceof Number) return String.valueOf(value);
    return (String) ruleMap.get(key);
  }

  public static boolean getBoolean(Map<String, Object> ruleMap, String key, boolean defaultValue) {
    Object value = ruleMap.get(key);
    if (value == null) return defaultValue;
    if (value instanceof Boolean) return (Boolean) value;
    if (value instanceof String) return Boolean.parseBoolean((String) value);
    throw new IllegalArgumentException();
  }

  public static int getInteger(Map<String, Object> ruleMap, String key, int defaultValue) {
    Object value = ruleMap.get(key);
    if (value == null) return defaultValue;
    if (value instanceof Number) return ((Number) value).intValue();
    if (value instanceof String) return Integer.parseInt((String) value);
    throw new IllegalArgumentException();
  }

  public static long parseTextualTimeExact(String interval, long anchorTime, TimeZone timeZone) {
    Calendar instance = Calendar.getInstance();
    instance.setTimeZone(timeZone);
    instance.setTimeInMillis(anchorTime);
    com.mdimension.jchronic.utils.Span parse = Chronic.parse(unquote(interval),
        new Options(instance));
    if (parse == null) {
      throw new IllegalArgumentException("Failed to parse " + interval + " as a time-expression");
    }
    return parse.getBeginCalendar().getTimeInMillis();
  }

  public static <T> Predicate<T> parsePredicateString(String predicateString) {
    ConditionLexer lexer = new ConditionLexer(CharStreams.fromString(predicateString));
    lexer.removeErrorListeners();
    ErrorListener errorListener = new ErrorListener();
    lexer.addErrorListener(errorListener);
    CommonTokenStream tokens = new CommonTokenStream(lexer);
    ConditionParser parser = new ConditionParser(tokens);
    parser.removeErrorListeners();
    parser.addErrorListener(errorListener);
    ConditionVisitorImpl visitor = new ConditionVisitorImpl(System::currentTimeMillis);
    try {
      ConditionParser.ProgramContext context = parser.program();
      EvalExpression result = (EvalExpression) context.evalExpression().accept(visitor);
      if (errorListener.getErrors().length() == 0) {
        return new ExpressionPredicate<>(result);
      } else {
        throw new IllegalArgumentException(errorListener.getErrors().toString());
      }
    } catch (Exception e) {
      System.out.println("Exception: " + e);
      throw new RuntimeException(e);
    }
  }
}
