package com.wavefront.agent.preprocessor;

import com.google.common.base.Preconditions;
import com.google.common.base.Predicates;

import com.wavefront.agent.preprocessor.predicate.ContainsPredicate;
import com.wavefront.agent.preprocessor.predicate.EndsWithPredicate;
import com.wavefront.agent.preprocessor.predicate.EqualsPredicate;
import com.wavefront.agent.preprocessor.predicate.InPredicate;
import com.wavefront.agent.preprocessor.predicate.RegexMatchPredicate;
import com.wavefront.agent.preprocessor.predicate.StartsWithPredicate;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.function.Predicate;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import wavefront.report.Annotation;
import wavefront.report.ReportPoint;
import wavefront.report.Span;

import static org.apache.commons.lang3.ObjectUtils.firstNonNull;

/**
 * Utility class for methods used by preprocessors.
 *
 * @author vasily@wavefront.com
 */
public abstract class PreprocessorUtil {

  private static final Pattern PLACEHOLDERS = Pattern.compile("\\{\\{(.*?)}}");
  public static final String[] LOGICAL_OPS = {"all", "any", "none", "noop"};
  public static final String v2PredicateKey = "if";

  /**
   * Substitute {{...}} placeholders with corresponding components of the point
   * {{metricName}} {{sourceName}} are replaced with the metric name and source respectively
   * {{anyTagK}} is replaced with the value of the anyTagK point tag
   *
   * @param input        input string with {{...}} placeholders
   * @param reportPoint  ReportPoint object to extract components from
   * @return string with substituted placeholders
   */
  public static String expandPlaceholders(String input, @Nonnull ReportPoint reportPoint) {
    if (input.contains("{{")) {
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
  public static String expandPlaceholders(String input, @Nonnull Span span) {
    if (input.contains("{{")) {
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

  @Nullable
  public static Map<String, Object> getPredicate(Map<String, Object> ruleMap, String key) {
    Object value = ruleMap.get(key);
    if (value == null) return null;
    Map<String, Object> v2PredicateMap = null;
    if (key.equals(v2PredicateKey)) {
      v2PredicateMap = (Map<String, Object>) ruleMap.get(key);
      Preconditions.checkArgument(v2PredicateMap.size() == 1,
          "Argument ["+v2PredicateKey+"] can have only one mapping.");
    }
    return v2PredicateMap;
  }

  public static Predicate parsePredicate(Map<String, Object> v2Predicate) {
    if(v2Predicate != null && !v2Predicate.isEmpty()) {
      return processLogicalOp(v2Predicate);
    }
    return x -> true;
  }

  public static Predicate processLogicalOp(Map<String, Object> element) {
    if (element.size() != 1) {
      throw new IllegalArgumentException("Argument [if] can have only 1 top level predicate, but " +
          "found :: " + element.size() + ".");
    }
    Predicate finalPred;
    for (Map.Entry<String, Object> tlEntry : element.entrySet()) {
      switch (tlEntry.getKey()) {
        case "all":
          finalPred =  x -> true;
          for (Map<String, Object> tlValue : (List<Map<String, Object>>) tlEntry.getValue()) { //
            for (Map.Entry<String, Object> tlValueEntry : tlValue.entrySet()) {
              if (Arrays.stream(LOGICAL_OPS).parallel().anyMatch(tlValueEntry.getKey()::equals)) {
                finalPred = finalPred.and(processLogicalOp(tlValue));
              } else {
                finalPred = finalPred.and(processComparisonOp(tlValueEntry));
              }
            }
          }
          return finalPred;
        case "any":
          finalPred =  x -> false;
          for (Map<String, Object> tlValue : (List<Map<String, Object>>) tlEntry.getValue()) { //
            for (Map.Entry<String, Object> tlValueEntry : tlValue.entrySet()) {

              if (Arrays.stream(LOGICAL_OPS).parallel().anyMatch(tlValueEntry.getKey()::equals)) {
                finalPred = finalPred.or(processLogicalOp(tlValue));
              } else {
                finalPred = finalPred.or(processComparisonOp(tlValueEntry));
              }
            }
          }
          return finalPred;
        case "none":
          finalPred = x -> true;
          for (Map<String, Object> tlValue : (List<Map<String, Object>>) tlEntry.getValue()) { //
            for (Map.Entry<String, Object> tlValueEntry : tlValue.entrySet()) {
              if (Arrays.stream(LOGICAL_OPS).parallel().anyMatch(tlValueEntry.getKey()::equals)) {
                finalPred = finalPred.and(processLogicalOp(tlValue).negate());
              } else {
                finalPred = finalPred.and(processComparisonOp(tlValueEntry).negate());
              }
            }
          }
          return finalPred;
        case "noop":
          // Always return true.
          return Predicates.alwaysTrue();
        default:
          return processComparisonOp(tlEntry);
      }
    }
    return Predicates.alwaysFalse();
  }

  private static Predicate processComparisonOp(Map.Entry<String,Object> subElement) {
    Map<String, Object> svpair = (Map<String, Object>) subElement.getValue();
    if (svpair.size() != 2) {
      throw new IllegalArgumentException("Argument [ + " + subElement.getKey() + "] can have only" +
          " 2 elements, but found :: " + svpair.size() + ".");
    }
    String ruleVal = (String) svpair.get("value");
    String scope = (String) svpair.get("scope");
    if (scope == null) {
      throw new IllegalArgumentException("Argument [scope] can't be null/blank.");
    } else if (ruleVal == null) {
      throw new IllegalArgumentException("Argument [value] can't be null/blank.");
    }
    switch (subElement.getKey()) {
      case "equals":
        return new EqualsPredicate(scope, ruleVal);
      case "startsWith":
        return new StartsWithPredicate(scope, ruleVal);
      case "contains":
        return new ContainsPredicate(scope, ruleVal);
      case "endsWith":
        return new EndsWithPredicate(scope, ruleVal);
      case "regexMatch":
        return new RegexMatchPredicate(scope, ruleVal);
      case "in":
        return new InPredicate(scope, ruleVal);
      default:
        throw new IllegalArgumentException("UnSupported comparison argument [" + subElement.getKey() + "].");
    }
  }

  public static String getReportableEntityComparableValue(Object scope, ReportPoint point) {
    switch ((String) scope) {
      case "metricName": return point.getMetric();
      case "sourceName": return point.getHost();
      default: return point.getAnnotations().get(scope);
    }
  }

  public static List<String> getReportableEntityComparableValue(Object scope, Span span) {
    switch ((String) scope) {
      case "spanName": return Arrays.asList(span.getName());
      case "sourceName": return Arrays.asList(span.getSource());
      default: return span.getAnnotations().stream().
          filter(a -> a.getKey().equals(scope)).
          map(Annotation::getValue).
          collect(Collectors.toList());
    }
  }
}
