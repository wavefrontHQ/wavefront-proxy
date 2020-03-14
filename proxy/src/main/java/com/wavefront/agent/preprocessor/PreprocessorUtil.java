package com.wavefront.agent.preprocessor;

import com.google.common.base.Preconditions;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
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
  private static final String[] logicalOps = {"all", "any", "none", "noop"};
  private static final String[] comparisonOps = {"startsWith", "regexMatch", "equals", "In"};
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

  public static boolean isRuleApplicable(Map<String, Object> v2Predicate,
                                         ReportPoint reportPoint) {
    if( v2Predicate != null && !v2Predicate.isEmpty()) {
      return processLogicalOp(v2Predicate, reportPoint);
    }
    return true;
  }

  public static boolean isRuleApplicable(Map<String, Object> v2Predicate,
                                         Span span) {
    if( v2Predicate != null && !v2Predicate.isEmpty()) {
      return processLogicalOp(v2Predicate, span);
    }
    return true;
  }


  public static boolean processLogicalOp(Map<String, Object> element, Object reportableEntity) {
    boolean acceptRule = false;
    for (Map.Entry<String, Object> tlEntry : element.entrySet()) {
      switch (tlEntry.getKey()) {
        case "all":
          for (Map<String, Object> tlValue : (List<Map<String, Object>>) tlEntry.getValue()) { //
            // any
            for (Map.Entry<String, Object> tlValueEntry : tlValue.entrySet()) {
              if (Arrays.stream(logicalOps).parallel().anyMatch(tlValueEntry.getKey()::equals)) {
                acceptRule = processLogicalOp(tlValue, reportableEntity);
              } else {
                acceptRule = processComparisonOp(tlValueEntry, reportableEntity);
              }
            }
            // If any comparison fails, we return false.
            if (!acceptRule) {
              return false;
            }
          }
          // If all comparisons pass, we return true;
          return true;
        case "any":
          for (Map<String, Object> tlValue : (List<Map<String, Object>>) tlEntry.getValue()) { //
            // any
            for (Map.Entry<String, Object> tlValueEntry : tlValue.entrySet()) {

              if (Arrays.stream(logicalOps).parallel().anyMatch(tlValueEntry.getKey()::equals)) {
                acceptRule = processLogicalOp(tlValue, reportableEntity);
              } else {
                acceptRule = processComparisonOp(tlValueEntry, reportableEntity);
              }
            }
            // If any comparison function is true, we return true.
            if (acceptRule) {
              return true;
            }
          }
          // If all comparisons fail, we return false;
          return false;
        case "none":
          for (Map<String, Object> tlValue : (List<Map<String, Object>>) tlEntry.getValue()) { //
            // any
            for (Map.Entry<String, Object> tlValueEntry : tlValue.entrySet()) {
              if (Arrays.stream(logicalOps).parallel().anyMatch(tlValueEntry.getKey()::equals)) {
                acceptRule = processLogicalOp(tlValue, reportableEntity);
              } else {
                acceptRule = processComparisonOp(tlValueEntry, reportableEntity);
              }
            }
            // If any comparison function is true, we return false.
            if (acceptRule) {
              return false;
            }
          }
          // If all comparisons fail, we return true.
          return true;
        case "noop":
          // Ignore evaluating the comparison functions.
          return true;
        default:
          return processComparisonOp(tlEntry, reportableEntity);
      }
    }
    return false;
  }

  public static boolean processComparisonOp(Map.Entry<String,Object> subElement,
                                            Object reportableEntity) {
    if (reportableEntity instanceof ReportPoint) {
      return processComparisonOp(subElement, (ReportPoint) reportableEntity);
    } else if (reportableEntity instanceof Span) {
      return processComparisonOp(subElement, (Span) reportableEntity);
    }
    throw new IllegalArgumentException("Invalid Reportable Entity");
  }

  public static boolean processComparisonOp(Map.Entry<String,Object> subElement, ReportPoint point) {
    // TODO: check if size of svpair is > 2, if so throw invalid rule exception ?
    Map<String, Object> svpair = (Map<String, Object>) subElement.getValue();
    String pointVal = getReportableEntityComparableValue(svpair.get("scope"), point);
    String ruleVal = (String) svpair.get("value");
    if (pointVal != null && ruleVal != null) {
      switch (subElement.getKey()) {
        case "equals":
          return pointVal.equals(ruleVal);
        case "startsWith":
          return pointVal.startsWith(ruleVal);
        case "regexMatch":
          return Pattern.compile(ruleVal).matcher(pointVal).matches();
        case "In":
          String[] ruleValArray = ruleVal.trim().split("\\s*,\\s*");
          return Arrays.asList(ruleValArray).contains(pointVal);
        default:
          throw new IllegalArgumentException("Unsupported comparison value :: " +
              subElement.getKey() + ", valid values :: " + comparisonOps.toString());
      }
    }

    return false;
  }

  public static String getReportableEntityComparableValue(Object scope, ReportPoint point) {
    switch ((String) scope) {
      case "metricName": return point.getMetric();
      case "sourceName": return point.getHost();
      default: return point.getAnnotations().get(scope);
    }
  }

  public static boolean processComparisonOp(Map.Entry<String,Object> subElement, Span span) {
    // TODO: check if size of svpair is > 2, if so throw invalid rule exception ?
    Map<String, Object> svpair = (Map<String, Object>) subElement.getValue();
    // if scope is spanAnnotationKey, then we can have multiple span annotation values.
    List<String> spanVal = getReportableEntityComparableValue(svpair.get("scope"), span);
    String ruleVal = (String) svpair.get("value");
    if (spanVal != null && ruleVal != null) {
      switch (subElement.getKey()) {
        case "equals":
          return spanVal.contains(ruleVal);
        case "startsWith":
          return spanVal.stream().anyMatch(v -> v.startsWith(ruleVal));
        case "regexMatch":
          return spanVal.stream().anyMatch(v -> Pattern.compile(ruleVal).matcher(v).matches());
        case "In":
          String[] ruleValArray = ruleVal.trim().split("\\s*,\\s*");
          return !Collections.disjoint(spanVal, Arrays.asList(ruleValArray));
      }
    }

    return false;
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
