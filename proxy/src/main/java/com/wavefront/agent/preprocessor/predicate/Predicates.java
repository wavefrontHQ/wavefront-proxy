package com.wavefront.agent.preprocessor.predicate;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.function.Predicate;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import org.apache.commons.lang.StringUtils;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.wavefront.agent.preprocessor.PreprocessorUtil;

import static com.wavefront.agent.preprocessor.predicate.EvalExpression.*;

/**
 * Collection of helper methods Base factory class for predicates; supports both text parsing as
 * well as YAML logic.
 *
 * @author vasily@wavefront.com.
 */
public abstract class Predicates {
  @VisibleForTesting
  static final String[] LOGICAL_OPS = {"all", "any", "none", "ignore"};

  private Predicates() {
  }

  @Nullable
  public static <T> Predicate<T> getPredicate(Map<String, Object> ruleMap) {
    Object value = ruleMap.get("if");
    if (value == null) return null;
    if (value instanceof String) {
      return PreprocessorUtil.parsePredicateString((String) value);
    } else if (value instanceof Map) {
      //noinspection unchecked
      Map<String, Object> v2PredicateMap = (Map<String, Object>) value;
      Preconditions.checkArgument(v2PredicateMap.size() == 1,
          "Argument [if] can have only 1 top level predicate, but found :: " +
              v2PredicateMap.size() + ".");
      return new ExpressionPredicate<>(parsePredicate(v2PredicateMap));
    } else {
      throw new IllegalArgumentException("Argument [if] value can only be String or Map, got " +
          value.getClass().getCanonicalName());
    }
  }

  /**
   * Parses the entire v2 Predicate tree into a Predicate.
   *
   * @param v2Predicate      the predicate tree.
   * @return parsed predicate
   */
  @VisibleForTesting
  static EvalExpression parsePredicate(Map<String, Object> v2Predicate) {
    if(v2Predicate != null && !v2Predicate.isEmpty()) {
      return processLogicalOp(v2Predicate);
    }
    return x -> 1;
  }

  private static EvalExpression processLogicalOp(Map<String, Object> element) {
    for (Map.Entry<String, Object> tlEntry : element.entrySet()) {
      switch (tlEntry.getKey()) {
        case "all":
          List<EvalExpression> allOps = processOperation(tlEntry);
          return entity -> asDouble(allOps.stream().allMatch(x -> isTrue(x.getValue(entity))));
        case "any":
          List<EvalExpression> anyOps = processOperation(tlEntry);
          return entity -> asDouble(anyOps.stream().anyMatch(x -> isTrue(x.getValue(entity))));
        case "none":
          List<EvalExpression> noneOps = processOperation(tlEntry);
          return entity -> asDouble(noneOps.stream().noneMatch(x -> isTrue(x.getValue(entity))));
        case "ignore":
          // Always return true.
          return x -> 1;
        default:
          return processComparisonOp(tlEntry);
      }
    }
    return x -> 0;
  }

  private static List<EvalExpression> processOperation(Map.Entry<String, Object> tlEntry) {
    List<EvalExpression> ops = new ArrayList<>();
    //noinspection unchecked
    for (Map<String, Object> tlValue : (List<Map<String, Object>>) tlEntry.getValue()) { //
      for (Map.Entry<String, Object> tlValueEntry : tlValue.entrySet()) {
        if (Arrays.stream(LOGICAL_OPS).parallel().anyMatch(tlValueEntry.getKey()::equals)) {
          ops.add(processLogicalOp(tlValue));
        } else {
          ops.add(processComparisonOp(tlValueEntry));
        }
      }
    }
    return ops;
  }

  @SuppressWarnings("unchecked")
  private static EvalExpression processComparisonOp(Map.Entry<String, Object> subElement) {
    Map<String, Object> svpair = (Map<String, Object>) subElement.getValue();
    if (svpair.size() != 2) {
      throw new IllegalArgumentException("Argument [ + " + subElement.getKey() + "] can have only" +
          " 2 elements, but found :: " + svpair.size() + ".");
    }
    Object ruleVal = svpair.get("value");
    String scope = (String) svpair.get("scope");
    if (scope == null) {
      throw new IllegalArgumentException("Argument [scope] can't be null/blank.");
    } else if (ruleVal == null) {
      throw new IllegalArgumentException("Argument [value] can't be null/blank.");
    }
    if (ruleVal instanceof List) {
      List<EvalExpression> options = ((List<String>) ruleVal).stream().map(option ->
          getStringComparisonExpression(new TemplateExpression("{{" + scope + "}}"),
              x -> option, subElement.getKey())).collect(Collectors.toList());
      return entity -> asDouble(options.stream().anyMatch(x -> isTrue(x.getValue(entity))));
    } else if (ruleVal instanceof String) {
      return getStringComparisonExpression(new TemplateExpression("{{" + scope + "}}"),
          x -> (String) ruleVal, subElement.getKey());
    } else {
      throw new IllegalArgumentException("[value] can only be String or List, got " +
          ruleVal.getClass().getCanonicalName());
    }
  }

  public static EvalExpression getStringComparisonExpression(StringExpression left,
                                                             StringExpression right,
                                                             String op) {
    switch (op) {
      case "=":
      case "equals":
        return new StringComparisonExpression(left, right, String::equals);
      case "startsWith":
        return new StringComparisonExpression(left, right, String::startsWith);
      case "endsWith":
        return new StringComparisonExpression(left, right, String::endsWith);
      case "contains":
        return new StringComparisonExpression(left, right, String::contains);
      case "regexMatch":
        return new StringComparisonExpression(left, right, new CachingPatternMatcher());
      case "equalsIgnoreCase":
        return new StringComparisonExpression(left, right, String::equalsIgnoreCase);
      case "startsWithIgnoreCase":
        return new StringComparisonExpression(left, right, StringUtils::startsWithIgnoreCase);
      case "endsWithIgnoreCase":
        return new StringComparisonExpression(left, right, StringUtils::endsWithIgnoreCase);
      case "containsIgnoreCase":
        return new StringComparisonExpression(left, right, StringUtils::containsIgnoreCase);
      case "regexMatchIgnoreCase":
        return new StringComparisonExpression(left, right,
            new CachingPatternMatcher(Pattern.CASE_INSENSITIVE));
      default:
        throw new IllegalArgumentException(op + " is not handled");
    }
  }

  public static EvalExpression getMultiStringComparisonExpression(String scope,
                                                                  StringExpression argument,
                                                                  boolean all,
                                                                  String op) {
    switch (op) {
      case "=":
      case "equals":
        return new MultiStringComparisonExpression(scope, argument, all, String::equals);
      case "startsWith":
        return new MultiStringComparisonExpression(scope, argument, all, String::startsWith);
      case "endsWith":
        return new MultiStringComparisonExpression(scope, argument, all, String::endsWith);
      case "contains":
        return new MultiStringComparisonExpression(scope, argument, all, String::contains);
      case "regexMatch":
        return new MultiStringComparisonExpression(scope, argument, all,
            new CachingPatternMatcher());
      case "equalsIgnoreCase":
        return new MultiStringComparisonExpression(scope, argument, all,
            String::equalsIgnoreCase);
      case "startsWithIgnoreCase":
        return new MultiStringComparisonExpression(scope, argument, all,
            StringUtils::startsWithIgnoreCase);
      case "endsWithIgnoreCase":
        return new MultiStringComparisonExpression(scope, argument, all,
            StringUtils::endsWithIgnoreCase);
      case "containsIgnoreCase":
        return new MultiStringComparisonExpression(scope, argument, all,
            StringUtils::containsIgnoreCase);
      case "regexMatchIgnoreCase":
        return new MultiStringComparisonExpression(scope, argument, all,
            new CachingPatternMatcher(Pattern.CASE_INSENSITIVE));
      default:
        throw new IllegalArgumentException(op + " is not handled");
    }
  }
}
