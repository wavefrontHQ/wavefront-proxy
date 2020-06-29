package com.wavefront.agent.preprocessor;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.wavefront.predicates.ExpressionPredicate;
import com.wavefront.predicates.PredicateEvalExpression;
import com.wavefront.predicates.StringComparisonExpression;
import com.wavefront.predicates.TemplateStringExpression;
import static com.wavefront.predicates.PredicateEvalExpression.asDouble;
import static com.wavefront.predicates.PredicateEvalExpression.isTrue;
import static com.wavefront.predicates.Predicates.fromPredicateEvalExpression;

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
      return fromPredicateEvalExpression((String) value);
    } else if (value instanceof Map) {
      //noinspection unchecked
      Map<String, Object> v2PredicateMap = (Map<String, Object>) value;
      Preconditions.checkArgument(v2PredicateMap.size() == 1,
          "Argument [if] can have only 1 top level predicate, but found :: " +
              v2PredicateMap.size() + ".");
      return parsePredicate(v2PredicateMap);
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
  static <T> Predicate<T> parsePredicate(Map<String, Object> v2Predicate) {
    if(v2Predicate != null && !v2Predicate.isEmpty()) {
      return new ExpressionPredicate<>(processLogicalOp(v2Predicate));
    }
    return x -> true;
  }

  private static PredicateEvalExpression processLogicalOp(Map<String, Object> element) {
    for (Map.Entry<String, Object> tlEntry : element.entrySet()) {
      switch (tlEntry.getKey()) {
        case "all":
          List<PredicateEvalExpression> allOps = processOperation(tlEntry);
          return entity -> asDouble(allOps.stream().allMatch(x -> isTrue(x.getValue(entity))));
        case "any":
          List<PredicateEvalExpression> anyOps = processOperation(tlEntry);
          return entity -> asDouble(anyOps.stream().anyMatch(x -> isTrue(x.getValue(entity))));
        case "none":
          List<PredicateEvalExpression> noneOps = processOperation(tlEntry);
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

  private static List<PredicateEvalExpression> processOperation(Map.Entry<String, Object> tlEntry) {
    List<PredicateEvalExpression> ops = new ArrayList<>();
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
  private static PredicateEvalExpression processComparisonOp(Map.Entry<String, Object> subElement) {
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
      List<PredicateEvalExpression> options = ((List<String>) ruleVal).stream().map(option ->
          StringComparisonExpression.of(new TemplateStringExpression("{{" + scope + "}}"),
              x -> option, subElement.getKey())).collect(Collectors.toList());
      return entity -> asDouble(options.stream().anyMatch(x -> isTrue(x.getValue(entity))));
    } else if (ruleVal instanceof String) {
      return StringComparisonExpression.of(new TemplateStringExpression("{{" + scope + "}}"),
          x -> (String) ruleVal, subElement.getKey());
    } else {
      throw new IllegalArgumentException("[value] can only be String or List, got " +
          ruleVal.getClass().getCanonicalName());
    }
  }
}
