package com.wavefront.agent.preprocessor.predicate;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.CommonTokenStream;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.wavefront.common.TimeProvider;
import parser.predicate.ConditionLexer;
import parser.predicate.ConditionParser;

import static com.wavefront.agent.preprocessor.predicate.EvalExpression.asDouble;
import static com.wavefront.agent.preprocessor.predicate.EvalExpression.isTrue;

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
      return new ExpressionPredicate<>(parseEvalExpression((String) value));
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
   * Parses an expression string into an {@link EvalExpression}.
   *
   * @param predicateString string to parse.
   * @return parsed expression
   */
  public static EvalExpression parseEvalExpression(String predicateString) {
    return parseEvalExpression(predicateString, System::currentTimeMillis);
  }

  static EvalExpression parseEvalExpression(String predicateString, TimeProvider timeProvider) {
    ConditionLexer lexer = new ConditionLexer(CharStreams.fromString(predicateString));
    lexer.removeErrorListeners();
    ErrorListener errorListener = new ErrorListener();
    lexer.addErrorListener(errorListener);
    CommonTokenStream tokens = new CommonTokenStream(lexer);
    ConditionParser parser = new ConditionParser(tokens);
    parser.removeErrorListeners();
    parser.addErrorListener(errorListener);
    ConditionVisitorImpl visitor = new ConditionVisitorImpl(timeProvider);
    try {
      ConditionParser.ProgramContext context = parser.program();
      EvalExpression result = (EvalExpression) context.evalExpression().accept(visitor);
      if (errorListener.getErrors().length() == 0) {
        return result;
      } else {
        throw new IllegalArgumentException(errorListener.getErrors().toString());
      }
    } catch (Exception e) {
      System.out.println("Exception: " + e);
      throw new RuntimeException(e);
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
          StringComparisonExpression.of(new TemplateExpression("{{" + scope + "}}"),
              x -> option, subElement.getKey())).collect(Collectors.toList());
      return entity -> asDouble(options.stream().anyMatch(x -> isTrue(x.getValue(entity))));
    } else if (ruleVal instanceof String) {
      return StringComparisonExpression.of(new TemplateExpression("{{" + scope + "}}"),
          x -> (String) ruleVal, subElement.getKey());
    } else {
      throw new IllegalArgumentException("[value] can only be String or List, got " +
          ruleVal.getClass().getCanonicalName());
    }
  }
}
