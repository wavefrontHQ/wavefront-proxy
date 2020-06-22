package com.wavefront.agent.preprocessor.predicate;

import java.util.function.BiFunction;
import java.util.regex.Pattern;

import org.apache.commons.lang.StringUtils;

import static com.wavefront.agent.preprocessor.predicate.EvalExpression.asDouble;

/**
 * Adapter that converts two {@link StringExpression} to {@link EvalExpression}.
 *
 * @author vasily@wavefront.com.
 */
public class StringComparisonExpression implements EvalExpression {

  private final StringExpression left;
  private final StringExpression right;
  private final BiFunction<String, String, Boolean> func;

  private StringComparisonExpression(StringExpression left,
                                     StringExpression right,
                                     BiFunction<String, String, Boolean> func) {
    this.left = left;
    this.right = right;
    this.func = func;
  }

  @Override
  public double getValue(Object entity) {
    return asDouble(func.apply(left.getString(entity), right.getString(entity)));
  }

  public static EvalExpression of(StringExpression left, StringExpression right, String op) {
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
      case "matches":
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
      case "matchesIgnoreCase":
      case "regexMatchIgnoreCase":
        return new StringComparisonExpression(left, right,
            new CachingPatternMatcher(Pattern.CASE_INSENSITIVE));
      default:
        throw new IllegalArgumentException(op + " is not handled");
    }
  }
}
