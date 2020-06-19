package com.wavefront.agent.preprocessor.predicate;

import java.util.List;
import java.util.Random;
import java.util.TimeZone;
import java.util.stream.Collectors;

import org.apache.commons.lang.StringUtils;

import com.wavefront.agent.preprocessor.PreprocessorUtil;
import com.wavefront.common.TimeProvider;

import wavefront.report.ReportPoint;
import wavefront.report.Span;

import static com.wavefront.agent.preprocessor.predicate.Predicates.getMultiStringComparisonExpression;
import static com.wavefront.agent.preprocessor.predicate.Predicates.getStringComparisonExpression;
import static com.wavefront.agent.preprocessor.predicate.EvalExpression.asDouble;
import static com.wavefront.agent.preprocessor.predicate.EvalExpression.isTrue;
import static com.wavefront.ingester.AbstractIngesterFormatter.unquote;
import static org.apache.commons.lang3.ObjectUtils.firstNonNull;

@SuppressWarnings("unchecked")
public class ConditionVisitorImpl extends ConditionBaseVisitor<Expression> {
  private static final Random RANDOM = new Random();
  private final TimeProvider timeProvider;

  public ConditionVisitorImpl(TimeProvider timeProvider) {
    this.timeProvider = timeProvider;
  }

  @Override
  public Expression visitEvalExpression(ConditionParser.EvalExpressionContext ctx) {
    if (ctx == null) {
      throw new IllegalArgumentException("Syntax error");
    } else if (ctx.op != null) {
      if (ctx.op.getText().length() > 1) {
        if (ctx.op.getText().equalsIgnoreCase("and")) { // AND
          EvalExpression left = evalExpression(ctx.evalExpression(0));
          EvalExpression right = evalExpression(ctx.evalExpression(1));
          return (EvalExpression) entity -> asDouble(isTrue(left.getValue(entity)) &&
              isTrue(right.getValue(entity)));
        } else { // OR
          EvalExpression left = evalExpression(ctx.evalExpression(0));
          EvalExpression right = evalExpression(ctx.evalExpression(1));
          return (EvalExpression) entity ->
              asDouble(isTrue(left.getValue(entity)) || isTrue(right.getValue(entity)));
        }
      } else { // + - * / %
        return new MathExpression(evalExpression(ctx.evalExpression(0)),
            evalExpression(ctx.evalExpression(1)), ctx.op.getText());
      }
    } else if (ctx.comparisonOperator() != null) { // = > < <= >= !=
      return new MathExpression(evalExpression(ctx.evalExpression(0)),
          evalExpression(ctx.evalExpression(1)), ctx.comparisonOperator().getText());
    } else if (ctx.not != null) {
      EvalExpression expression = evalExpression(ctx.evalExpression(0));
      return (EvalExpression) entity -> asDouble(!isTrue(expression.getValue(entity)));
    } else if (ctx.multiModifier != null) {
      String scope = firstNonNull(ctx.placeholder().Letters(),
          ctx.placeholder().Identifier()).getText();
      StringExpression argument = stringExpression(ctx.stringExpression(0));
      boolean all = ctx.multiModifier.getText().equalsIgnoreCase("all");
      return getMultiStringComparisonExpression(scope, argument, all,
          ctx.stringComparisonOp().getText());
    } else if (ctx.stringComparisonOp() != null) {
      StringExpression left = stringExpression(ctx.stringExpression(0));
      StringExpression right = stringExpression(ctx.stringExpression(1));
      return getStringComparisonExpression(left, right, ctx.stringComparisonOp().getText());
    } else if (ctx.in != null && ctx.stringExpression().size() > 1) {
      StringExpression left = stringExpression(ctx.stringExpression(0));
      List<EvalExpression> branches = ctx.stringExpression().subList(1, ctx.stringExpression().size()).
          stream().map(exp -> new StringComparisonExpression(left,
          stringExpression(exp), String::equals)).
          collect(Collectors.toList());
      return (EvalExpression) entity ->
          asDouble(branches.stream().anyMatch(x -> isTrue(x.getValue(entity))));
    } else if (ctx.propertyAccessor() != null) {
      switch (ctx.propertyAccessor().getText()) {
        case "value":
          return (EvalExpression) entity -> ((ReportPoint) entity).getValue() instanceof Number ?
              ((Number) ((ReportPoint) entity).getValue()).doubleValue() : 0;
        case "timestamp":
          return (EvalExpression) entity -> ((ReportPoint) entity).getTimestamp();
        case "startMillis":
          return (EvalExpression) entity -> ((Span) entity).getStartMillis();
        case "duration":
          return (EvalExpression) entity -> ((Span) entity).getDuration();
        default:
          throw new IllegalArgumentException("Unknown property: " +
              ctx.propertyAccessor().getText());
      }
    } else if (ctx.number() != null) {
      return (EvalExpression) entity -> getNumber(ctx.number());
    } else {
      return visitChildren(ctx);
    }
  }

  @Override
  public Expression visitStringExpression(ConditionParser.StringExpressionContext ctx) {
    if (ctx.concat != null) {
      StringExpression left = stringExpression(ctx.stringExpression(0));
      StringExpression right = stringExpression(ctx.stringExpression(1));
      return (StringExpression) entity -> left.getString(entity) + right.getString(entity);
    } else if (ctx.stringFunc() != null) {
      StringExpression input = stringExpression(ctx.stringExpression(0));
      if (ctx.stringFunc().replace() != null) {
        StringExpression search = stringExpression(ctx.stringFunc().
            replace().stringExpression(0));
        StringExpression replacement = stringExpression(ctx.stringFunc().
            replace().stringExpression(1));
        return (StringExpression) entity -> input.getString(entity).
            replace(search.getString(entity), replacement.getString(entity));
      } else if (ctx.stringFunc().replaceAll() != null) {
        StringExpression regex = stringExpression(ctx.stringFunc().
            replaceAll().stringExpression(0));
        StringExpression replacement = stringExpression(ctx.stringFunc().
            replaceAll().stringExpression(1));
        return (StringExpression) entity -> input.getString(entity).
            replaceAll(regex.getString(entity), replacement.getString(entity));
      } else if (ctx.stringFunc().substring() != null) {
        EvalExpression fromExp = evalExpression(ctx.stringFunc().substring().evalExpression(0));
        if (ctx.stringFunc().substring().evalExpression().size() > 1) {
          EvalExpression toExp = evalExpression(ctx.stringFunc().substring().evalExpression(1));
          return (StringExpression) entity -> input.getString(entity).
              substring((int) fromExp.getValue(entity), (int) toExp.getValue(entity));
        } else {
          return (StringExpression) entity -> input.getString(entity).
              substring((int) fromExp.getValue(entity));
        }
      } else if (ctx.stringFunc().strLeft() != null) {
        EvalExpression index = evalExpression(ctx.stringFunc().substring().evalExpression(0));
        return (StringExpression) entity -> input.getString(entity).
            substring(0, (int) index.getValue(entity));
      } else if (ctx.stringFunc().strRight() != null) {
        EvalExpression index = evalExpression(ctx.stringFunc().substring().evalExpression(0));
        return (StringExpression) entity -> {
          String str = input.getString(entity);
          return str.substring(str.length(), str.length() - (int) index.getValue(entity));
        };
      } else if (ctx.stringFunc().toLowerCase() != null) {
        return (StringExpression) entity -> input.getString(entity).toLowerCase();
      } else if (ctx.stringFunc().toUpperCase() != null) {
        return (StringExpression) entity -> input.getString(entity).toUpperCase();
      } else {
        throw new IllegalArgumentException("Unknown string function");
      }
    } else if (ctx.string() != null) {
      String text = ctx.string().getText();
      return new TemplateExpression(ctx.string().Quoted() != null ? unquote(text) : text);
    } else if (ctx.stringExpression(0) != null) {
      return visitStringExpression(ctx.stringExpression(0));
    }
    return super.visitStringExpression(ctx);
  }

  @Override
  public Expression visitIff(ConditionParser.IffContext ctx) {
    if (ctx == null) {
      throw new IllegalArgumentException("Syntax error for if()");
    }
    EvalExpression condition = (EvalExpression) ctx.evalExpression(0);
    EvalExpression thenExpression = (EvalExpression) ctx.evalExpression(1);
    EvalExpression elseExpression = (EvalExpression) ctx.evalExpression(1);
    return (EvalExpression) entity ->
        isTrue(condition.getValue(entity)) ?
            thenExpression.getValue(entity) :
            elseExpression.getValue(entity);
  }

  @Override
  public Expression visitParse(ConditionParser.ParseContext ctx) {
    StringExpression strExp = stringExpression(ctx.stringExpression());
    EvalExpression defaultValue = evalExpression(ctx.evalExpression());
    return (EvalExpression) entity -> {
      try {
        return Double.parseDouble(strExp.getString(entity));
      } catch (NumberFormatException e) {
        return defaultValue.getValue(entity);
      }
    };
  }

  @Override
  public Expression visitTime(ConditionParser.TimeContext ctx) {
    if (ctx.stringExpression(0) == null) {
      throw new IllegalArgumentException("Cannot parse time argument");
    }
    StringExpression timeExp = stringExpression(ctx.stringExpression(0));
    TimeZone tz;
    if (ctx.stringExpression().size() == 1) {
      tz = TimeZone.getTimeZone("UTC");
    } else {
      StringExpression tzExp = stringExpression(ctx.stringExpression(1));
      tz = TimeZone.getTimeZone(tzExp.getString(null));
    }
    // if we can, parse current timestamp against the time argument to fail fast
    String testString = timeExp.getString(null);
    PreprocessorUtil.parseTextualTimeExact(testString, timeProvider.currentTimeMillis(), tz);
    return (EvalExpression) entity ->
        PreprocessorUtil.parseTextualTimeExact(timeExp.getString(entity),
            timeProvider.currentTimeMillis(), tz);
  }

  @Override
  public Expression visitLength(ConditionParser.LengthContext ctx) {
    StringExpression exp = stringExpression(ctx.stringExpression());
    return (EvalExpression) entity -> exp.getString(entity).length();
  }

  @Override
  public Expression visitStrIsEmpty(ConditionParser.StrIsEmptyContext ctx) {
    StringExpression exp = stringExpression(ctx.stringExpression());
    return (EvalExpression) entity -> asDouble(StringUtils.isEmpty(exp.getString(entity)));
  }

  @Override
  public Expression visitStrIsNotEmpty(ConditionParser.StrIsNotEmptyContext ctx) {
    StringExpression exp = stringExpression(ctx.stringExpression());
    return (EvalExpression) entity -> asDouble(StringUtils.isNotEmpty(exp.getString(entity)));
  }

  @Override
  public Expression visitStrIsBlank(ConditionParser.StrIsBlankContext ctx) {
    StringExpression exp = stringExpression(ctx.stringExpression());
    return (EvalExpression) entity -> asDouble(StringUtils.isBlank(exp.getString(entity)));
  }

  @Override
  public Expression visitStrIsNotBlank(ConditionParser.StrIsNotBlankContext ctx) {
    StringExpression exp = stringExpression(ctx.stringExpression());
    return (EvalExpression) entity -> asDouble(StringUtils.isNotBlank(exp.getString(entity)));
  }

  @Override
  public Expression visitRandom(ConditionParser.RandomContext ctx) {
    return (EvalExpression) entity -> RANDOM.nextDouble();
  }

  private EvalExpression evalExpression(ConditionParser.EvalExpressionContext ctx) {
    return (EvalExpression) visitEvalExpression(ctx);
  }

  private StringExpression stringExpression(ConditionParser.StringExpressionContext ctx) {
    return (StringExpression) visitStringExpression(ctx);
  }

  private static double getNumber(ConditionParser.NumberContext numberContext) {
    if (numberContext == null) {
      throw new IllegalArgumentException("Cannot parse number (null)");
    }
    if (numberContext.Number() == null) {
      throw new IllegalArgumentException("Cannot parse number from: \"" +
          numberContext.getText() + "\" " + numberContext.getText());
    }
    double toReturn = Double.parseDouble(numberContext.Number().getText());
    if (numberContext.MinusSign() != null) {
      toReturn = -toReturn;
    }
    if (numberContext.siSuffix() != null) {
      String suffix = numberContext.siSuffix().getText();
      switch (suffix) {
        case "Y":
          toReturn *= 1E24;
          break;
        case "Z":
          toReturn *= 1E21;
          break;
        case "E":
          toReturn *= 1E18;
          break;
        case "P":
          toReturn *= 1E15;
          break;
        case "T":
          toReturn *= 1E12;
          break;
        case "G":
          toReturn *= 1E9;
          break;
        case "M":
          toReturn *= 1E6;
          break;
        case "k":
          toReturn *= 1E3;
          break;
        case "h":
          toReturn *= 1E2;
          break;
        case "da":
          toReturn *= 10;
          break;
        case "d":
          toReturn *= 1E-1;
          break;
        case "c":
          toReturn *= 1E-2;
          break;
        case "m":
          toReturn *= 1E-3;
          break;
        case "µ":
          toReturn *= 1E-6;
          break;
        case "n":
          toReturn *= 1E-9;
          break;
        case "p":
          toReturn *= 1E-12;
          break;
        case "f":
          toReturn *= 1E-15;
          break;
        case "a":
          toReturn *= 1E-18;
          break;
        case "z":
          toReturn *= 1E-21;
          break;
        case "y":
          toReturn *= 1E-24;
          break;
        default:
          throw new IllegalArgumentException("Unknown SI Suffix: " + suffix);
      }
    }
    return toReturn;
  }
}
