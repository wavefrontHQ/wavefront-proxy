package com.wavefront.agent.preprocessor.predicate;

import java.util.List;
import java.util.Random;
import java.util.TimeZone;
import java.util.stream.Collectors;

import org.apache.commons.lang.StringUtils;

import com.wavefront.agent.preprocessor.PreprocessorUtil;
import com.wavefront.common.TimeProvider;

import parser.predicate.ConditionBaseVisitor;
import parser.predicate.ConditionParser;
import wavefront.report.ReportPoint;
import wavefront.report.Span;

import static com.wavefront.agent.preprocessor.predicate.EvalExpression.asDouble;
import static com.wavefront.agent.preprocessor.predicate.EvalExpression.isTrue;
import static com.wavefront.ingester.AbstractIngesterFormatter.unquote;
import static org.apache.commons.lang3.ObjectUtils.firstNonNull;

/**
 * Expression parser.
 *
 * @author vasily@wavefront.com.
 */
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
      return new MathExpression(eval(ctx.evalExpression(0)), eval(ctx.evalExpression(1)),
          ctx.op.getText().toLowerCase().replace(" ", ""));
    } else if (ctx.comparisonOperator() != null) { // = > < <= >= !=
      return new MathExpression(eval(ctx.evalExpression(0)), eval(ctx.evalExpression(1)),
          ctx.comparisonOperator().getText().replace(" ", ""));
    } else if (ctx.not != null) {
      EvalExpression expression = eval(ctx.evalExpression(0));
      return (EvalExpression) entity -> asDouble(!isTrue(expression.getValue(entity)));
    } else if (ctx.complement != null) {
      EvalExpression expression = eval(ctx.evalExpression(0));
      return (EvalExpression) entity -> ~ (long) expression.getValue(entity);
    } else if (ctx.multiModifier != null) {
      String scope = firstNonNull(ctx.placeholder().Letters(),
          ctx.placeholder().Identifier()).getText();
      StringExpression argument = stringExpression(ctx.stringExpression(0));
      String op = ctx.stringComparisonOp().getText();
      return MultiStringComparisonExpression.of(scope, argument,
          PredicateMatchOp.fromString(ctx.multiModifier.getText()), op);
    } else if (ctx.stringComparisonOp() != null) {
      StringExpression left = stringExpression(ctx.stringExpression(0));
      StringExpression right = stringExpression(ctx.stringExpression(1));
      return StringComparisonExpression.of(left, right, ctx.stringComparisonOp().getText());
    } else if (ctx.in != null && ctx.stringExpression().size() > 1) {
      StringExpression left = stringExpression(ctx.stringExpression(0));
      List<EvalExpression> branches = ctx.stringExpression().
          subList(1, ctx.stringExpression().size()).
          stream().
          map(exp -> StringComparisonExpression.of(left, stringExpression(exp), "=")).
          collect(Collectors.toList());
      return (EvalExpression) entity ->
          asDouble(branches.stream().anyMatch(x -> isTrue(x.getValue(entity))));
    } else if (ctx.stringEvalFunc() != null) {
      StringExpression input = stringExpression(ctx.stringExpression(0));
      if (ctx.stringEvalFunc().strLength() != null) {
        return (EvalExpression) entity -> input.getString(entity).length();
      } else if (ctx.stringEvalFunc().strHashCode() != null) {
        return (EvalExpression) entity -> murmurhash3_x86_32(input.getString(entity));
      } else if (ctx.stringEvalFunc().strIsEmpty() != null) {
        return (EvalExpression) entity -> asDouble(StringUtils.isEmpty(input.getString(entity)));
      } else if (ctx.stringEvalFunc().strIsNotEmpty() != null) {
        return (EvalExpression) entity -> asDouble(StringUtils.isNotEmpty(input.getString(entity)));
      } else if (ctx.stringEvalFunc().strIsBlank() != null) {
        return (EvalExpression) entity -> asDouble(StringUtils.isBlank(input.getString(entity)));
      } else if (ctx.stringEvalFunc().strIsNotBlank() != null) {
        return (EvalExpression) entity -> asDouble(StringUtils.isNotBlank(input.getString(entity)));
      } else if (ctx.stringEvalFunc().strParse() != null) {
        EvalExpression defaultExp = ctx.stringEvalFunc().strParse().evalExpression() == null ? x -> 0 :
            eval(ctx.stringEvalFunc().strParse().evalExpression());
        return (EvalExpression) entity -> {
          try {
            return Double.parseDouble(input.getString(entity));
          } catch (NumberFormatException e) {
            return defaultExp.getValue(entity);
          }
        };
      } else {
        throw new IllegalArgumentException("Unknown string eval function");
      }
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
    } else if (ctx.evalExpression(0) != null) {
      return eval(ctx.evalExpression(0));
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
      if (ctx.stringFunc().strReplace() != null) {
        StringExpression search = stringExpression(ctx.stringFunc().
            strReplace().stringExpression(0));
        StringExpression replacement = stringExpression(ctx.stringFunc().
            strReplace().stringExpression(1));
        return (StringExpression) entity -> input.getString(entity).
            replace(search.getString(entity), replacement.getString(entity));
      } else if (ctx.stringFunc().strReplaceAll() != null) {
        StringExpression regex = stringExpression(ctx.stringFunc().
            strReplaceAll().stringExpression(0));
        StringExpression replacement = stringExpression(ctx.stringFunc().
            strReplaceAll().stringExpression(1));
        return (StringExpression) entity -> input.getString(entity).
            replaceAll(regex.getString(entity), replacement.getString(entity));
      } else if (ctx.stringFunc().strSubstring() != null) {
        EvalExpression fromExp = eval(ctx.stringFunc().strSubstring().evalExpression(0));
        if (ctx.stringFunc().strSubstring().evalExpression().size() > 1) {
          EvalExpression toExp = eval(ctx.stringFunc().strSubstring().evalExpression(1));
          return (StringExpression) entity -> input.getString(entity).
              substring((int) fromExp.getValue(entity), (int) toExp.getValue(entity));
        } else {
          return (StringExpression) entity -> input.getString(entity).
              substring((int) fromExp.getValue(entity));
        }
      } else if (ctx.stringFunc().strLeft() != null) {
        EvalExpression index = eval(ctx.stringFunc().strLeft().evalExpression());
        return (StringExpression) entity -> input.getString(entity).
            substring(0, (int) index.getValue(entity));
      } else if (ctx.stringFunc().strRight() != null) {
        EvalExpression index = eval(ctx.stringFunc().strRight().evalExpression());
        return (StringExpression) entity -> {
          String str = input.getString(entity);
          return str.substring(str.length() - (int) index.getValue(entity));
        };
      } else if (ctx.stringFunc().strToLowerCase() != null) {
        return (StringExpression) entity -> input.getString(entity).toLowerCase();
      } else if (ctx.stringFunc().strToUpperCase() != null) {
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
    return visitChildren(ctx);
  }

  @Override
  public Expression visitIff(ConditionParser.IffContext ctx) {
    if (ctx == null) {
      throw new IllegalArgumentException("Syntax error for if()");
    }
    EvalExpression condition = eval(ctx.evalExpression(0));
    EvalExpression thenExpression = eval(ctx.evalExpression(1));
    EvalExpression elseExpression = eval(ctx.evalExpression(2));
    return (EvalExpression) entity ->
        isTrue(condition.getValue(entity)) ?
            thenExpression.getValue(entity) :
            elseExpression.getValue(entity);
  }

  @Override
  public Expression visitParse(ConditionParser.ParseContext ctx) {
    StringExpression strExp = stringExpression(ctx.stringExpression());
    EvalExpression defaultExp = ctx.evalExpression() == null ? x -> 0 : eval(ctx.evalExpression());
    return (EvalExpression) entity -> {
      try {
        return Double.parseDouble(strExp.getString(entity));
      } catch (NumberFormatException e) {
        return defaultExp.getValue(entity);
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
  public Expression visitEvalLength(ConditionParser.EvalLengthContext ctx) {
    StringExpression exp = stringExpression(ctx.stringExpression());
    return (EvalExpression) entity -> exp.getString(entity).length();
  }

  @Override
  public Expression visitEvalHashCode(ConditionParser.EvalHashCodeContext ctx) {
    StringExpression exp = stringExpression(ctx.stringExpression());
    return (EvalExpression) entity -> murmurhash3_x86_32(exp.getString(entity));
  }

  @Override
  public Expression visitEvalIsEmpty(ConditionParser.EvalIsEmptyContext ctx) {
    StringExpression exp = stringExpression(ctx.stringExpression());
    return (EvalExpression) entity -> asDouble(StringUtils.isEmpty(exp.getString(entity)));
  }

  @Override
  public Expression visitEvalIsNotEmpty(ConditionParser.EvalIsNotEmptyContext ctx) {
    StringExpression exp = stringExpression(ctx.stringExpression());
    return (EvalExpression) entity -> asDouble(StringUtils.isNotEmpty(exp.getString(entity)));
  }

  @Override
  public Expression visitEvalIsBlank(ConditionParser.EvalIsBlankContext ctx) {
    StringExpression exp = stringExpression(ctx.stringExpression());
    return (EvalExpression) entity -> asDouble(StringUtils.isBlank(exp.getString(entity)));
  }

  @Override
  public Expression visitEvalIsNotBlank(ConditionParser.EvalIsNotBlankContext ctx) {
    StringExpression exp = stringExpression(ctx.stringExpression());
    return (EvalExpression) entity -> asDouble(StringUtils.isNotBlank(exp.getString(entity)));
  }

  @Override
  public Expression visitRandom(ConditionParser.RandomContext ctx) {
    return (EvalExpression) entity -> RANDOM.nextDouble();
  }

  @Override
  public Expression visitStrIff(ConditionParser.StrIffContext ctx) {
    if (ctx == null) {
      throw new IllegalArgumentException("Syntax error for if()");
    }
    EvalExpression condition = eval(ctx.evalExpression());
    StringExpression thenExpression = stringExpression(ctx.stringExpression(0));
    StringExpression elseExpression = stringExpression(ctx.stringExpression(1));
    return (StringExpression) entity ->
        isTrue(condition.getValue(entity)) ?
            thenExpression.getString(entity) :
            elseExpression.getString(entity);
  }

  private EvalExpression eval(ConditionParser.EvalExpressionContext ctx) {
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
    String text = numberContext.Number().getText();
    double toReturn = text.startsWith("0x") ? Long.decode(text) : Double.parseDouble(text);
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

  private static int murmurhash3_x86_32(CharSequence data) {
    final int c1 = 0xcc9e2d51;
    final int c2 = 0x1b873593;
    int h1 = 0;
    int pos = 0;
    int end = data.length();
    int k1 = 0;
    int k2 = 0;
    int shift = 0;
    int bits = 0;
    int nBytes = 0;   // length in UTF8 bytes

    while (pos < end) {
      int code = data.charAt(pos++);
      if (code < 0x80) {
        k2 = code;
        bits = 8;
      } else if (code < 0x800) {
        k2 = (0xC0 | (code >> 6))
            | ((0x80 | (code & 0x3F)) << 8);
        bits = 16;
      } else if (code < 0xD800 || code > 0xDFFF || pos >= end) {
        // we check for pos>=end to encode an unpaired surrogate as 3 bytes.
        k2 = (0xE0 | (code >> 12))
            | ((0x80 | ((code >> 6) & 0x3F)) << 8)
            | ((0x80 | (code & 0x3F)) << 16);
        bits = 24;
      } else {
        // surrogate pair
        // int utf32 = pos < end ? (int) data.charAt(pos++) : 0;
        int utf32 = (int) data.charAt(pos++);
        utf32 = ((code - 0xD7C0) << 10) + (utf32 & 0x3FF);
        k2 = (0xff & (0xF0 | (utf32 >> 18)))
            | ((0x80 | ((utf32 >> 12) & 0x3F))) << 8
            | ((0x80 | ((utf32 >> 6) & 0x3F))) << 16
            | (0x80 | (utf32 & 0x3F)) << 24;
        bits = 32;
      }
      k1 |= k2 << shift;
      shift += bits;
      if (shift >= 32) {
        // mix after we have a complete word
        k1 *= c1;
        k1 = (k1 << 15) | (k1 >>> 17);  // ROTL32(k1,15);
        k1 *= c2;

        h1 ^= k1;
        h1 = (h1 << 13) | (h1 >>> 19);  // ROTL32(h1,13);
        h1 = h1 * 5 + 0xe6546b64;

        shift -= 32;
        // unfortunately, java won't let you shift 32 bits off, so we need to check for 0
        if (shift != 0) {
          k1 = k2 >>> (bits - shift);   // bits used == bits - newshift
        } else {
          k1 = 0;
        }
        nBytes += 4;
      }

    } // inner

    // handle tail
    if (shift > 0) {
      nBytes += shift >> 3;
      k1 *= c1;
      k1 = (k1 << 15) | (k1 >>> 17);  // ROTL32(k1,15);
      k1 *= c2;
      h1 ^= k1;
    }

    // finalization
    h1 ^= nBytes;

    // fmix(h1);
    h1 ^= h1 >>> 16;
    h1 *= 0x85ebca6b;
    h1 ^= h1 >>> 13;
    h1 *= 0xc2b2ae35;
    h1 ^= h1 >>> 16;

    return h1;
  }
}
