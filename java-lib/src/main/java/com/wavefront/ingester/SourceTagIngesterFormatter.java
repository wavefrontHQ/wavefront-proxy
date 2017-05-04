package com.wavefront.ingester;

import com.google.common.base.Predicate;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;

import org.antlr.v4.runtime.ANTLRInputStream;
import org.antlr.v4.runtime.BaseErrorListener;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.Lexer;
import org.antlr.v4.runtime.RecognitionException;
import org.antlr.v4.runtime.Recognizer;
import org.antlr.v4.runtime.Token;

import java.util.ArrayDeque;
import java.util.List;
import java.util.Queue;
import java.util.logging.Logger;

import sunnylabs.report.ReportSourceTag;
import queryserver.parser.DSWrapperLexer;

/**
 * This class can be used to parse sourceTags and description.
 *
 * @author Suranjan Pramanik (suranjan@wavefront.com).
 */
public class SourceTagIngesterFormatter {

  private static final Logger logger = Logger.getLogger(SourceTagIngesterFormatter.class
      .getCanonicalName());

  public static final String SOURCE = "source";
  public static final String DESCRIPTION = "description";
  public static final String ACTION = "action";
  public static final String ACTION_SAVE = "save";
  public static final String  ACTION_DELETE ="delete";

  private static final FormatterElement WHITESPACE_ELEMENT = new Whitespace();
  private static final BaseErrorListener THROWING_ERROR_LISTENER = new BaseErrorListener() {
    @Override
    public void syntaxError(Recognizer<?, ?> recognizer, Object offendingSymbol, int line,
                            int charPositionInLine, String msg, RecognitionException e) {
      throw new RuntimeException(msg, e);
    }
  };

  private final List<FormatterElement> elements;
  private final ThreadLocal<DSWrapperLexer> dsWrapperLexerThreadLocal =
      new ThreadLocal<DSWrapperLexer>() {
        @Override
        protected DSWrapperLexer initialValue() {
          final DSWrapperLexer lexer = new DSWrapperLexer(new ANTLRInputStream(""));
          // note that other errors are not thrown by the lexer and hence we only need to handle the
          // syntaxError case.
          lexer.removeErrorListeners();
          lexer.addErrorListener(THROWING_ERROR_LISTENER);
          return lexer;
        }
      };

  private SourceTagIngesterFormatter(List<FormatterElement> elements) {this.elements = elements;}

  public static SourceTagIngesterFormatBuilder newBuilder() {
    return new SourceTagIngesterFormatBuilder();
  }

  public ReportSourceTag drive(String input, String defaultHostName, String customerId,
                               List<String> customerSourceTags) {

    DSWrapperLexer lexer = dsWrapperLexerThreadLocal.get();
    lexer.setInputStream(new ANTLRInputStream(input));
    CommonTokenStream commonTokenStream = new CommonTokenStream(lexer);
    commonTokenStream.getText();
    List<Token> tokens = commonTokenStream.getTokens();
    if (tokens.isEmpty()) {
      throw new RuntimeException("Could not parse: " + input);
    }
    // filter all EOF tokens first.
    Queue<Token> queue = new ArrayDeque<>(Lists.newArrayList(Iterables.filter(tokens,
        new Predicate<Token>() {
          @Override
          public boolean apply(Token input) {
            return input.getType() != Lexer.EOF;
          }
        })));

    ReportSourceTag sourceTag = new ReportSourceTag();
    try {
      for (FormatterElement element : elements) {
        element.consume(queue, sourceTag);
      }
    } catch (Exception ex) {
      throw new RuntimeException("Could not parse: " + input, ex);
    }
    if (!queue.isEmpty()) {
      throw new RuntimeException("Could not parse: " + input);
    }
    // verify the values - especially 'action' field
    if(sourceTag.getSource() == null)
      throw new RuntimeException("No source key was present in the input: " + input);

    if (sourceTag.getAction() != null) {
      // verify that only 'add' or 'delete' is present
      String actionStr = sourceTag.getAction();
      if (!actionStr.equals(ACTION_SAVE) && !actionStr.equals(ACTION_DELETE))
        throw new RuntimeException("Action string did not match save/delete: " + input);
    } else {
      // no value was specified hence throw an exception
      throw new RuntimeException("No action key was present in the input: " + input);
    }
    return sourceTag;
  }

  public static class SourceTagIngesterFormatBuilder {
    private final List<FormatterElement> elements = Lists.newArrayList();

    public SourceTagIngesterFormatBuilder appendCaseSensitiveLiterals(String[] literals) {
      elements.add(new Literal(literals, true));
      return this;
    }

    public SourceTagIngesterFormatBuilder appendLoopOfTags() {
      elements.add(new LoopOfTags());
      return this;
    }

    public SourceTagIngesterFormatBuilder appendLoopOfValues() {
      elements.add(new LoopOfValues());
      return this;
    }

    public SourceTagIngesterFormatBuilder whitespace() {
      elements.add(new Whitespace());
      return this;
    }

    public SourceTagIngesterFormatter build() {return new SourceTagIngesterFormatter(elements);}
  }

  private interface FormatterElement {
    void consume(Queue<Token> tokenQueue, ReportSourceTag sourceTag);
  }

  public static class LoopOfTags implements FormatterElement {

    private final FormatterElement tagElement = new Tag();

    @Override
    public void consume(Queue<Token> tokenQueue, ReportSourceTag sourceTag) {
      if (sourceTag.getSourceTagLiteral() == null) {
        // throw an exception since we expected that field to be populated
        throw new RuntimeException("Expected either @SourceTag or @SourceDescription in the " +
            "message");
      } else if (sourceTag.getSourceTagLiteral().equals("SourceTag")) {
        // process it as a sourceTag -- 2 tag elements; action="add" source="aSource"
        int count = 0, max = 2;
        while (count < max) {
          WHITESPACE_ELEMENT.consume(tokenQueue, sourceTag);
          tagElement.consume(tokenQueue, sourceTag);
          count++;
        }
      } else if (sourceTag.getSourceTagLiteral().equals("SourceDescription")) {
        // process it as a description -- all the remaining should be tags
        while (!tokenQueue.isEmpty()) {
          WHITESPACE_ELEMENT.consume(tokenQueue, sourceTag);
          tagElement.consume(tokenQueue, sourceTag);
        }
      } else {
        // throw exception, since it should be one of the above
        throw new RuntimeException("Expected either @SourceTag or @SourceDescription in the " +
            "message");
      }
    }
  }

  public static class LoopOfValues implements FormatterElement {
    private final FormatterElement valueElement = new Value();
    @Override
    public void consume(Queue<Token> tokenQueue, ReportSourceTag sourceTag) {
      while (!tokenQueue.isEmpty()) {
        WHITESPACE_ELEMENT.consume(tokenQueue, sourceTag);
        if (tokenQueue.isEmpty()) return;
        valueElement.consume(tokenQueue, sourceTag);
      }
    }
  }

  public static class Literal implements FormatterElement {
    private final String[] literals;
    private final boolean caseSensitive;

    public Literal(String[] literals, boolean caseSensitive) {
      this.literals = literals;
      this.caseSensitive = caseSensitive;
    }
    @Override
    public void consume(Queue<Token> tokenQueue, ReportSourceTag sourceTag) {
      if (literals == null || literals.length != 2)
        throw new RuntimeException("Sourcetag metadata parser is not properly initialized.");

      if (tokenQueue.isEmpty()) {
        throw new RuntimeException("Expecting a literal string: " + literals[0] + " or " +
            literals[1] + " but found EOF");
      }
      String literal = getLiteral(tokenQueue);
      if (caseSensitive) {
        for (String specLiteral : literals) {
          if (literal.equals(specLiteral)) {
            sourceTag.setSourceTagLiteral(literal.substring(1));
            return;
          }
        }
        throw new RuntimeException("Expecting a literal string: " + literals[0] + " or " +
            literals[1]  + " but found: " + literal);
      } else {
        for (String specLiteral : literals) {
          if (literal.equalsIgnoreCase(specLiteral)) {
            sourceTag.setSourceTagLiteral(literal.substring(1));
            return;
          }
        }
        throw new RuntimeException("Expecting a literal string: " + literals[0] + " or " +
            literals[1]  + " but found: " + literal);
      }
    }
  }

  public static class Value implements FormatterElement {

    @Override
    public void consume(Queue<Token> tokenQueue, ReportSourceTag sourceTag) {
      WHITESPACE_ELEMENT.consume(tokenQueue, sourceTag);
      String value = "";
      Token current = tokenQueue.poll();
      if (current == null) throw new RuntimeException("Invalid value, found EOF");

      if (current == null) throw new RuntimeException("Invalid value, found EOF");
      if (current.getType() == DSWrapperLexer.Quoted) {
        if (!value.equals("")) {
          throw new RuntimeException("invalid metric value: " + value + current.getText());
        }
        value += IngesterFormatter.unquote(current.getText());
      } else if (current.getType() == DSWrapperLexer.Letters ||
          current.getType() == DSWrapperLexer.Literal ||
          current.getType() == DSWrapperLexer.Number) {
        value += current.getText();
      } else {
        throw new RuntimeException("invalid value: " + current.getText());
      }
      if (sourceTag.getAnnotations() == null)
        sourceTag.setAnnotations(Lists.<String>newArrayList());

      sourceTag.getAnnotations().add(value);
    }
  }

  public static class Tag implements FormatterElement {

    @Override
    public void consume(Queue<Token> queue, ReportSourceTag sourceTag) {
      // extract tags.
      String tagk;
      tagk = getLiteral(queue);
      if (tagk.length() == 0) {
        throw new RuntimeException("Invalid tag name");
      }
      WHITESPACE_ELEMENT.consume(queue, sourceTag);
      Token current = queue.poll();
      if (current == null || current.getType() != DSWrapperLexer.EQ) {
        throw new RuntimeException("Tag keys and values must be separated by '='" +
            (current != null ? ", " + "found: " + current.getText() : ", found EOF"));
      }
      WHITESPACE_ELEMENT.consume(queue, sourceTag);
      String tagv = getLiteral(queue);
      if (tagv.length() == 0) throw new RuntimeException("Invalid tag value for: " + tagk);

      switch (tagk) {
        case SourceTagIngesterFormatter.ACTION:
          sourceTag.setAction(tagv);
          break;
        case SourceTagIngesterFormatter.SOURCE:
          sourceTag.setSource(tagv);
          break;
        case SourceTagIngesterFormatter.DESCRIPTION:
          sourceTag.setDescription(tagv);
          break;
        default:
          throw new RuntimeException("Unknown tag key = " + tagk + " specified.");
      }
    }
  }

  public static class Whitespace implements FormatterElement {

    @Override
    public void consume(Queue<Token> tokens, ReportSourceTag sourceTag) {
      while (!tokens.isEmpty() && tokens.peek().getType() == DSWrapperLexer.WS) {
        tokens.poll();
      }
    }
  }

  private static String getLiteral(Queue<Token> tokens) {
    String toReturn = "";
    Token next = tokens.peek();
    if (next == null) return "";
    if (next.getType() == DSWrapperLexer.Quoted) {
      return IngesterFormatter.unquote(tokens.poll().getText());
    }
    while (next != null && (next.getType() == DSWrapperLexer.Letters ||
        next.getType() == DSWrapperLexer.Number ||
        next.getType() == DSWrapperLexer.SLASH ||
        next.getType() == DSWrapperLexer.AT ||
        next.getType() == DSWrapperLexer.Literal ||
        next.getType() == DSWrapperLexer.IpV4Address ||
        next.getType() == DSWrapperLexer.MinusSign ||
        next.getType() == DSWrapperLexer.IpV6Address)) {
      toReturn += tokens.poll().getText();
      next = tokens.peek();
    }
    return toReturn;
  }
}
