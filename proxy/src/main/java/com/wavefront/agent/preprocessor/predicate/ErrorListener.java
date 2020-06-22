package com.wavefront.agent.preprocessor.predicate;

import org.antlr.v4.runtime.BaseErrorListener;
import org.antlr.v4.runtime.RecognitionException;
import org.antlr.v4.runtime.Recognizer;

/**
 * Parser error listener.
 *
 * @author vasily@wavefront.com
 */
public class ErrorListener extends BaseErrorListener {
  private final StringBuilder errors = new StringBuilder();

  @Override
  public void syntaxError(Recognizer<?, ?> recognizer, Object offendingSymbol, int line,
                          int charPositionInLine, String msg, RecognitionException e) {
    errors.append("Syntax error at position ");
    errors.append(charPositionInLine);
    errors.append(": ");
    errors.append(msg);
    super.syntaxError(recognizer, offendingSymbol, line, charPositionInLine, msg, e);
  }

  public StringBuilder getErrors() {
    return errors;
  }
}
