package com.wavefront.ingester;

import org.antlr.v4.runtime.Token;

import java.util.List;
import java.util.Queue;
import java.util.logging.Logger;

import sunnylabs.report.ReportSourceTag;

/**
 * This class can be used to parse sourceTags and description.
 *
 * @author Suranjan Pramanik (suranjan@wavefront.com).
 */
public class SourceTagIngesterFormatter extends AbstractIngesterFormatter {

  private static final Logger logger = Logger.getLogger(SourceTagIngesterFormatter.class
      .getCanonicalName());

  public static final String SOURCE = "source";
  public static final String DESCRIPTION = "description";
  public static final String ACTION = "action";
  public static final String ACTION_SAVE = "save";
  public static final String ACTION_DELETE = "delete";

  private SourceTagIngesterFormatter(List<FormatterElement> elements) {
    super(elements);
  }

  /**
   * Factory method to create an instance of the format builder.
   *
   * @return
   */
  public static SourceTagIngesterFormatBuilder newBuilder() {
    return new SourceTagIngesterFormatBuilder();
  }

  public ReportSourceTag drive(String input, String defaultHostName, String customerId,
                               List<String> customerSourceTags) {

    Queue<Token> queue = getQueue(input);

    ReportSourceTag sourceTag = new ReportSourceTag();
    ReportSourceTagWrapper wrapper = new ReportSourceTagWrapper(sourceTag);
    try {
      for (FormatterElement element : elements) {
        element.consume(queue, wrapper);
      }
    } catch (Exception ex) {
      throw new RuntimeException("Could not parse: " + input, ex);
    }
    if (!queue.isEmpty()) {
      throw new RuntimeException("Could not parse: " + input);
    }
    // verify the values - especially 'action' field
    if (sourceTag.getSource() == null)
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

  /**
   * A builder pattern to create a format for the source tag parser.
   */
  public static class SourceTagIngesterFormatBuilder extends IngesterFormatBuilder {

    @Override
    public SourceTagIngesterFormatter build() {
      return new SourceTagIngesterFormatter(elements);
    }
  }
}
