package com.wavefront.ingester;

import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.logging.Logger;

import wavefront.report.ReportSourceTag;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * This class is used to test the source tag points
 *
 * @author Suranjan Pramanik (suranjan@wavefront.com).
 */
public class SourceTagDecoderTest {
  private static final Logger logger = Logger.getLogger(SourceTagDecoderTest.class
      .getCanonicalName());

  private static final String SOURCE_TAG = "SourceTag";
  private static final String SOURCE_DESCRIPTION = "SourceDescription";

  @Test
  public void testSimpleSourceTagFormat() throws Exception {
    SourceTagDecoder decoder = new SourceTagDecoder();
    List<ReportSourceTag> out = new ArrayList<>();
    // Testwith 3sourceTags
    decoder.decodeSourceTagLine(String.format("@%s %s=%s %s=aSource sourceTag1 sourceTag2 " +
            "sourceTag3", SOURCE_TAG,
        ReportSourceTagIngesterFormatter.ACTION, ReportSourceTagIngesterFormatter.ACTION_SAVE,
        ReportSourceTagIngesterFormatter.SOURCE), out);
    ReportSourceTag reportSourceTag = out.get(0);
    assertEquals("Action name didn't match.", ReportSourceTagIngesterFormatter.ACTION_SAVE,
        reportSourceTag.getAction());
    assertEquals("Source did not match.", "aSource", reportSourceTag.getSource());
    assertTrue("SourceTag1 did not match.", reportSourceTag.getAnnotations().contains
        ("sourceTag1"));

    // Test with one sourceTag
    out.clear();
    decoder.decodeSourceTagLine(String.format("@%s action = %s source = \"A Source\" sourceTag3",
        SOURCE_TAG, ReportSourceTagIngesterFormatter.ACTION_SAVE), out);
    reportSourceTag = out.get(0);
    assertEquals("Action name didn't match.", ReportSourceTagIngesterFormatter.ACTION_SAVE,
        reportSourceTag.getAction());
    assertEquals("Source did not match.", "A Source", reportSourceTag.getSource());
    assertTrue("SourceTag3 did not match.", reportSourceTag.getAnnotations()
        .contains("sourceTag3"));

    // Test with a multi-word source tag
    out.clear();
    decoder.decodeSourceTagLine(String.format("@%s action = %s source=aSource \"A source tag\" " +
        "\"Another tag\"", SOURCE_TAG, ReportSourceTagIngesterFormatter.ACTION_SAVE), out);
    reportSourceTag = out.get(0);
    assertEquals("Action name didn't match.", ReportSourceTagIngesterFormatter.ACTION_SAVE,
        reportSourceTag.getAction());
    assertEquals("Source did not match.", "aSource", reportSourceTag.getSource());
    assertTrue("'A source tag' did not match.", reportSourceTag.getAnnotations()
        .contains("A source tag"));
    assertTrue("'Another tag' did not match", reportSourceTag.getAnnotations()
        .contains("Another tag"));

    // Test sourceTag without any action -- this should result in an exception
    String msg = String.format("@%s source=aSource sourceTag4 sourceTag5", SOURCE_TAG);
    out.clear();
    boolean isException = false;
    try {
      decoder.decodeSourceTagLine(msg, out);
    } catch (Exception ex) {
      isException = true;
      logger.info(ex.getMessage());
    }
    assertTrue("Did not see an exception for SourceTag message without an action for input : " +
        msg, isException);

    // Test sourceTag without any source -- this should result in an exception
    msg = String.format("@%s action=save description=desc sourceTag5", SOURCE_TAG);
    out.clear();
    isException = false;
    try {
      decoder.decodeSourceTagLine(msg, out);
    } catch (Exception ex) {
      isException = true;
      logger.info(ex.getMessage());
    }
    assertTrue("Did not see an exception for SourceTag message without a source for input : " +
        msg, isException);

    // Test sourceTag with action, source, and description -- this should result in an exception
    out.clear();
    msg = String.format("@%s action = save source = aSource description = desc sourceTag5",
        SOURCE_TAG);
    isException = false;
    try {
      decoder.decodeSourceTagLine(msg, out);
    } catch (Exception ex) {
      isException = true;
      logger.info(ex.getMessage());
    }
    assertTrue("Did not see an exception when description was present in SourceTag message for " +
            "input : " + msg, isException);

    // Test sourceTag message without the source field -- should throw an exception
    out.clear();
    isException = false;
    msg = "@SourceTag action=remove sourceTag3";
    try {
      decoder.decodeSourceTagLine(msg, out);
    } catch (Exception ex) {
      isException = true;
      logger.info(ex.getMessage());
    }
    assertTrue("Did not see an exception when source field was absent for input: " + msg,
        isException);

    // Test a message where action is not save or delete -- should throw an exception
    out.clear();
    isException = false;
    msg = String.format("@%s action = anAction source=aSource sourceTag2 sourceTag3", SOURCE_TAG);
    try {
      decoder.decodeSourceTagLine(msg, out);
    } catch (Exception ex) {
      isException = true;
      logger.info(ex.getMessage());
    }
    assertTrue("Did not see an exception when action field was invalid for input : " + msg,
        isException);
  }

  @Test
  public void testSimpleSourceDescriptions() throws Exception {
      SourceTagDecoder decoder = new SourceTagDecoder();
      List<ReportSourceTag> out = new ArrayList<>();
      // Testwith source description
      decoder.decodeSourceTagLine(String.format("@%s %s=%s %s= aSource description=desc",
          SOURCE_DESCRIPTION,
          ReportSourceTagIngesterFormatter.ACTION, ReportSourceTagIngesterFormatter.ACTION_SAVE,
          ReportSourceTagIngesterFormatter.SOURCE), out);
      ReportSourceTag reportSourceTag = out.get(0);
    assertEquals("Action name didn't match.", ReportSourceTagIngesterFormatter.ACTION_SAVE,
          reportSourceTag.getAction());
      assertEquals("Source did not match.", "aSource", reportSourceTag.getSource());
      assertEquals("Description did not match.", "desc", reportSourceTag.getDescription());

      // Test delete action where description field is not necessary
      out.clear();
      String format = String.format("@%s %s=%s %s=aSource", SOURCE_DESCRIPTION,
          ReportSourceTagIngesterFormatter.ACTION, ReportSourceTagIngesterFormatter.ACTION_DELETE,
          ReportSourceTagIngesterFormatter.SOURCE);
      decoder.decodeSourceTagLine(format, out);
      reportSourceTag = out.get(0);
    assertEquals("Action name did not match for input : " + format, ReportSourceTagIngesterFormatter
              .ACTION_DELETE,
          reportSourceTag.getAction());
      assertEquals("Source did not match for input : " + format, "aSource", reportSourceTag
          .getSource());

      // Add a source tag to the SourceDescription message -- this should cause an exception
      out.clear();
      String msg = String.format("@%s action = save source = aSource description = desc " +
          "sourceTag4", SOURCE_DESCRIPTION);
      boolean isException = false;
      try {
        decoder.decodeSourceTagLine(msg, out);
      } catch (Exception ex) {
        isException = true;
        logger.info(ex.getMessage());
      }
      assertTrue("Expected an exception, since source tag was supplied in SourceDescription " +
          "message for input : " + msg, isException);

  }
}
