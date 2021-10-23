package com.wavefront.agent.logforwarder.ingestion.processors.model.event.parser;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.IOException;
import java.io.InputStream;

/**
 * @author Manoj Ramakrishnan (rmanoj@vmware.com).
 * @since 10/21/21 2:48 PM
 */
public class TestData {
  public static final String requestBody = "{" + "\"messages\": [" + "{" + "\"text\": \"message1\","
      + "\"timestamp\": 1491782400000," + "\"fields\": [" + "{ \"name\": \"name1\", \"content\": \"content1\" }"
      + "]" + "}," + "{" + "\"text\": \"message2\"," + "\"timestamp\": 1491782400000," + "\"fields\": ["
      + "{ \"name\": \"name1\", \"content\": \"content2\" }" + "]" + "}" + "]" + "}";
  private static final Logger logger = LoggerFactory.getLogger(TestData.class);
  private static ObjectMapper jsonMapper = new ObjectMapper();

  static {
    jsonMapper.setSerializationInclusion(JsonInclude.Include.NON_NULL);
    jsonMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
  }

  public static String getCloudTrailLogMessageSample1() throws IOException {
    return readFile("/ingestion-data/CloudTrailLogMessageSample1.json");
  }

  public static String readFile(String filepath) throws IOException {
    try (InputStream testFileStream = TestData.class.getResourceAsStream(filepath)) {
      return IOUtils.toString(testFileStream, "UTF-8");
    }
  }

  public static String getValidWavefrontFormatLineMetrics() throws IOException {
    return readFile("/WavefrontValidLineFormat.txt");
  }

  public static String getValidWavefrontFormatJsonMetrics() throws IOException {
    return readFile("/WavefrontValidJsonFormat.txt");
  }

  public static String getSampleFirehoseRequest() throws IOException {
    return readFile("/FirehoseHttpInputPayload1.json");
  }

  public static String getSampleFirehoseParsedData() throws IOException {
    return readFile("/FirehoseHttpExpectedPayload1.json");
  }

  public static String getSampleGcpPubSubRequestWithEncodedField() throws IOException {
    return readFile("/GcpPubSubHttpInputPayload1.json");
  }

  public static String getSampleGcpPubSubParsedDataWithEncodedField() throws IOException {
    return readFile("/GcpPubSubHttpExpectedPayload1.json");
  }

  public static String getSampleGcpPubSubRequestWithOutEncodedField() throws IOException {
    return readFile("/GcpPubSubHttpInputPayload2.json");
  }

  public static String getSampleGcpPubSubParsedDataWithOutEncodedField() throws IOException {
    return readFile("/GcpPubSubHttpExpectedPayload2.json");
  }

  private static <T> T readFile(String fileName, Class<T> objectClazz) {
    try (InputStream is = TestData.class.getResourceAsStream(fileName)) {
      return jsonMapper.readValue(is, objectClazz);
    } catch (Exception e) {
      logger.error(e.toString());
    }
    return null;
  }
}