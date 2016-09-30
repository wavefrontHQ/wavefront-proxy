package com.wavefront.agent.logsharvesting;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.wavefront.agent.PointHandler;
import com.wavefront.agent.PointMatchers;
import com.wavefront.agent.config.ConfigurationException;
import com.wavefront.agent.config.LogsIngestionConfig;

import org.easymock.Capture;
import org.easymock.CaptureType;
import org.easymock.EasyMock;
import org.junit.Test;
import org.logstash.beats.Message;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Map;

import oi.thekraken.grok.api.exception.GrokException;
import sunnylabs.report.ReportPoint;

import static org.easymock.EasyMock.createMock;
import static org.easymock.EasyMock.expectLastCall;
import static org.easymock.EasyMock.replay;
import static org.easymock.EasyMock.reset;
import static org.easymock.EasyMock.verify;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;

/**
 * @author Mori Bellamy (mori@wavefront.com)
 */
public class FilebeatListenerTest {
  private LogsIngestionConfig logsIngestionConfig;
  private FilebeatListener filebeatListenerUnderTest;
  private PointHandler mockPointHandler;

  private void setup(String configPath) throws IOException, GrokException, ConfigurationException {
    File configFile = new File(FilebeatListenerTest.class.getClassLoader().getResource(configPath).getPath());
    ObjectMapper objectMapper = new ObjectMapper(new YAMLFactory());
    logsIngestionConfig = objectMapper.readValue(configFile, LogsIngestionConfig.class);
    logsIngestionConfig.aggregationIntervalSeconds = 5;
    logsIngestionConfig.verify();
    mockPointHandler = createMock(PointHandler.class);
    filebeatListenerUnderTest = new FilebeatListener(
        mockPointHandler, logsIngestionConfig, "myhostname", null, false);
  }

  private void recieveLog(String log) {
    Map<String, Object> data = Maps.newHashMap();
    data.put("message", log);
    data.put("beat", Maps.newHashMap());
    filebeatListenerUnderTest.onNewMessage(null, new Message(0, data));
  }

  private List<ReportPoint> getPoints(int numPoints, String... logLines) throws Exception {
    Capture<ReportPoint> reportPointCapture = Capture.newInstance(CaptureType.ALL);
    reset(mockPointHandler);
    mockPointHandler.reportPoint(EasyMock.capture(reportPointCapture), EasyMock.isNull(String.class));
    expectLastCall().times(numPoints);
    replay(mockPointHandler);
    for (String line : logLines) recieveLog(line);
    filebeatListenerUnderTest.flush();
    verify(mockPointHandler);
    return reportPointCapture.getValues();
  }

  @Test
  public void testPrefixIsApplied() throws Exception {
    setup("test.yml");
    filebeatListenerUnderTest = new FilebeatListener(
        mockPointHandler, logsIngestionConfig, "myhostname", "myPrefix", false);
    assertThat(
        getPoints(1, "plainCounter"),
        contains(PointMatchers.matches(1L, "myPrefix.plainCounter", ImmutableMap.of())));
  }

  @Test(expected = ConfigurationException.class)
  public void testGaugeWithoutValue() throws Exception {
    setup("badGauge.yml");
  }

  @Test(expected = ConfigurationException.class)
  public void testTagsNonParallelArrays() throws Exception {
    setup("badTags.yml");
  }

  @Test
  public void testMetricsAggregation() throws Exception {
    setup("test.yml");
    assertThat(
        getPoints(6,
            "plainCounter", "noMatch 42.123 bar", "plainCounter",
            "gauges 42",
            "counterWithValue 2", "counterWithValue 3",
            "dynamicCounter foo 1 done", "dynamicCounter foo 2 done", "dynamicCounter baz 1 done"),
        containsInAnyOrder(
            ImmutableList.of(
                PointMatchers.matches(2L, "plainCounter", ImmutableMap.of()),
                PointMatchers.matches(5L, "counterWithValue", ImmutableMap.of()),
                PointMatchers.matches(1L, "dynamic_foo_1", ImmutableMap.of()),
                PointMatchers.matches(1L, "dynamic_foo_2", ImmutableMap.of()),
                PointMatchers.matches(1L, "dynamic_baz_1", ImmutableMap.of()),
                PointMatchers.matches(42.0, "myGauge", ImmutableMap.of())))
    );
  }

  @Test
  public void testDynamicLabels() throws Exception {
    setup("test.yml");
    assertThat(
        getPoints(3,
            "operation foo took 2 seconds in DC=wavefront AZ=2a",
            "operation foo took 2 seconds in DC=wavefront AZ=2a",
            "operation foo took 3 seconds in DC=wavefront AZ=2b",
            "operation bar took 4 seconds in DC=wavefront AZ=2a"),
        containsInAnyOrder(
            ImmutableList.of(
                PointMatchers.matches(4L, "foo.totalSeconds", ImmutableMap.of("theDC", "wavefront", "theAZ", "2a")),
                PointMatchers.matches(3L, "foo.totalSeconds", ImmutableMap.of("theDC", "wavefront", "theAZ", "2b")),
                PointMatchers.matches(4L, "bar.totalSeconds", ImmutableMap.of("theDC", "wavefront", "theAZ", "2a"))
            )
        ));
  }

  @Test
  public void testAdditionalPatterns() throws Exception {
    setup("test.yml");
    assertThat(
        getPoints(1, "foo and 42"),
        contains(PointMatchers.matches(42L, "customPatternCounter", ImmutableMap.of())));
  }
}