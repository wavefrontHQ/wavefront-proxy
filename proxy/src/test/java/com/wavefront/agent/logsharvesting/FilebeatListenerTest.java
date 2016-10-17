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
import org.junit.After;
import org.junit.Test;
import org.logstash.beats.Message;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Map;

import oi.thekraken.grok.api.exception.GrokException;
import sunnylabs.report.Histogram;
import sunnylabs.report.ReportPoint;

import static org.easymock.EasyMock.createMock;
import static org.easymock.EasyMock.expectLastCall;
import static org.easymock.EasyMock.mock;
import static org.easymock.EasyMock.replay;
import static org.easymock.EasyMock.reset;
import static org.easymock.EasyMock.verify;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;

/**
 * @author Mori Bellamy (mori@wavefront.com)
 */
public class FilebeatListenerTest {
  private LogsIngestionConfig logsIngestionConfig;
  private FilebeatListener filebeatListenerUnderTest;
  private PointHandler mockPointHandler;
  private Long now = 1476408638L;  // 6:30PM california time Oct 13 2016

  private void setup(String configPath) throws IOException, GrokException, ConfigurationException {
    File configFile = new File(FilebeatListenerTest.class.getClassLoader().getResource(configPath).getPath());
    ObjectMapper objectMapper = new ObjectMapper(new YAMLFactory());
    logsIngestionConfig = objectMapper.readValue(configFile, LogsIngestionConfig.class);
    logsIngestionConfig.aggregationIntervalSeconds = 10000; // HACK: Never call flush automatically.
    logsIngestionConfig.verifyAndInit();
    mockPointHandler = createMock(PointHandler.class);
    filebeatListenerUnderTest = new FilebeatListener(
        mockPointHandler, logsIngestionConfig, null, () -> now);
  }

  private void recieveLog(String log) {
    Map<String, Object> data = Maps.newHashMap();
    data.put("message", log);
    data.put("beat", Maps.newHashMap());
    data.put("@timestamp", "2016-10-13T20:43:45.172Z");
    filebeatListenerUnderTest.onNewMessage(null, new Message(0, data));
  }

  @After
  public void cleanup() {
    filebeatListenerUnderTest = null;
  }

  private void tick(int millis) {
    now += millis;
  }

  private List<ReportPoint> getPoints(int numPoints, String... logLines) throws Exception {
    return getPoints(numPoints, 0, logLines);
  }

  private List<ReportPoint> getPoints(int numPoints, int lagPerLogLine, String... logLines) throws Exception {
    Capture<ReportPoint> reportPointCapture = Capture.newInstance(CaptureType.ALL);
    reset(mockPointHandler);
    mockPointHandler.reportPoint(EasyMock.capture(reportPointCapture), EasyMock.isNull(String.class));
    expectLastCall().times(numPoints);
    replay(mockPointHandler);
    for (String line : logLines) {
      recieveLog(line);
      tick(lagPerLogLine);
    }
    filebeatListenerUnderTest.flush();
    verify(mockPointHandler);
    return reportPointCapture.getValues();
  }

  @Test
  public void testPrefixIsApplied() throws Exception {
    setup("test.yml");
    filebeatListenerUnderTest = new FilebeatListener(
        mockPointHandler, logsIngestionConfig, "myPrefix", () -> now);
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

  @Test
  public void testParseValueFromCombinedApacheLog() throws Exception {
    setup("test.yml");
    assertThat(
        getPoints(3,
            "52.34.54.96 - - [11/Oct/2016:06:35:45 +0000] \"GET /api/alert/summary HTTP/1.0\" " +
                "200 632 \"https://dev-2b.corp.wavefront.com/chart\" " +
                "\"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_6) " +
                "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/53.0.2785.143 Safari/537.36\""
        ),
        containsInAnyOrder(
            ImmutableList.of(
                PointMatchers.matches(632L, "apacheBytes", ImmutableMap.of()),
                PointMatchers.matches(632L, "apacheBytes2", ImmutableMap.of()),
                PointMatchers.matches(200.0, "apacheStatus", ImmutableMap.of())
            )
        ));
  }

  @Test
  public void testIncrementCounterWithImplied1() throws Exception {
    setup("test.yml");
    assertThat(
        getPoints(1, "plainCounter"),
        contains(PointMatchers.matches(1L, "plainCounter", ImmutableMap.of())));
    assertThat(
        getPoints(1, "plainCounter"),
        contains(PointMatchers.matches(2L, "plainCounter", ImmutableMap.of())));
  }

  @Test
  public void testHistogram() throws Exception {
    setup("test.yml");
    String[] lines = new String[100];
    for (int i = 1; i < 101; i++) {
      lines[i - 1] = "histo " + i;
    }
    assertThat(
        getPoints(9, lines),
        containsInAnyOrder(ImmutableList.of(
            PointMatchers.almostMatches(100.0, "myHisto.count", ImmutableMap.of()),
            PointMatchers.almostMatches(1.0, "myHisto.min", ImmutableMap.of()),
            PointMatchers.almostMatches(100.0, "myHisto.max", ImmutableMap.of()),
            PointMatchers.almostMatches(50.5, "myHisto.mean", ImmutableMap.of()),
            PointMatchers.almostMatches(50.5, "myHisto.median", ImmutableMap.of()),
            PointMatchers.almostMatches(75.75, "myHisto.p75", ImmutableMap.of()),
            PointMatchers.almostMatches(95.95, "myHisto.p95", ImmutableMap.of()),
            PointMatchers.almostMatches(99.99, "myHisto.p99", ImmutableMap.of()),
            PointMatchers.almostMatches(100.0, "myHisto.p999", ImmutableMap.of())
        ))
    );
  }

  @Test
  public void testWavefrontHistogram() throws Exception {
    setup("histos.yml");
    String[] lines = new String[100];
    for (int i = 1; i < 101; i++) {
      lines[i - 1] = "histo " + i;
    }
    ReportPoint reportPoint = getPoints(1, lines).get(0);
    assertThat(reportPoint.getValue(), instanceOf(Histogram.class));
    Histogram wavefrontHistogram = (Histogram) reportPoint.getValue();
    assertThat(wavefrontHistogram.getBins(), hasSize(1));
    assertThat(wavefrontHistogram.getBins(), contains(50.5));
    assertThat(wavefrontHistogram.getCounts(), hasSize(1));
    assertThat(wavefrontHistogram.getCounts(), contains(100));
  }

  @Test
  public void testWavefrontHistogramMultipleCentroids() throws Exception {
    setup("histos.yml");
    String[] lines = new String[60];
    for (int i = 1; i < 61; i++) {
      lines[i - 1] = "histo " + i;
    }
    ReportPoint reportPoint = getPoints(1, 1000, lines).get(0);
    assertThat(reportPoint.getValue(), instanceOf(Histogram.class));
    Histogram wavefrontHistogram = (Histogram) reportPoint.getValue();
    assertThat(wavefrontHistogram.getBins(), hasSize(2));
    assertThat(wavefrontHistogram.getCounts(), hasSize(2));
    assertThat(wavefrontHistogram.getCounts().stream().reduce(Integer::sum).get(), equalTo(60));
  }

  @Test
  public void testExpiry() throws Exception {
    setup("expiry.yml");
    assertThat(
        getPoints(1, "plainCounter"),
        contains(PointMatchers.matches(1L, "plainCounter", ImmutableMap.of())));
    tick(5);
    // Flush, so that the FilebeatListener can notice this metric should be evicted.
    filebeatListenerUnderTest.flush();
    // HACK: Give the removal listener time to fire. On a MBP, this test has more than 4 9s reliability.
    Thread.sleep(10);
    // Should have expired, so started a new counter.
    assertThat(
        getPoints(1, "plainCounter"),
        contains(PointMatchers.matches(1L, "plainCounter", ImmutableMap.of())));
  }
}