package com.wavefront.agent.logsharvesting;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.wavefront.agent.PointHandler;
import com.wavefront.agent.PointMatchers;
import com.wavefront.agent.config.ConfigurationException;
import com.wavefront.agent.config.LogsIngestionConfig;
import com.wavefront.agent.config.MetricMatcher;

import org.easymock.Capture;
import org.easymock.CaptureType;
import org.easymock.EasyMock;
import org.junit.After;
import org.junit.Test;
import org.logstash.beats.Message;

import java.io.File;
import java.io.IOException;
import java.net.SocketAddress;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

import javax.annotation.Nullable;

import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import oi.thekraken.grok.api.exception.GrokException;
import sunnylabs.report.Histogram;
import sunnylabs.report.ReportPoint;

import static org.easymock.EasyMock.createMock;
import static org.easymock.EasyMock.expectLastCall;
import static org.easymock.EasyMock.replay;
import static org.easymock.EasyMock.reset;
import static org.easymock.EasyMock.verify;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.notNullValue;

/**
 * @author Mori Bellamy (mori@wavefront.com)
 */
public class LogsIngesterTest {
  private LogsIngestionConfig logsIngestionConfig;
  private LogsIngester logsIngesterUnderTest;
  private FilebeatIngester filebeatIngesterUnderTest;
  private RawLogsIngester rawLogsIngesterUnderTest;
  private PointHandler mockPointHandler;
  private Long now = 1476408638L;  // 6:30PM california time Oct 13 2016
  private ObjectMapper objectMapper = new ObjectMapper(new YAMLFactory());

  private LogsIngestionConfig parseConfigFile(String configPath) throws IOException {
    File configFile = new File(LogsIngesterTest.class.getClassLoader().getResource(configPath).getPath());
    return objectMapper.readValue(configFile, LogsIngestionConfig.class);
  }

  private void setup(String configPath) throws IOException, GrokException, ConfigurationException {
    logsIngestionConfig = parseConfigFile(configPath);
    logsIngestionConfig.aggregationIntervalSeconds = 10000; // HACK: Never call flush automatically.
    logsIngestionConfig.verifyAndInit();
    mockPointHandler = createMock(PointHandler.class);
    logsIngesterUnderTest = new LogsIngester(mockPointHandler, () -> logsIngestionConfig, null, () -> now);
    filebeatIngesterUnderTest = new FilebeatIngester(logsIngesterUnderTest, () -> now);
    rawLogsIngesterUnderTest = new RawLogsIngester(logsIngesterUnderTest, -1, () -> now);
  }

  private void receiveFilebeatLog(String log) {
    Map<String, Object> data = Maps.newHashMap();
    data.put("message", log);
    data.put("beat", Maps.newHashMap());
    data.put("@timestamp", "2016-10-13T20:43:45.172Z");
    filebeatIngesterUnderTest.onNewMessage(null, new Message(0, data));
  }

  private void receiveRawLog(String log) {
    ChannelHandlerContext ctx = EasyMock.createMock(ChannelHandlerContext.class);
    Channel channel = EasyMock.createMock(Channel.class);
    EasyMock.expect(ctx.channel()).andReturn(channel);
    // Hack: Returning a mock SocketAddress simply causes the fallback to be used in getHostOrDefault.
    EasyMock.expect(channel.remoteAddress()).andReturn(EasyMock.createMock(SocketAddress.class));
    EasyMock.replay(ctx, channel);
    rawLogsIngesterUnderTest.ingestLog(ctx, log);
    EasyMock.verify(ctx, channel);
  }

  private void receiveLog(String log) {
    LogsMessage logsMessage = new LogsMessage() {
      @Override
      public String getLogLine() {
        return log;
      }

      @Override
      public String hostOrDefault(String fallbackHost) {
        return "testHost";
      }
    };
    logsIngesterUnderTest.ingestLog(logsMessage);
  }

  @After
  public void cleanup() {
    logsIngesterUnderTest = null;
    filebeatIngesterUnderTest = null;
  }

  private void tick(int millis) {
    now += millis;
  }

  private List<ReportPoint> getPoints(int numPoints, String... logLines) throws Exception {
    return getPoints(numPoints, 0, this::receiveLog, logLines);
  }

  private List<ReportPoint> getPoints(int numPoints, int lagPerLogLine, Consumer<String> consumer, String... logLines)
      throws Exception {
    Capture<ReportPoint> reportPointCapture = Capture.newInstance(CaptureType.ALL);
    reset(mockPointHandler);
    if (numPoints > 0) {
      mockPointHandler.reportPoint(EasyMock.capture(reportPointCapture), EasyMock.notNull(String.class));
      expectLastCall().times(numPoints);
    }
    replay(mockPointHandler);
    for (String line : logLines) {
      consumer.accept(line);
      tick(lagPerLogLine);
    }
    logsIngesterUnderTest.getMetricsReporter().run();
    verify(mockPointHandler);
    return reportPointCapture.getValues();
  }

  @Test
  public void testPrefixIsApplied() throws Exception {
    setup("test.yml");
    logsIngesterUnderTest = new LogsIngester(
        mockPointHandler, () -> logsIngestionConfig, "myPrefix", () -> now);
    assertThat(
        getPoints(1, "plainCounter"),
        contains(PointMatchers.matches(1L, "myPrefix.plainCounter", ImmutableMap.of())));
  }

  @Test
  public void testFilebeatIngester() throws Exception {
    setup("test.yml");
    assertThat(
        getPoints(1, 0, this::receiveFilebeatLog, "plainCounter"),
        contains(PointMatchers.matches(1L, "plainCounter", ImmutableMap.of())));
  }

  @Test
  public void testRawLogsIngester() throws Exception {
    setup("test.yml");
    assertThat(
        getPoints(1, 0, this::receiveRawLog, "plainCounter"),
        contains(PointMatchers.matches(1L, "plainCounter", ImmutableMap.of())));
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
  public void testHotloadedConfigClearsOldMetrics() throws Exception {
    setup("test.yml");
    assertThat(
        getPoints(1, "plainCounter"),
        contains(PointMatchers.matches(1L, "plainCounter", ImmutableMap.of())));
    assertThat(
        getPoints(2, "plainCounter", "counterWithValue 42"),
        containsInAnyOrder(
            ImmutableList.of(
                PointMatchers.matches(42L, "counterWithValue", ImmutableMap.of()),
                PointMatchers.matches(2L, "plainCounter", ImmutableMap.of()))));
    List<MetricMatcher> counters = Lists.newCopyOnWriteArrayList(logsIngestionConfig.counters);
    int oldSize = counters.size();
    counters.removeIf((metricMatcher -> metricMatcher.getPattern().equals("plainCounter")));
    assertThat(counters, hasSize(oldSize - 1));
    // Get a new config file because the SUT has a reference to the old one, and we'll be monkey patching
    // this one.
    logsIngestionConfig = parseConfigFile("test.yml");
    logsIngestionConfig.verifyAndInit();
    logsIngestionConfig.counters = counters;
    logsIngesterUnderTest.logsIngestionConfigManager.forceConfigReload();
    assertThat(
        getPoints(1, "plainCounter"),
        contains(PointMatchers.matches(42L, "counterWithValue", ImmutableMap.of())));
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

  @Test(expected = ClassCastException.class)
  public void testDuplicateMetric() throws Exception {
    setup("dupe.yml");
    assertThat(getPoints(2, "plainCounter", "plainGauge 42"), notNullValue());
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
  public void testProxyLogLine() throws Exception {
    setup("test.yml");
    assertThat(
        getPoints(1, "WARNING: [2878] (SUMMARY): points attempted: 859432; blocked: 0"),
        contains(PointMatchers.matches(859432.0, "wavefrontPointsSent.2878", ImmutableMap.of()))
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
    ReportPoint reportPoint = getPoints(1, 1000, this::receiveLog, lines).get(0);
    assertThat(reportPoint.getValue(), instanceOf(Histogram.class));
    Histogram wavefrontHistogram = (Histogram) reportPoint.getValue();
    assertThat(wavefrontHistogram.getBins(), hasSize(2));
    assertThat(wavefrontHistogram.getCounts(), hasSize(2));
    assertThat(wavefrontHistogram.getCounts().stream().reduce(Integer::sum).get(), equalTo(60));
  }

  @Test(expected = ConfigurationException.class)
  public void testBadName() throws Exception {
    setup("badName.yml");
  }
}