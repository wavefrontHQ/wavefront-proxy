package com.wavefront.agent.logsharvesting;

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

import org.easymock.Capture;
import org.easymock.CaptureType;
import org.easymock.EasyMock;
import org.junit.After;
import org.junit.Test;
import org.logstash.beats.Message;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.wavefront.agent.PointMatchers;
import com.wavefront.agent.auth.TokenAuthenticatorBuilder;
import com.wavefront.agent.channel.NoopHealthCheckManager;
import com.wavefront.agent.config.ConfigurationException;
import com.wavefront.agent.config.LogsIngestionConfig;
import com.wavefront.agent.config.MetricMatcher;
import com.wavefront.agent.handlers.HandlerKey;
import com.wavefront.agent.handlers.ReportableEntityHandler;
import com.wavefront.agent.handlers.ReportableEntityHandlerFactory;
import com.wavefront.agent.listeners.RawLogsIngesterPortUnificationHandler;
import com.wavefront.common.MetricConstants;
import com.wavefront.data.ReportableEntityType;

import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import oi.thekraken.grok.api.exception.GrokException;
import wavefront.report.Histogram;
import wavefront.report.ReportPoint;

import static org.easymock.EasyMock.createMock;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.expectLastCall;
import static org.easymock.EasyMock.replay;
import static org.easymock.EasyMock.reset;
import static org.easymock.EasyMock.verify;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.emptyIterable;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.lessThan;

/**
 * @author Mori Bellamy (mori@wavefront.com)
 */
public class LogsIngesterTest {
  private LogsIngestionConfig logsIngestionConfig;
  private LogsIngester logsIngesterUnderTest;
  private FilebeatIngester filebeatIngesterUnderTest;
  private RawLogsIngesterPortUnificationHandler rawLogsIngesterUnderTest;
  private ReportableEntityHandlerFactory mockFactory;
  private ReportableEntityHandler<ReportPoint, String> mockPointHandler;
  private ReportableEntityHandler<ReportPoint, String> mockHistogramHandler;
  private AtomicLong now = new AtomicLong((System.currentTimeMillis() / 60000) * 60000);
  private AtomicLong nanos = new AtomicLong(System.nanoTime());
  private ObjectMapper objectMapper = new ObjectMapper(new YAMLFactory());

  private LogsIngestionConfig parseConfigFile(String configPath) throws IOException {
    File configFile = new File(LogsIngesterTest.class.getClassLoader().getResource(configPath).getPath());
    return objectMapper.readValue(configFile, LogsIngestionConfig.class);
  }

  private void setup(LogsIngestionConfig config) throws IOException, GrokException, ConfigurationException {
    logsIngestionConfig = config;
    logsIngestionConfig.aggregationIntervalSeconds = 10000; // HACK: Never call flush automatically.
    logsIngestionConfig.verifyAndInit();
    mockPointHandler = createMock(ReportableEntityHandler.class);
    mockHistogramHandler = createMock(ReportableEntityHandler.class);
    mockFactory = createMock(ReportableEntityHandlerFactory.class);
    expect((ReportableEntityHandler) mockFactory.getHandler(HandlerKey.of(ReportableEntityType.POINT, "logs-ingester"))).
        andReturn(mockPointHandler).anyTimes();
    expect((ReportableEntityHandler) mockFactory.getHandler(HandlerKey.of(ReportableEntityType.HISTOGRAM, "logs-ingester"))).
        andReturn(mockHistogramHandler).anyTimes();
    replay(mockFactory);
    logsIngesterUnderTest = new LogsIngester(mockFactory, () -> logsIngestionConfig, null,
        now::get, nanos::get);
    logsIngesterUnderTest.start();
    filebeatIngesterUnderTest = new FilebeatIngester(logsIngesterUnderTest, now::get);
    rawLogsIngesterUnderTest = new RawLogsIngesterPortUnificationHandler("12345",
        logsIngesterUnderTest, x -> "testHost", TokenAuthenticatorBuilder.create().build(),
        new NoopHealthCheckManager(), null);
  }

  private void receiveRawLog(String log) {
    ChannelHandlerContext ctx = EasyMock.createMock(ChannelHandlerContext.class);
    Channel channel = EasyMock.createMock(Channel.class);
    EasyMock.expect(ctx.channel()).andReturn(channel);
    InetSocketAddress addr = InetSocketAddress.createUnresolved("testHost", 1234);
    EasyMock.expect(channel.remoteAddress()).andReturn(addr);
    EasyMock.replay(ctx, channel);
    rawLogsIngesterUnderTest.processLine(ctx, log, null);
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

  private void tick(long millis) {
    now.addAndGet(millis);
  }

  private List<ReportPoint> getPoints(int numPoints, String... logLines) throws Exception {
    return getPoints(numPoints, 0, this::receiveLog, logLines);
  }

  private List<ReportPoint> getPoints(int numPoints, int lagPerLogLine, Consumer <String> consumer,
                                      String... logLines) throws Exception {
    return getPoints(mockPointHandler, numPoints, lagPerLogLine, consumer, logLines);
  }

  private List<ReportPoint> getPoints(ReportableEntityHandler<ReportPoint, String> handler, int numPoints, int lagPerLogLine,
                                      Consumer <String> consumer, String... logLines)
      throws Exception {
    Capture<ReportPoint> reportPointCapture = Capture.newInstance(CaptureType.ALL);
    reset(handler);
    if (numPoints > 0) {
      handler.report(EasyMock.capture(reportPointCapture));
      expectLastCall().times(numPoints);
    }
    replay(handler);
    for (String line : logLines) {
      consumer.accept(line);
      tick(lagPerLogLine);
    }

    // Simulate that one minute has elapsed and histogram bins are ready to be flushed ...
    tick(60000L);

    logsIngesterUnderTest.getMetricsReporter().run();
    verify(handler);
    return reportPointCapture.getValues();
  }

  @Test
  public void testPrefixIsApplied() throws Exception {
    setup(parseConfigFile("test.yml"));
    logsIngesterUnderTest = new LogsIngester(
        mockFactory, () -> logsIngestionConfig, "myPrefix", now::get, nanos::get);
    assertThat(
        getPoints(1, "plainCounter"),
        contains(PointMatchers.matches(1L, MetricConstants.DELTA_PREFIX + "myPrefix" +
            ".plainCounter", ImmutableMap.of())));
  }

  @Test
  public void testFilebeatIngesterDefaultHostname() throws Exception {
    setup(parseConfigFile("test.yml"));
    assertThat(
        getPoints(1, 0, log -> {
          Map<String, Object> data = Maps.newHashMap();
          data.put("message", log);
          data.put("beat", Maps.newHashMap());
          data.put("@timestamp", "2016-10-13T20:43:45.172Z");
          filebeatIngesterUnderTest.onNewMessage(null, new Message(0, data));
        }, "plainCounter"),
        contains(PointMatchers.matches(1L, MetricConstants.DELTA_PREFIX + "plainCounter", "parsed-logs",
            ImmutableMap.of())));
  }

  @Test
  public void testFilebeatIngesterOverrideHostname() throws Exception {
    setup(parseConfigFile("test.yml"));
    assertThat(
        getPoints(1, 0, log -> {
          Map<String, Object> data = Maps.newHashMap();
          data.put("message", log);
          data.put("beat", new HashMap<>(ImmutableMap.of("hostname", "overrideHostname")));
          data.put("@timestamp", "2016-10-13T20:43:45.172Z");
          filebeatIngesterUnderTest.onNewMessage(null, new Message(0, data));
        }, "plainCounter"),
        contains(PointMatchers.matches(1L, MetricConstants.DELTA_PREFIX + "plainCounter", "overrideHostname",
            ImmutableMap.of())));
  }


  @Test
  public void testFilebeat7Ingester() throws Exception {
    setup(parseConfigFile("test.yml"));
    assertThat(
        getPoints(1, 0, log -> {
          Map<String, Object> data = Maps.newHashMap();
          data.put("message", log);
          data.put("host", ImmutableMap.of("name", "filebeat7hostname"));
          data.put("@timestamp", "2016-10-13T20:43:45.172Z");
          filebeatIngesterUnderTest.onNewMessage(null, new Message(0, data));
        }, "plainCounter"),
        contains(PointMatchers.matches(1L, MetricConstants.DELTA_PREFIX + "plainCounter", "filebeat7hostname",
            ImmutableMap.of())));
  }

  @Test
  public void testFilebeat7IngesterAlternativeHostname() throws Exception {
    setup(parseConfigFile("test.yml"));
    assertThat(
        getPoints(1, 0, log -> {
          Map<String, Object> data = Maps.newHashMap();
          data.put("message", log);
          data.put("agent", ImmutableMap.of("hostname", "filebeat7althost"));
          data.put("@timestamp", "2016-10-13T20:43:45.172Z");
          filebeatIngesterUnderTest.onNewMessage(null, new Message(0, data));
        }, "plainCounter"),
        contains(PointMatchers.matches(1L, MetricConstants.DELTA_PREFIX + "plainCounter", "filebeat7althost",
            ImmutableMap.of())));
  }

  @Test
  public void testRawLogsIngester() throws Exception {
    setup(parseConfigFile("test.yml"));
    assertThat(
        getPoints(1, 0, this::receiveRawLog, "plainCounter"),
        contains(PointMatchers.matches(1L, MetricConstants.DELTA_PREFIX + "plainCounter",
            ImmutableMap.of())));
  }

  @Test
  public void testEmptyTagValuesIgnored() throws Exception {
    setup(parseConfigFile("test.yml"));
    assertThat(
        getPoints(1, 0, this::receiveRawLog, "pingSSO|2020-03-13 09:11:30,490|OAuth| RLin123456| " +
            "10.0.0.1 | | pa_wam| OAuth20| sso2-prod| AS| success| SecurID| | 249"),
            contains(PointMatchers.matches(249.0, "pingSSO",
                ImmutableMap.<String, String>builder().put("sso_host", "sso2-prod").
                    put("protocol", "OAuth20").put("role", "AS").put("subject", "RLin123456").
                    put("ip", "10.0.0.1").put("connectionid", "pa_wam").
                    put("adapterid", "SecurID").put("event", "OAuth").put("status", "success").
                    build())));
  }

  @Test(expected = ConfigurationException.class)
  public void testGaugeWithoutValue() throws Exception {
    setup(parseConfigFile("badGauge.yml"));
  }

  @Test(expected = ConfigurationException.class)
  public void testTagsNonParallelArrays() throws Exception {
    setup(parseConfigFile("badTags.yml"));
  }

  @Test
  public void testHotloadedConfigClearsOldMetrics() throws Exception {
    setup(parseConfigFile("test.yml"));
    assertThat(
        getPoints(1, "plainCounter"),
        contains(PointMatchers.matches(1L, MetricConstants.DELTA_PREFIX + "plainCounter",
            ImmutableMap.of())));
    // once the counter is reported, it is reset because now it is treated as delta counter.
    // hence we check that plainCounter has value 1L below.
    assertThat(
        getPoints(2, "plainCounter", "counterWithValue 42"),
        containsInAnyOrder(
            ImmutableList.of(
                PointMatchers.matches(42L, MetricConstants.DELTA_PREFIX + "counterWithValue",
                    ImmutableMap.of()),
                PointMatchers.matches(1L, MetricConstants.DELTA_PREFIX + "plainCounter",
                    ImmutableMap.of()))));
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
    // once the counter is reported, it is reset because now it is treated as delta counter.
    // since zero values are filtered out, no new values are expected.
    assertThat(
        getPoints(0, "plainCounter"),
        emptyIterable());
  }

  @Test
  public void testEvictedDeltaMetricReportingAgain() throws Exception {
    setup(parseConfigFile("test.yml"));
    assertThat(
        getPoints(1, "plainCounter", "plainCounter", "plainCounter", "plainCounter"),
        contains(PointMatchers.matches(4L, MetricConstants.DELTA_PREFIX + "plainCounter",
            ImmutableMap.of())));
    nanos.addAndGet(1_800_000L * 1000L * 1000L);
    assertThat(
        getPoints(1, "plainCounter", "plainCounter", "plainCounter"),
        contains(PointMatchers.matches(3L, MetricConstants.DELTA_PREFIX + "plainCounter",
            ImmutableMap.of())));
    nanos.addAndGet(3_601_000L * 1000L * 1000L);
    assertThat(
        getPoints(1, "plainCounter", "plainCounter"),
        contains(PointMatchers.matches(2L, MetricConstants.DELTA_PREFIX + "plainCounter",
            ImmutableMap.of())));
  }

  @Test
  public void testEvictedMetricReportingAgain() throws Exception {
    setup(parseConfigFile("test-non-delta.yml"));
    assertThat(
        getPoints(1, "plainCounter", "plainCounter", "plainCounter", "plainCounter"),
        contains(PointMatchers.matches(4L, "plainCounter", ImmutableMap.of())));
    nanos.addAndGet(1_800_000L * 1000L * 1000L);
    assertThat(
        getPoints(1, "plainCounter", "plainCounter", "plainCounter"),
        contains(PointMatchers.matches(7L, "plainCounter", ImmutableMap.of())));
    nanos.addAndGet(3_601_000L * 1000L * 1000L);
    assertThat(
        getPoints(1, "plainCounter", "plainCounter"),
        contains(PointMatchers.matches(2L, "plainCounter", ImmutableMap.of())));
  }

  @Test
  public void testMetricsAggregation() throws Exception {
    setup(parseConfigFile("test.yml"));
    assertThat(
        getPoints(6,
            "plainCounter", "noMatch 42.123 bar", "plainCounter",
            "gauges 42",
            "counterWithValue 2", "counterWithValue 3",
            "dynamicCounter foo 1 done", "dynamicCounter foo 2 done", "dynamicCounter baz 1 done"),
        containsInAnyOrder(
            ImmutableList.of(
                PointMatchers.matches(2L, MetricConstants.DELTA_PREFIX + "plainCounter",
                    ImmutableMap.of()),
                PointMatchers.matches(5L, MetricConstants.DELTA_PREFIX + "counterWithValue",
                    ImmutableMap.of()),
                PointMatchers.matches(1L, MetricConstants.DELTA_PREFIX + "dynamic_foo_1",
                    ImmutableMap.of()),
                PointMatchers.matches(1L, MetricConstants.DELTA_PREFIX + "dynamic_foo_2",
                    ImmutableMap.of()),
                PointMatchers.matches(1L, MetricConstants.DELTA_PREFIX + "dynamic_baz_1",
                    ImmutableMap.of()),
                PointMatchers.matches(42.0, "myGauge", ImmutableMap.of())))
    );
  }

  @Test
  public void testMetricsAggregationNonDeltaCounters() throws Exception {
    LogsIngestionConfig config = parseConfigFile("test.yml");
    config.useDeltaCounters = false;
    setup(config);
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
  public void testExtractHostname() throws Exception {
    setup(parseConfigFile("test.yml"));
    assertThat(
        getPoints(3,
            "operation foo on host web001 took 2 seconds",
            "operation foo on host web001 took 2 seconds",
            "operation foo on host web002 took 3 seconds",
            "operation bar on host web001 took 4 seconds"),
        containsInAnyOrder(
            ImmutableList.of(
                PointMatchers.matches(4L, MetricConstants.DELTA_PREFIX + "Host.foo.totalSeconds",
                    "web001.acme.corp", ImmutableMap.of("static", "value")),
                PointMatchers.matches(3L, MetricConstants.DELTA_PREFIX + "Host.foo.totalSeconds",
                    "web002.acme.corp", ImmutableMap.of("static", "value")),
                PointMatchers.matches(4L, MetricConstants.DELTA_PREFIX + "Host.bar.totalSeconds",
                    "web001.acme.corp", ImmutableMap.of("static", "value"))
            )
        ));

  }

  /**
   * This test is not required, because delta counters have different naming convention than gauges

   @Test(expected = ClassCastException.class)
   public void testDuplicateMetric() throws Exception {
   setup(parseConfigFile("dupe.yml"));
   assertThat(getPoints(2, "plainCounter", "plainGauge 42"), notNullValue());
   }
   */

  @Test
  public void testDynamicLabels() throws Exception {
    setup(parseConfigFile("test.yml"));
    assertThat(
        getPoints(3,
            "operation foo took 2 seconds in DC=wavefront AZ=2a",
            "operation foo took 2 seconds in DC=wavefront AZ=2a",
            "operation foo took 3 seconds in DC=wavefront AZ=2b",
            "operation bar took 4 seconds in DC=wavefront AZ=2a"),
        containsInAnyOrder(
            ImmutableList.of(
                PointMatchers.matches(4L, MetricConstants.DELTA_PREFIX + "foo.totalSeconds",
                    ImmutableMap.of("theDC", "wavefront", "theAZ", "2a")),
                PointMatchers.matches(3L, MetricConstants.DELTA_PREFIX + "foo.totalSeconds",
                    ImmutableMap.of("theDC", "wavefront", "theAZ", "2b")),
                PointMatchers.matches(4L, MetricConstants.DELTA_PREFIX + "bar.totalSeconds",
                    ImmutableMap.of("theDC", "wavefront", "theAZ", "2a"))
            )
        ));
  }

  @Test
  public void testDynamicTagValues() throws Exception {
    setup(parseConfigFile("test.yml"));
    assertThat(
        getPoints(3,
            "operation TagValue foo took 2 seconds in DC=wavefront AZ=2a",
            "operation TagValue foo took 2 seconds in DC=wavefront AZ=2a",
            "operation TagValue foo took 3 seconds in DC=wavefront AZ=2b",
            "operation TagValue bar took 4 seconds in DC=wavefront AZ=2a"),
        containsInAnyOrder(
            ImmutableList.of(
                PointMatchers.matches(4L, MetricConstants.DELTA_PREFIX +
                        "TagValue.foo.totalSeconds",
                    ImmutableMap.of("theDC", "wavefront", "theAZ", "az-2a", "static", "value", "noMatch", "aa%{q}bb")),
                PointMatchers.matches(3L, MetricConstants.DELTA_PREFIX +
                        "TagValue.foo.totalSeconds",
                    ImmutableMap.of("theDC", "wavefront", "theAZ", "az-2b", "static", "value", "noMatch", "aa%{q}bb")),
                PointMatchers.matches(4L, MetricConstants.DELTA_PREFIX +
                        "TagValue.bar.totalSeconds",
                    ImmutableMap.of("theDC", "wavefront", "theAZ", "az-2a", "static", "value", "noMatch", "aa%{q}bb"))
            )
        ));
  }

  @Test
  public void testAdditionalPatterns() throws Exception {
    setup(parseConfigFile("test.yml"));
    assertThat(
        getPoints(1, "foo and 42"),
        contains(PointMatchers.matches(42L, MetricConstants.DELTA_PREFIX +
            "customPatternCounter", ImmutableMap.of())));
  }

  @Test
  public void testParseValueFromCombinedApacheLog() throws Exception {
    setup(parseConfigFile("test.yml"));
    assertThat(
        getPoints(3,
            "52.34.54.96 - - [11/Oct/2016:06:35:45 +0000] \"GET /api/alert/summary HTTP/1.0\" " +
                "200 632 \"https://dev-2b.corp.wavefront.com/chart\" " +
                "\"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_6) " +
                "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/53.0.2785.143 Safari/537.36\""
        ),
        containsInAnyOrder(
            ImmutableList.of(
                PointMatchers.matches(632L, MetricConstants.DELTA_PREFIX + "apacheBytes",
                    ImmutableMap.of()),
                PointMatchers.matches(632L, MetricConstants.DELTA_PREFIX + "apacheBytes2",
                    ImmutableMap.of()),
                PointMatchers.matches(200.0, "apacheStatus", ImmutableMap.of())
            )
        ));
  }

  @Test
  public void testIncrementCounterWithImplied1() throws Exception {
    setup(parseConfigFile("test.yml"));
    assertThat(
        getPoints(1, "plainCounter"),
        contains(PointMatchers.matches(1L, MetricConstants.DELTA_PREFIX + "plainCounter",
            ImmutableMap.of())));
    // once the counter has been reported, the counter is reset because it is now treated as delta
    // counter. Hence we check that plainCounter has value 1 below.
    assertThat(
        getPoints(1, "plainCounter"),
        contains(PointMatchers.matches(1L, MetricConstants.DELTA_PREFIX + "plainCounter",
            ImmutableMap.of())));
  }

  @Test
  public void testHistogram() throws Exception {
    setup(parseConfigFile("test.yml"));
    String[] lines = new String[100];
    for (int i = 1; i < 101; i++) {
      lines[i - 1] = "histo " + i;
    }
    List<ReportPoint> points = getPoints(9, 2000, this::receiveLog, lines);
    tick(60000);
    assertThat(points,
        containsInAnyOrder(ImmutableList.of(
            PointMatchers.almostMatches(100.0, "myHisto.count", ImmutableMap.of()),
            PointMatchers.almostMatches(1.0, "myHisto.min", ImmutableMap.of()),
            PointMatchers.almostMatches(100.0, "myHisto.max", ImmutableMap.of()),
            PointMatchers.almostMatches(50.5, "myHisto.mean", ImmutableMap.of()),
            PointMatchers.almostMatches(50.5, "myHisto.median", ImmutableMap.of()),
            PointMatchers.almostMatches(75.5, "myHisto.p75", ImmutableMap.of()),
            PointMatchers.almostMatches(95.5, "myHisto.p95", ImmutableMap.of()),
            PointMatchers.almostMatches(99.5, "myHisto.p99", ImmutableMap.of()),
            PointMatchers.almostMatches(100, "myHisto.p999", ImmutableMap.of())
        ))
    );
  }

  @Test
  public void testProxyLogLine() throws Exception {
    setup(parseConfigFile("test.yml"));
    assertThat(
        getPoints(1, "WARNING: [2878] (SUMMARY): points attempted: 859432; blocked: 0"),
        contains(PointMatchers.matches(859432.0, "wavefrontPointsSent.2878", ImmutableMap.of()))
    );
  }

  @Test
  public void testWavefrontHistogram() throws Exception {
    setup(parseConfigFile("histos.yml"));
    List<String> logs = new ArrayList<>();
    logs.add("histo 100");
    logs.add("histo 100");
    logs.add("histo 100");
    for (int i = 0; i < 10; i++) logs.add("histo 1");
    for (int i = 0; i < 1000; i++) {
      logs.add("histo 75");
    }
    for (int i = 0; i < 100; i++) {
      logs.add("histo 90");
    }
    for (int i = 0; i < 10; i++) {
      logs.add("histo 99");
    }
    for (int i = 0; i < 10000; i++) {
      logs.add("histo 50");
    }

    ReportPoint reportPoint = getPoints(mockHistogramHandler, 1, 0, this::receiveLog, logs.toArray(new String[0])).get(0);
    assertThat(reportPoint.getValue(), instanceOf(Histogram.class));
    Histogram wavefrontHistogram = (Histogram) reportPoint.getValue();
    assertThat(wavefrontHistogram.getCounts().stream().reduce(Integer::sum).get(), equalTo(11123));
    assertThat(wavefrontHistogram.getBins().size(), lessThan(110));
    assertThat(wavefrontHistogram.getBins().get(0), equalTo(1.0));
    assertThat(wavefrontHistogram.getBins().get(wavefrontHistogram.getBins().size() - 1), equalTo(100.0));
  }

  @Test
  public void testWavefrontHistogramMultipleCentroids() throws Exception {
    setup(parseConfigFile("histos.yml"));
    String[] lines = new String[240];
    for (int i = 0; i < 240; i++) {
      lines[i] = "histo " + (i + 1);
    }
    List<ReportPoint> reportPoints = getPoints(mockHistogramHandler, 2, 500, this::receiveLog, lines);
    assertThat(reportPoints.size(), equalTo(2));
    assertThat(reportPoints, containsInAnyOrder(PointMatchers.histogramMatches(120, 7260.0),
        PointMatchers.histogramMatches(120, 21660.0)));
  }

  @Test(expected = ConfigurationException.class)
  public void testBadName() throws Exception {
    setup(parseConfigFile("badName.yml"));
  }
}
