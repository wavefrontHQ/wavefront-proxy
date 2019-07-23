package com.wavefront.integrations.metrics;

import com.google.common.collect.Lists;
import com.wavefront.common.Pair;
import com.wavefront.common.TaggedMetricName;
import com.wavefront.metrics.MetricTranslator;
import com.yammer.metrics.core.*;
import org.apache.commons.lang.StringUtils;
import org.apache.http.*;
import org.apache.http.impl.nio.bootstrap.HttpServer;
import org.apache.http.impl.nio.bootstrap.ServerBootstrap;
import org.apache.http.impl.nio.reactor.IOReactorConfig;
import org.apache.http.message.BasicHttpEntityEnclosingRequest;
import org.apache.http.nio.protocol.BasicAsyncRequestConsumer;
import org.apache.http.nio.protocol.HttpAsyncExchange;
import org.apache.http.nio.protocol.HttpAsyncRequestConsumer;
import org.apache.http.nio.protocol.HttpAsyncRequestHandler;
import org.apache.http.protocol.HttpContext;
import org.hamcrest.text.MatchesPattern;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.*;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.zip.GZIPInputStream;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.hamcrest.core.IsCollectionContaining.hasItem;

/**
 * @author Mike McMahon (mike@wavefront.com)
 */
public class WavefrontYammerHttpMetricsReporterTest {

  private MetricsRegistry metricsRegistry;
  private WavefrontYammerHttpMetricsReporter wavefrontYammerHttpMetricsReporter;
  private HttpServer metricsServer, histogramsServer;
  private LinkedBlockingQueue<String> inputMetrics, inputHistograms;
  private Long stubbedTime = 1485224035000L;

  private void innerSetUp(boolean prependGroupName, MetricTranslator metricTranslator,
                          boolean includeJvmMetrics, boolean clear) throws IOException {

    metricsRegistry = new MetricsRegistry();
    inputMetrics = new LinkedBlockingQueue<>();
    inputHistograms = new LinkedBlockingQueue<>();

    IOReactorConfig metricsIOreactor = IOReactorConfig.custom().
        setTcpNoDelay(true).
        setIoThreadCount(10).
        setSelectInterval(200).
        build();
    metricsServer = ServerBootstrap.bootstrap().
        setLocalAddress(InetAddress.getLocalHost()).
        setListenerPort(0).
        setServerInfo("Test/1.1").
        setIOReactorConfig(metricsIOreactor).
        registerHandler("*", new HttpAsyncRequestHandler<HttpRequest>() {
          @Override
          public HttpAsyncRequestConsumer<HttpRequest> processRequest(HttpRequest httpRequest, HttpContext httpContext) throws HttpException, IOException {
            return new BasicAsyncRequestConsumer();
          }

          @Override
          public void handle(HttpRequest httpRequest, HttpAsyncExchange httpAsyncExchange, HttpContext httpContext) throws HttpException, IOException {
            if (httpRequest instanceof BasicHttpEntityEnclosingRequest) {
              HttpEntity entity = ((BasicHttpEntityEnclosingRequest) httpRequest).getEntity();
              InputStream fromMetrics = entity.getContent();
              GZIPInputStream gzip = new GZIPInputStream(fromMetrics);

              int c = 0;
              while (c != -1) {
                ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
                while ((c = gzip.read()) != '\n' && c != -1) {
                  outputStream.write(c);
                }
                String metric = new String(outputStream.toByteArray(), StandardCharsets.UTF_8);
                if (StringUtils.isEmpty(metric))
                  continue;
                inputMetrics.offer(metric);
              }
            }
            // Send an OK response
            httpAsyncExchange.submitResponse();
          }
        }).
        create();
    metricsServer.start();

    IOReactorConfig histogramsIOReactor = IOReactorConfig.custom().
        setTcpNoDelay(true).
        setIoThreadCount(10).
        setSelectInterval(200).
        build();
    histogramsServer = ServerBootstrap.bootstrap().
        setLocalAddress(InetAddress.getLocalHost()).
        setListenerPort(0).
        setServerInfo("Test/1.1").
        setIOReactorConfig(histogramsIOReactor).
        registerHandler("*", new HttpAsyncRequestHandler<HttpRequest>() {
          @Override
          public HttpAsyncRequestConsumer<HttpRequest> processRequest(HttpRequest httpRequest, HttpContext httpContext) throws HttpException, IOException {
            return new BasicAsyncRequestConsumer();
          }

          @Override
          public void handle(HttpRequest httpRequest, HttpAsyncExchange httpAsyncExchange, HttpContext httpContext) throws HttpException, IOException {
            if (httpRequest instanceof BasicHttpEntityEnclosingRequest) {
              HttpEntity entity = ((BasicHttpEntityEnclosingRequest) httpRequest).getEntity();
              InputStream fromMetrics = new BufferedInputStream(entity.getContent());
              GZIPInputStream gzip = new GZIPInputStream(fromMetrics);

              int c = 0;
              while (c != -1) {
                ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
                while ((c = gzip.read()) != '\n' && c != -1) {
                  outputStream.write(c);
                }
                String histogram = new String(outputStream.toByteArray(), StandardCharsets.UTF_8);
                if (StringUtils.isEmpty(histogram))
                  continue;
                inputHistograms.offer(histogram);
              }
            }
            // Send an OK response
            httpAsyncExchange.submitResponse();
          }
        }).
        create();
    histogramsServer.start();

    try {
      // Allow time for the Async HTTP Servers to bind
      Thread.sleep(50);
    } catch (InterruptedException ex) {
      throw new RuntimeException("Interrupted trying to sleep.", ex);
    }

    wavefrontYammerHttpMetricsReporter = new WavefrontYammerHttpMetricsReporter.Builder().
        withName("test-http").
        withMetricsRegistry(metricsRegistry).
        withHost("http://" + InetAddress.getLocalHost().getHostAddress()).
        withPorts(
            ((InetSocketAddress) metricsServer.getEndpoint().getAddress()).getPort(),
            ((InetSocketAddress) histogramsServer.getEndpoint().getAddress()).getPort()).
        withTimeSupplier(() -> stubbedTime).
        withMetricTranslator(metricTranslator).
        withPrependedGroupNames(prependGroupName).
        clearHistogramsAndTimers(clear).
        includeJvmMetrics(includeJvmMetrics).
        withMaxConnectionsPerRoute(10).
        build();
  }

  @Before
  public void setUp() throws Exception {
    innerSetUp(false, null, false, false);
  }

  @After
  public void tearDown() throws IOException, InterruptedException{
    this.wavefrontYammerHttpMetricsReporter.shutdown(1, TimeUnit.MILLISECONDS);
    this.metricsServer.shutdown(1, TimeUnit.MILLISECONDS);
    this.histogramsServer.shutdown(1, TimeUnit.MILLISECONDS);
    inputMetrics.clear();
    inputHistograms.clear();
  }

  @Test()
  public void testJvmMetrics() throws Exception {
    innerSetUp(true, null, true, false);
    runReporter();
    List<String> metrics = processFromAsyncHttp(inputMetrics);
    assertThat(metrics, hasSize(wavefrontYammerHttpMetricsReporter.getMetricsGeneratedLastPass()));
    assertThat(metrics, not(hasItem(MatchesPattern.matchesPattern("\".* .*\".*"))));
    assertThat(metrics, hasItem(startsWith("\"jvm.memory.heapCommitted\"")));
    assertThat(metrics, hasItem(startsWith("\"jvm.fd_usage\"")));
    assertThat(metrics, hasItem(startsWith("\"jvm.buffers.mapped.totalCapacity\"")));
    assertThat(metrics, hasItem(startsWith("\"jvm.buffers.direct.totalCapacity\"")));
    assertThat(metrics, hasItem(startsWith("\"jvm.thread-states.runnable\"")));
  }

  @Test(timeout = 5000)
  public void testPlainCounter() throws Exception {
    Counter counter = metricsRegistry.newCounter(WavefrontYammerMetricsReporterTest.class, "mycount");
    counter.inc();
    counter.inc();
    runReporter();
    List<String> metrics = processFromAsyncHttp(inputMetrics);
    assertThat(metrics, hasSize(wavefrontYammerHttpMetricsReporter.getMetricsGeneratedLastPass()));
    assertThat(metrics, contains(equalTo("\"mycount\" 2.0 1485224035")));
  }

  @Test(timeout = 5000)
  public void testTransformer() throws Exception {
    innerSetUp(false, pair -> Pair.of(new TaggedMetricName(
        pair._1.getGroup(), pair._1.getName(), "tagA", "valueA"), pair._2), false, false);
    TaggedMetricName taggedMetricName = new TaggedMetricName("group", "mycounter",
        "tag1", "value1", "tag2", "value2");
    Counter counter = metricsRegistry.newCounter(taggedMetricName);
    counter.inc();
    counter.inc();
    runReporter();
    List<String> metrics = processFromAsyncHttp(inputMetrics);
    assertThat(metrics, hasSize(wavefrontYammerHttpMetricsReporter.getMetricsGeneratedLastPass()));
    assertThat(
        metrics,
        contains(equalTo("\"mycounter\" 2.0 1485224035 tagA=\"valueA\"")));
  }

  @Test(timeout = 5000)
  public void testTaggedCounter() throws Exception {
    TaggedMetricName taggedMetricName = new TaggedMetricName("group", "mycounter",
        "tag1", "value1", "tag2", "value2");
    Counter counter = metricsRegistry.newCounter(taggedMetricName);
    counter.inc();
    counter.inc();
    runReporter();
    List<String> metrics = processFromAsyncHttp(inputMetrics);
    assertThat(metrics, hasSize(wavefrontYammerHttpMetricsReporter.getMetricsGeneratedLastPass()));
    assertThat(
        metrics,
        contains(equalTo("\"mycounter\" 2.0 1485224035 tag1=\"value1\" tag2=\"value2\"")));
  }

  @Test(timeout = 5000)
  public void testPlainHistogramWithClear() throws Exception {
    innerSetUp(false, null, false, true /* clear */);
    Histogram histogram = metricsRegistry.newHistogram(WavefrontYammerMetricsReporterTest.class, "myhisto");
    histogram.update(1);
    histogram.update(10);
    runReporter();
    List<String> metrics = processFromAsyncHttp(inputMetrics);
    assertThat(metrics, hasSize(11));
    assertThat(metrics, containsInAnyOrder(
        equalTo("\"myhisto.count\" 2.0 1485224035"),
        equalTo("\"myhisto.min\" 1.0 1485224035"),
        equalTo("\"myhisto.max\" 10.0 1485224035"),
        equalTo("\"myhisto.mean\" 5.5 1485224035"),
        equalTo("\"myhisto.sum\" 11.0 1485224035"),
        startsWith("\"myhisto.stddev\""),
        equalTo("\"myhisto.median\" 5.5 1485224035"),
        equalTo("\"myhisto.p75\" 10.0 1485224035"),
        equalTo("\"myhisto.p95\" 10.0 1485224035"),
        equalTo("\"myhisto.p99\" 10.0 1485224035"),
        equalTo("\"myhisto.p999\" 10.0 1485224035")
    ));
    // Second run should clear data.
    runReporter();
    metrics = processFromAsyncHttp(inputMetrics);;
    assertThat(metrics, hasItem("\"myhisto.count\" 0.0 1485224035"));
  }

  @Test(timeout = 5000)
  public void testPlainHistogramWithoutClear() throws Exception {
    innerSetUp(false, null, false, false /* clear */);
    Histogram histogram = metricsRegistry.newHistogram(WavefrontYammerMetricsReporterTest.class, "myhisto");
    histogram.update(1);
    histogram.update(10);
    runReporter();
    List<String> metrics = processFromAsyncHttp(inputMetrics);
    assertThat(metrics, hasSize(11));
    assertThat(metrics, containsInAnyOrder(
        equalTo("\"myhisto.count\" 2.0 1485224035"),
        equalTo("\"myhisto.min\" 1.0 1485224035"),
        equalTo("\"myhisto.max\" 10.0 1485224035"),
        equalTo("\"myhisto.mean\" 5.5 1485224035"),
        equalTo("\"myhisto.sum\" 11.0 1485224035"),
        startsWith("\"myhisto.stddev\""),
        equalTo("\"myhisto.median\" 5.5 1485224035"),
        equalTo("\"myhisto.p75\" 10.0 1485224035"),
        equalTo("\"myhisto.p95\" 10.0 1485224035"),
        equalTo("\"myhisto.p99\" 10.0 1485224035"),
        equalTo("\"myhisto.p999\" 10.0 1485224035")
    ));
    // Second run should be the same.
    runReporter();
    metrics = processFromAsyncHttp(inputMetrics);
    assertThat(metrics, hasSize(11));
    assertThat(metrics, containsInAnyOrder(
        equalTo("\"myhisto.count\" 2.0 1485224035"),
        equalTo("\"myhisto.min\" 1.0 1485224035"),
        equalTo("\"myhisto.max\" 10.0 1485224035"),
        equalTo("\"myhisto.mean\" 5.5 1485224035"),
        equalTo("\"myhisto.sum\" 11.0 1485224035"),
        startsWith("\"myhisto.stddev\""),
        equalTo("\"myhisto.median\" 5.5 1485224035"),
        equalTo("\"myhisto.p75\" 10.0 1485224035"),
        equalTo("\"myhisto.p95\" 10.0 1485224035"),
        equalTo("\"myhisto.p99\" 10.0 1485224035"),
        equalTo("\"myhisto.p999\" 10.0 1485224035")
    ));
  }

  @Test(timeout = 5000)
  public void testWavefrontHistogram() throws Exception {
    AtomicLong clock = new AtomicLong(System.currentTimeMillis());
    long timeBin = (clock.get() / 60000 * 60);
    WavefrontHistogram wavefrontHistogram = WavefrontHistogram.get(metricsRegistry, new TaggedMetricName(
        "group", "myhisto", "tag1", "value1", "tag2", "value2"), clock::get);
    for (int i = 0; i < 101; i++) {
      wavefrontHistogram.update(i);
    }

    // Advance the clock by 1 min ...
    clock.addAndGet(60000L + 1);

    runReporter();
    List<String> histos = processFromAsyncHttp(inputHistograms);
    assertThat(histos, hasSize(wavefrontYammerHttpMetricsReporter.getMetricsGeneratedLastPass()));
    assertThat(histos, contains(equalTo(
        "!M " + timeBin + " #1 0.0 #1 1.0 #1 2.0 #1 3.0 #1 4.0 #1 5.0 #1 6.0 #1 7.0 #1 8.0 #1 9.0 #1 10.0 #1 11.0 #1 12.0 #1 13.0 #1 14.0 #1 15.0 #1 16.0 #1 17.0 #1 18.0 #1 19.0 #1 20.0 #1 21.0 #1 22.0 #1 23.0 #1 24.0 #1 25.0 #1 26.0 #1 27.0 #1 28.0 #1 29.0 #1 30.0 #1 31.0 #1 32.0 #1 33.0 #1 34.0 #1 35.0 #1 36.0 #1 37.0 #1 38.0 #1 39.0 #1 40.0 #1 41.0 #1 42.0 #1 43.0 #1 44.0 #1 45.0 #1 46.0 #1 47.0 #1 48.0 #1 49.0 #1 50.0 #1 51.0 #1 52.0 #1 53.0 #1 54.0 #1 55.0 #1 56.0 #1 57.0 #1 58.0 #1 59.0 #1 60.0 #1 61.0 #1 62.0 #1 63.0 #1 64.0 #1 65.0 #1 66.0 #1 67.0 #1 68.0 #1 69.0 #1 70.0 #1 71.0 #1 72.0 #1 73.0 #1 74.0 #1 75.0 #1 76.0 #1 77.0 #1 78.0 #1 79.0 #1 80.0 #1 81.0 #1 82.0 #1 83.0 #1 84.0 #1 85.0 #1 86.0 #1 87.0 #1 88.0 #1 89.0 #1 90.0 #1 91.0 #1 92.0 #1 93.0 #1 94.0 #1 95.0 #1 96.0 #1 97.0 #1 98.0 #1 99.0 #1 100.0 \"myhisto\" tag1=\"value1\" tag2=\"value2\"")));
  }

  @Test(timeout = 5000)
  public void testPlainMeter() throws Exception {
    Meter meter = metricsRegistry.newMeter(WavefrontYammerMetricsReporterTest.class, "mymeter", "requests",
        TimeUnit.SECONDS);
    meter.mark(42);
    runReporter();
    List<String> metrics = processFromAsyncHttp(inputMetrics);
    assertThat(metrics, containsInAnyOrder(
        equalTo("\"mymeter.count\" 42.0 1485224035"),
        startsWith("\"mymeter.mean\""),
        startsWith("\"mymeter.m1\""),
        startsWith("\"mymeter.m5\""),
        startsWith("\"mymeter.m15\"")
    ));
  }

  @Test(timeout = 5000)
  public void testPlainGauge() throws Exception {
    Gauge gauge = metricsRegistry.newGauge(
        WavefrontYammerMetricsReporterTest.class, "mygauge", new Gauge<Double>() {
          @Override
          public Double value() {
            return 13.0;
          }
        });
    runReporter();
    List<String> metrics = processFromAsyncHttp(inputMetrics);
    assertThat(metrics, hasSize(wavefrontYammerHttpMetricsReporter.getMetricsGeneratedLastPass()));
    assertThat(metrics, contains(equalTo("\"mygauge\" 13.0 1485224035")));
  }

  @Test(timeout = 5000)
  public void testTimerWithClear() throws Exception {
    innerSetUp(false, null, false, true /* clear */);
    Timer timer = metricsRegistry.newTimer(new TaggedMetricName("", "mytimer", "foo", "bar"),
        TimeUnit.SECONDS, TimeUnit.SECONDS);
    timer.time().stop();
    runReporter();
    List<String> metrics = processFromAsyncHttp(inputMetrics);
    assertThat(metrics, hasSize(15));
    assertThat(metrics, containsInAnyOrder(
        equalTo("\"mytimer.rate.count\" 1.0 1485224035 foo=\"bar\""),
        startsWith("\"mytimer.duration.min\""),
        startsWith("\"mytimer.duration.max\""),
        startsWith("\"mytimer.duration.mean\""),
        startsWith("\"mytimer.duration.sum\""),
        startsWith("\"mytimer.duration.stddev\""),
        startsWith("\"mytimer.duration.median\""),
        startsWith("\"mytimer.duration.p75\""),
        startsWith("\"mytimer.duration.p95\""),
        startsWith("\"mytimer.duration.p99\""),
        startsWith("\"mytimer.duration.p999\""),
        startsWith("\"mytimer.rate.m1\""),
        startsWith("\"mytimer.rate.m5\""),
        startsWith("\"mytimer.rate.m15\""),
        startsWith("\"mytimer.rate.mean\"")
    ));

    runReporter();
    metrics = processFromAsyncHttp(inputMetrics);
    assertThat(metrics, hasSize(15));
    assertThat(metrics, hasItem("\"mytimer.rate.count\" 0.0 1485224035 foo=\"bar\""));
  }

  @Test()
  public void testPlainTimerWithoutClear() throws Exception {
    innerSetUp(false, null, false, false /* clear */);
    Timer timer = metricsRegistry.newTimer(WavefrontYammerMetricsReporterTest.class, "mytimer");
    timer.time().stop();
    runReporter();
    List<String> metrics = processFromAsyncHttp(inputMetrics);
    assertThat(metrics, hasSize(15));
    assertThat(metrics, containsInAnyOrder(
        equalTo("\"mytimer.rate.count\" 1.0 1485224035"),
        startsWith("\"mytimer.duration.min\""),
        startsWith("\"mytimer.duration.max\""),
        startsWith("\"mytimer.duration.mean\""),
        startsWith("\"mytimer.duration.sum\""),
        startsWith("\"mytimer.duration.stddev\""),
        startsWith("\"mytimer.duration.median\""),
        startsWith("\"mytimer.duration.p75\""),
        startsWith("\"mytimer.duration.p95\""),
        startsWith("\"mytimer.duration.p99\""),
        startsWith("\"mytimer.duration.p999\""),
        startsWith("\"mytimer.rate.m1\""),
        startsWith("\"mytimer.rate.m5\""),
        startsWith("\"mytimer.rate.m15\""),
        startsWith("\"mytimer.rate.mean\"")
    ));

    // No changes.
    runReporter();
    metrics = processFromAsyncHttp(inputMetrics);
    assertThat(metrics, hasSize(15));
    assertThat(metrics, containsInAnyOrder(
        equalTo("\"mytimer.rate.count\" 1.0 1485224035"),
        startsWith("\"mytimer.duration.min\""),
        startsWith("\"mytimer.duration.max\""),
        startsWith("\"mytimer.duration.mean\""),
        startsWith("\"mytimer.duration.sum\""),
        startsWith("\"mytimer.duration.stddev\""),
        startsWith("\"mytimer.duration.median\""),
        startsWith("\"mytimer.duration.p75\""),
        startsWith("\"mytimer.duration.p95\""),
        startsWith("\"mytimer.duration.p99\""),
        startsWith("\"mytimer.duration.p999\""),
        startsWith("\"mytimer.rate.m1\""),
        startsWith("\"mytimer.rate.m5\""),
        startsWith("\"mytimer.rate.m15\""),
        startsWith("\"mytimer.rate.mean\"")
    ));
  }

  @Test(timeout = 5000)
  public void testPrependGroupName() throws Exception {
    innerSetUp(true, null, false, false);

    // Counter
    TaggedMetricName taggedMetricName = new TaggedMetricName("group", "mycounter",
        "tag1", "value1", "tag2", "value2");
    Counter counter = metricsRegistry.newCounter(taggedMetricName);
    counter.inc();
    counter.inc();

    AtomicLong clock = new AtomicLong(System.currentTimeMillis());
    long timeBin = (clock.get() / 60000 * 60);
    // Wavefront Histo
    WavefrontHistogram wavefrontHistogram = WavefrontHistogram.get(metricsRegistry, new TaggedMetricName(
        "group3", "myhisto", "tag1", "value1", "tag2", "value2"), clock::get);
    for (int i = 0; i < 101; i++) {
      wavefrontHistogram.update(i);
    }

    // Exploded Histo
    Histogram histogram = metricsRegistry.newHistogram(new MetricName("group2", "", "myhisto"), false);
    histogram.update(1);
    histogram.update(10);

    // Advance the clock by 1 min ...
    clock.addAndGet(60000L + 1);

    runReporter();
    List<String> metrics = processFromAsyncHttp(inputMetrics);
    assertThat(metrics, hasSize(12));
    assertThat(metrics,
        containsInAnyOrder(
            equalTo("\"group.mycounter\" 2.0 1485224035 tag1=\"value1\" tag2=\"value2\""),
            equalTo("\"group2.myhisto.count\" 2.0 1485224035"),
            equalTo("\"group2.myhisto.min\" 1.0 1485224035"),
            equalTo("\"group2.myhisto.max\" 10.0 1485224035"),
            equalTo("\"group2.myhisto.mean\" 5.5 1485224035"),
            equalTo("\"group2.myhisto.sum\" 11.0 1485224035"),
            startsWith("\"group2.myhisto.stddev\""),
            equalTo("\"group2.myhisto.median\" 5.5 1485224035"),
            equalTo("\"group2.myhisto.p75\" 10.0 1485224035"),
            equalTo("\"group2.myhisto.p95\" 10.0 1485224035"),
            equalTo("\"group2.myhisto.p99\" 10.0 1485224035"),
            equalTo("\"group2.myhisto.p999\" 10.0 1485224035")));

    List<String> histos = processFromAsyncHttp(inputHistograms);
    assertThat(histos, hasSize(1));
    assertThat(
        histos,
        contains(equalTo("!M " + timeBin + " #1 0.0 #1 1.0 #1 2.0 #1 3.0 #1 4.0 #1 5.0 #1 6.0 #1 7.0 #1 8.0 #1 9.0 #1 10.0 #1 11.0 #1 12.0 #1 13.0 #1 14.0 #1 15.0 #1 16.0 #1 17.0 #1 18.0 #1 19.0 #1 20.0 #1 21.0 #1 22.0 #1 23.0 #1 24.0 #1 25.0 #1 26.0 #1 27.0 #1 28.0 #1 29.0 #1 30.0 #1 31.0 #1 32.0 #1 33.0 #1 34.0 #1 35.0 #1 36.0 #1 37.0 #1 38.0 #1 39.0 #1 40.0 #1 41.0 #1 42.0 #1 43.0 #1 44.0 #1 45.0 #1 46.0 #1 47.0 #1 48.0 #1 49.0 #1 50.0 #1 51.0 #1 52.0 #1 53.0 #1 54.0 #1 55.0 #1 56.0 #1 57.0 #1 58.0 #1 59.0 #1 60.0 #1 61.0 #1 62.0 #1 63.0 #1 64.0 #1 65.0 #1 66.0 #1 67.0 #1 68.0 #1 69.0 #1 70.0 #1 71.0 #1 72.0 #1 73.0 #1 74.0 #1 75.0 #1 76.0 #1 77.0 #1 78.0 #1 79.0 #1 80.0 #1 81.0 #1 82.0 #1 83.0 #1 84.0 #1 85.0 #1 86.0 #1 87.0 #1 88.0 #1 89.0 #1 90.0 #1 91.0 #1 92.0 #1 93.0 #1 94.0 #1 95.0 #1 96.0 #1 97.0 #1 98.0 #1 99.0 #1 100.0 \"group3.myhisto\" tag1=\"value1\" tag2=\"value2\"")));
  }

  private List<String> processFromAsyncHttp(LinkedBlockingQueue<String> pollable) {
    List<String> found = Lists.newArrayList();
    String polled;
    try {
      while ((polled = pollable.poll(500, TimeUnit.MILLISECONDS)) != null) {
        found.add(polled);
      }
    } catch (InterruptedException ex) {
      throw new RuntimeException("Interrupted while polling the async endpoint");
    }

    return found;
  }

  private void runReporter() throws InterruptedException {
    inputMetrics.clear();
    wavefrontYammerHttpMetricsReporter.run();
  }
}
