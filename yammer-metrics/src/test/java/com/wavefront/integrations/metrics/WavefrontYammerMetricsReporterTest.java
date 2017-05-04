package com.wavefront.integrations.metrics;

import com.google.common.collect.Lists;

import com.wavefront.common.TaggedMetricName;
import com.yammer.metrics.core.Counter;
import com.yammer.metrics.core.Gauge;
import com.yammer.metrics.core.Histogram;
import com.yammer.metrics.core.Meter;
import com.yammer.metrics.core.MetricName;
import com.yammer.metrics.core.MetricsRegistry;
import com.yammer.metrics.core.Timer;
import com.yammer.metrics.core.WavefrontHistogram;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.BufferedInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.startsWith;
import static org.hamcrest.core.IsCollectionContaining.hasItem;

/**
 * @author Mori Bellamy (mori@wavefront.com)
 */
public class WavefrontYammerMetricsReporterTest {

  private MetricsRegistry metricsRegistry;
  private WavefrontYammerMetricsReporter wavefrontYammerMetricsReporter;
  private Socket metricsSocket, histogramsSocket;
  private ServerSocket metricsServer, histogramsServer;
  private BufferedInputStream fromMetrics, fromHistograms;
  private Long stubbedTime = 1485224035000L;

  private void innerSetUp(boolean prependGroupName, Function<MetricName, MetricName> transformer,
                          boolean includeJvmMetrics)
      throws Exception {
    metricsRegistry = new MetricsRegistry();
    metricsServer = new ServerSocket(0);
    histogramsServer = new ServerSocket(0);
    wavefrontYammerMetricsReporter = new WavefrontYammerMetricsReporter(
        metricsRegistry, "test", "localhost", metricsServer.getLocalPort(), histogramsServer.getLocalPort(),
        () -> stubbedTime, prependGroupName, transformer, includeJvmMetrics);
    metricsSocket = metricsServer.accept();
    histogramsSocket = histogramsServer.accept();
    fromMetrics = new BufferedInputStream(metricsSocket.getInputStream());
    fromHistograms = new BufferedInputStream(histogramsSocket.getInputStream());
  }

  @Before
  public void setUp() throws Exception {
    innerSetUp(false, null, false);
  }

  @After
  public void tearDown() throws IOException {
    metricsSocket.close();
    histogramsSocket.close();
    metricsServer.close();
    histogramsServer.close();
  }

  List<String> receiveFromSocket(int numMetrics, InputStream stream) throws IOException {
    List<String> received = Lists.newArrayListWithCapacity(numMetrics);
    // Read N metrics, which are produced from the prepared registry. If N is too high, we will time out. If
    // N is too low, the asserts later should fail.
    for (int i = 0; i < numMetrics; i++) {
      ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
      int c;
      while ((c = stream.read()) != '\n') {
        byteArrayOutputStream.write(c);
      }
      received.add(new String(byteArrayOutputStream.toByteArray(), "UTF-8"));
    }
    return received;
  }

  @Test(timeout = 1000)
  public void testJvmMetrics() throws Exception {
    innerSetUp(true, null, true);
    wavefrontYammerMetricsReporter.run();
    List<String> metrics = receiveFromSocket(
        wavefrontYammerMetricsReporter.getMetricsGeneratedLastPass(), fromMetrics);
    assertThat(metrics, hasItem(startsWith("\"jvm.memory.heapCommitted\"")));
    assertThat(metrics, hasItem(startsWith("\"jvm.fd_usage\"")));
    assertThat(metrics, hasItem(startsWith("\"jvm.buffers.mapped.totalCapacity\"")));
    assertThat(metrics, hasItem(startsWith("\"jvm.buffers.direct.totalCapacity\"")));
    assertThat(metrics, hasItem(startsWith("\"jvm.thread-states.runnable\"")));
  }


  @Test(timeout = 1000)
  public void testPlainCounter() throws Exception {
    Counter counter = metricsRegistry.newCounter(WavefrontYammerMetricsReporterTest.class, "mycount");
    counter.inc();
    counter.inc();
    wavefrontYammerMetricsReporter.run();
    assertThat(receiveFromSocket(1, fromMetrics), contains(equalTo("\"mycount\" 2.0")));
  }

  @Test(timeout = 1000)
  public void testTransformer() throws Exception {
    innerSetUp(false, metricName -> new TaggedMetricName(
        metricName.getGroup(), metricName.getName(), "tagA", "valueA"), false);
    TaggedMetricName taggedMetricName = new TaggedMetricName("group", "mycounter",
        "tag1", "value1", "tag2", "value2");
    Counter counter = metricsRegistry.newCounter(taggedMetricName);
    counter.inc();
    counter.inc();
    wavefrontYammerMetricsReporter.run();
    assertThat(
        receiveFromSocket(1, fromMetrics),
        contains(equalTo("\"mycounter\" 2.0 tagA=\"valueA\"")));
  }

  @Test(timeout = 1000)
  public void testTaggedCounter() throws Exception {
    TaggedMetricName taggedMetricName = new TaggedMetricName("group", "mycounter",
        "tag1", "value1", "tag2", "value2");
    Counter counter = metricsRegistry.newCounter(taggedMetricName);
    counter.inc();
    counter.inc();
    wavefrontYammerMetricsReporter.run();
    assertThat(
        receiveFromSocket(1, fromMetrics),
        contains(equalTo("\"mycounter\" 2.0 tag1=\"value1\" tag2=\"value2\"")));
  }

  @Test(timeout = 1000)
  public void testPlainHistogram() throws Exception {
    Histogram histogram = metricsRegistry.newHistogram(WavefrontYammerMetricsReporterTest.class, "myhisto");
    histogram.update(1);
    histogram.update(10);
    wavefrontYammerMetricsReporter.run();
    assertThat(receiveFromSocket(11, fromMetrics), containsInAnyOrder(
        equalTo("\"myhisto.count\" 2.0"),
        equalTo("\"myhisto.min\" 1.0"),
        equalTo("\"myhisto.max\" 10.0"),
        equalTo("\"myhisto.mean\" 5.5"),
        equalTo("\"myhisto.sum\" 11.0"),
        startsWith("\"myhisto.stddev\""),
        equalTo("\"myhisto.median\" 5.5"),
        equalTo("\"myhisto.p75\" 10.0"),
        equalTo("\"myhisto.p95\" 10.0"),
        equalTo("\"myhisto.p99\" 10.0"),
        equalTo("\"myhisto.p999\" 10.0")
    ));
  }

  @Test(timeout = 1000)
  public void testWavefrontHistogram() throws Exception {
    WavefrontHistogram wavefrontHistogram = WavefrontHistogram.get(metricsRegistry, new TaggedMetricName(
        "group", "myhisto", "tag1", "value1", "tag2", "value2"));
    for (int i = 0; i < 101; i++) {
      wavefrontHistogram.update(i);
    }
    wavefrontYammerMetricsReporter.run();
    assertThat(receiveFromSocket(1, fromHistograms), contains(equalTo(
        "!M 1485224035 #101 50.0 \"myhisto\" tag1=\"value1\" tag2=\"value2\"")));
  }

  @Test(timeout = 1000)
  public void testPlainMeter() throws Exception {
    Meter meter = metricsRegistry.newMeter(WavefrontYammerMetricsReporterTest.class, "mymeter", "requests",
        TimeUnit.SECONDS);
    meter.mark(42);
    wavefrontYammerMetricsReporter.run();
    assertThat(receiveFromSocket(5, fromMetrics), containsInAnyOrder(
        equalTo("\"mymeter.count\" 42.0"),
        startsWith("\"mymeter.mean\""),
        startsWith("\"mymeter.m1\""),
        startsWith("\"mymeter.m5\""),
        startsWith("\"mymeter.m15\"")
    ));
  }

  @Test(timeout = 1000)
  public void testPlainGauge() throws Exception {
    Gauge gauge = metricsRegistry.newGauge(
        WavefrontYammerMetricsReporterTest.class, "mygauge", new Gauge<Double>() {
          @Override
          public Double value() {
            return 13.0;
          }
        });
    wavefrontYammerMetricsReporter.run();
    assertThat(receiveFromSocket(1, fromMetrics), contains(equalTo("\"mygauge\" 13.0")));
  }

  @Test(timeout = 1000)
  public void testPlainTimer() throws Exception {
    Timer timer = metricsRegistry.newTimer(WavefrontYammerMetricsReporterTest.class, "mytimer");
    timer.time().stop();
    wavefrontYammerMetricsReporter.run();
    assertThat(receiveFromSocket(15, fromMetrics), containsInAnyOrder(
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
        startsWith("\"mytimer.rate.count\""),
        startsWith("\"mytimer.rate.m1\""),
        startsWith("\"mytimer.rate.m5\""),
        startsWith("\"mytimer.rate.m15\""),
        startsWith("\"mytimer.rate.mean\"")
    ));
  }

  @Test(timeout = 1000)
  public void testPrependGroupName() throws Exception {
    innerSetUp(true, null, false);

    // Counter
    TaggedMetricName taggedMetricName = new TaggedMetricName("group", "mycounter",
        "tag1", "value1", "tag2", "value2");
    Counter counter = metricsRegistry.newCounter(taggedMetricName);
    counter.inc();
    counter.inc();

    // Wavefront Histo
    WavefrontHistogram wavefrontHistogram = WavefrontHistogram.get(metricsRegistry, new TaggedMetricName(
        "group3", "myhisto", "tag1", "value1", "tag2", "value2"));
    for (int i = 0; i < 101; i++) {
      wavefrontHistogram.update(i);
    }

    // Exploded Histo
    Histogram histogram = metricsRegistry.newHistogram(new MetricName("group2", "", "myhisto"), false);
    histogram.update(1);
    histogram.update(10);

    wavefrontYammerMetricsReporter.run();
    assertThat(
        receiveFromSocket(12, fromMetrics),
        containsInAnyOrder(
            equalTo("\"group.mycounter\" 2.0 tag1=\"value1\" tag2=\"value2\""),
            equalTo("\"group2.myhisto.count\" 2.0"),
            equalTo("\"group2.myhisto.min\" 1.0"),
            equalTo("\"group2.myhisto.max\" 10.0"),
            equalTo("\"group2.myhisto.mean\" 5.5"),
            equalTo("\"group2.myhisto.sum\" 11.0"),
            startsWith("\"group2.myhisto.stddev\""),
            equalTo("\"group2.myhisto.median\" 5.5"),
            equalTo("\"group2.myhisto.p75\" 10.0"),
            equalTo("\"group2.myhisto.p95\" 10.0"),
            equalTo("\"group2.myhisto.p99\" 10.0"),
            equalTo("\"group2.myhisto.p999\" 10.0")));

    assertThat(
        receiveFromSocket(1, fromHistograms),
        contains(equalTo("!M 1485224035 #101 50.0 \"group3.myhisto\" tag1=\"value1\" tag2=\"value2\"")));
  }

}
