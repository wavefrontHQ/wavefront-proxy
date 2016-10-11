package com.wavefront.metrics;

import com.yammer.metrics.core.Histogram;
import com.yammer.metrics.core.MetricName;
import com.yammer.metrics.core.MetricsRegistry;
import com.yammer.metrics.core.WavefrontHistogram;

import org.junit.Before;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicLong;

import static com.google.common.truth.Truth.assertThat;

/**
 * Basic unit tests around {@link JsonMetricsGenerator}
 *
 * @author Tim Schmidt (tim@wavefront.com).
 */
public class JsonMetricsGeneratorTest {
  private AtomicLong time = new AtomicLong(0);
  private MetricsRegistry testRegistry;

  @Before
  public void setup() {
    testRegistry = new MetricsRegistry();
  }

  private String generate(boolean includeVMMetrics,
                          boolean includeBuildMetrics,
                          boolean clearMetrics) throws IOException {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    JsonMetricsGenerator.generateJsonMetrics(baos, testRegistry, includeVMMetrics, includeBuildMetrics, clearMetrics);
    return new String(baos.toByteArray());
  }

  @Test
  public void testYammerHistogram() throws IOException {
    Histogram wh = testRegistry.newHistogram(new MetricName("test", "", "metric"), false);

    wh.update(10);
    wh.update(100);
    wh.update(1000);

    String json = generate(false, false, false);

    assertThat(json).isEqualTo("{\"test.metric\":{\"count\":3,\"min\":10.0,\"max\":1000.0,\"mean\":370.0,\"median\":100.0,\"p75\":1000.0,\"p95\":1000.0,\"p99\":1000.0,\"p999\":1000.0}}");
  }

  @Test
  public void testWavefrontHistogram() throws IOException {
    Histogram wh = WavefrontHistogram.get(testRegistry, new MetricName("test", "", "metric"), time::get);

    wh.update(10);
    wh.update(100);
    wh.update(1000);

    String json = generate(false, false, false);

    assertThat(json).isEqualTo("{\"test.metric\":{\"bins\":[{\"count\":3,\"startMillis\":0,\"durationMillis\":60000,\"means\":[10.0,100.0,1000.0],\"counts\":[1,1,1]}]}}");
  }

  @Test
  public void testWavefrontHistogramClear() throws IOException {
    Histogram wh = WavefrontHistogram.get(testRegistry, new MetricName("test", "", "metric"), time::get);
    wh.update(10);

    generate(false, false, true);

    wh.update(100);

    String json = generate(false, false, true);

    assertThat(json).isEqualTo("{\"test.metric\":{\"bins\":[{\"count\":1,\"startMillis\":0,\"durationMillis\":60000,\"means\":[100.0],\"counts\":[1]}]}}");
  }

  @Test
  public void testWavefrontHistogramNoClear() throws IOException {
    Histogram wh = WavefrontHistogram.get(testRegistry, new MetricName("test", "", "metric"), time::get);

    wh.update(10);
    generate(false, false, false);
    wh.update(100);
    String json = generate(false, false, true);

    assertThat(json).isEqualTo("{\"test.metric\":{\"bins\":[{\"count\":2,\"startMillis\":0,\"durationMillis\":60000,\"means\":[10.0,100.0],\"counts\":[1,1]}]}}");
  }

  @Test
  public void testWavefrontHistogramSpanMultipleMinutes() throws IOException {
    Histogram wh = WavefrontHistogram.get(testRegistry, new MetricName("test", "", "metric"), time::get);

    wh.update(10);
    wh.update(100);

    time.set(61 * 1000);
    wh.update(1000);

    String json = generate(false, false, false);

    assertThat(json).isEqualTo("{\"test.metric\":{\"bins\":[{\"count\":2,\"startMillis\":0,\"durationMillis\":60000,\"means\":[10.0,100.0],\"counts\":[1,1]},{\"count\":1,\"startMillis\":60000,\"durationMillis\":60000,\"means\":[1000.0],\"counts\":[1]}]}}");
  }

  @Test
  public void testWavefrontHistogramPrunesOldBins() throws IOException {
    Histogram wh = WavefrontHistogram.get(testRegistry, new MetricName("test", "", "metric"), time::get);
    //1
    wh.update(10);
    //2
    time.set(61 * 1000);
    wh.update(100);
    //3
    time.set(121 * 1000);
    wh.update(1000);
    //4
    time.set(181 * 1000);
    wh.update(10000);
    //5
    time.set(241 * 1000);
    wh.update(100000);
    //6
    time.set(301 * 1000);
    wh.update(100001);
    //7
    time.set(361 * 1000);
    wh.update(100011);
    //8
    time.set(421 * 1000);
    wh.update(100111);
    //9
    time.set(481 * 1000);
    wh.update(101111);
    //10
    time.set(541 * 1000);
    wh.update(111111);
    //11
    time.set(601 * 1000);
    wh.update(111112);

    String json = generate(false, false, false);

    assertThat(json).isEqualTo("{\"test.metric\":{\"bins\":[{\"count\":1,\"startMillis\":60000,\"durationMillis\":60000,\"means\":[100.0],\"counts\":[1]},{\"count\":1,\"startMillis\":120000,\"durationMillis\":60000,\"means\":[1000.0],\"counts\":[1]},{\"count\":1,\"startMillis\":180000,\"durationMillis\":60000,\"means\":[10000.0],\"counts\":[1]},{\"count\":1,\"startMillis\":240000,\"durationMillis\":60000,\"means\":[100000.0],\"counts\":[1]},{\"count\":1,\"startMillis\":300000,\"durationMillis\":60000,\"means\":[100001.0],\"counts\":[1]},{\"count\":1,\"startMillis\":360000,\"durationMillis\":60000,\"means\":[100011.0],\"counts\":[1]},{\"count\":1,\"startMillis\":420000,\"durationMillis\":60000,\"means\":[100111.0],\"counts\":[1]},{\"count\":1,\"startMillis\":480000,\"durationMillis\":60000,\"means\":[101111.0],\"counts\":[1]},{\"count\":1,\"startMillis\":540000,\"durationMillis\":60000,\"means\":[111111.0],\"counts\":[1]},{\"count\":1,\"startMillis\":600000,\"durationMillis\":60000,\"means\":[111112.0],\"counts\":[1]}]}}");
  }
}