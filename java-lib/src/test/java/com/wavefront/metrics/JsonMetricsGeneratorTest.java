package com.wavefront.metrics;

import com.google.common.collect.ImmutableList;

import com.wavefront.common.Pair;
import com.yammer.metrics.core.Counter;
import com.yammer.metrics.core.Histogram;
import com.yammer.metrics.core.MetricName;
import com.yammer.metrics.core.MetricsRegistry;
import com.yammer.metrics.core.WavefrontHistogram;

import org.codehaus.jackson.map.ObjectMapper;
import org.junit.Before;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Map;
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
  private final ObjectMapper objectMapper = new ObjectMapper();

  @Before
  public void setup() {
    testRegistry = new MetricsRegistry();
    time = new AtomicLong(0);
  }

  private String generate(boolean includeVMMetrics,
                          boolean includeBuildMetrics,
                          boolean clearMetrics,
                          MetricTranslator metricTranslator) throws IOException {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    JsonMetricsGenerator.generateJsonMetrics(baos, testRegistry, includeVMMetrics, includeBuildMetrics, clearMetrics,
        metricTranslator);
    return new String(baos.toByteArray());
  }

  /**
   * @param map   A raw map.
   * @param key   A key.
   * @param clazz See T.
   * @param <T>   The expected dynamic type of map.get(key)
   * @return map.get(key) if it exists and is the right type. Otherwise, fail the calling test.
   */
  private <T> T safeGet(Map map, String key, Class<T> clazz) {
    assertThat(map.containsKey(key)).isTrue();
    assertThat(map.get(key)).isInstanceOf(clazz);
    return clazz.cast(map.get(key));
  }

  @Test
  public void testJvmMetrics() throws IOException {
    String json = generate(true, false, false, null);
    Map top = objectMapper.readValue(json, Map.class);
    Map jvm = safeGet(top, "jvm", Map.class);

    Map memory = safeGet(jvm, "memory", Map.class);
    safeGet(memory, "totalInit", Double.class);
    safeGet(memory, "memory_pool_usages", Map.class);

    Map buffers = safeGet(jvm, "buffers", Map.class);
    safeGet(buffers, "direct", Map.class);
    safeGet(buffers, "mapped", Map.class);

    safeGet(jvm, "fd_usage", Double.class);
    safeGet(jvm, "current_time", Long.class);

    Map threadStates = safeGet(jvm, "thread-states", Map.class);
    safeGet(threadStates, "runnable", Double.class);

    Map garbageCollectors = safeGet(jvm, "garbage-collectors", Map.class);
    assertThat(threadStates).isNotEmpty();
    // Check that any GC has a "runs" entry.
    String key = (String) garbageCollectors.keySet().iterator().next();  // e.g. "PS MarkSweep"
    Map gcMap = safeGet(garbageCollectors, key, Map.class);
    safeGet(gcMap, "runs", Double.class);
    safeGet(gcMap, "time", Double.class);
  }

  @Test
  public void testTranslator() throws IOException {
    Counter counter = testRegistry.newCounter(new MetricName("test", "foo", "bar"));
    counter.inc();
    counter.inc();
    String json = generate(false, false, false, metricNameMetricPair -> {
      assertThat(metricNameMetricPair._1).isEquivalentAccordingToCompareTo(new MetricName("test", "foo", "bar"));
      assertThat(metricNameMetricPair._2).isInstanceOf(Counter.class);
      assertThat(((Counter)metricNameMetricPair._2).count()).isEqualTo(2);
      return new Pair<>(new MetricName("test", "baz", "qux"), metricNameMetricPair._2);
    });
    assertThat(json).isEqualTo("{\"test.qux\":2}");
    json = generate(false, false, false, metricNameMetricPair -> null);
    assertThat(json).isEqualTo("{}");
  }

  @Test
  public void testYammerHistogram() throws IOException {
    Histogram wh = testRegistry.newHistogram(new MetricName("test", "", "metric"), false);

    wh.update(10);
    wh.update(100);
    wh.update(1000);

    String json = generate(false, false, false, null);

    assertThat(json).isEqualTo("{\"test.metric\":{\"count\":3,\"min\":10.0,\"max\":1000.0,\"mean\":370.0,\"sum\":1110.0,\"stddev\":547.4486277268397,\"median\":100.0,\"p75\":1000.0,\"p95\":1000.0,\"p99\":1000.0,\"p999\":1000.0}}");
  }

  @Test
  public void testWavefrontHistogram() throws IOException {
    Histogram wh = WavefrontHistogram.get(testRegistry, new MetricName("test", "", "metric"), time::get);

    wh.update(10);
    wh.update(100);
    wh.update(1000);

    // Simulate the 1 minute has passed and we are ready to flush the histogram
    // (i.e. all the values prior to the current minute) over the wire...
    time.addAndGet(60001L);

    String json = generate(false, false, false, null);

    assertThat(json).isEqualTo("{\"test.metric\":{\"bins\":[{\"count\":3,\"startMillis\":0,\"durationMillis\":60000,\"means\":[10.0,100.0,1000.0],\"counts\":[1,1,1]}]}}");
  }

  @Test
  public void testWavefrontHistogramClear() throws IOException {
    Histogram wh = WavefrontHistogram.get(testRegistry, new MetricName("test", "", "metric"), time::get);
    wh.update(10);

    // Simulate the 1 minute has passed and we are ready to flush the histogram
    // (i.e. all the values prior to the current minute) over the wire...
    time.addAndGet(60001L);

    generate(false, false, true, null);

    wh.update(100);

    // Simulate the 1 minute has passed and we are ready to flush the histogram
    // (i.e. all the values prior to the current minute) over the wire...
    time.addAndGet(60001L);

    String json = generate(false, false, true, null);

    assertThat(json).isEqualTo("{\"test.metric\":{\"bins\":[{\"count\":1,\"startMillis\":60000,\"durationMillis\":60000,\"means\":[100.0],\"counts\":[1]}]}}");
  }

  @Test
  public void testWavefrontHistogramNoClear() throws IOException {
    Histogram wh = WavefrontHistogram.get(testRegistry, new MetricName("test", "", "metric"), time::get);

    wh.update(10);
    generate(false, false, false, null);
    wh.update(100);

    // Simulate the 1 minute has passed and we are ready to flush the histogram
    // (i.e. all the values prior to the current minute) over the wire...
    time.addAndGet(60001L);

    String json = generate(false, false, true, null);

    assertThat(json).isEqualTo("{\"test.metric\":{\"bins\":[{\"count\":2,\"startMillis\":0,\"durationMillis\":60000,\"means\":[10.0,100.0],\"counts\":[1,1]}]}}");
  }

  @Test
  public void testWavefrontHistogramSpanMultipleMinutes() throws IOException {
    Histogram wh = WavefrontHistogram.get(testRegistry, new MetricName("test", "", "metric"), time::get);

    wh.update(10);
    wh.update(100);

    // Simulate the clock advanced by 1 minute
    time.set(61 * 1000);
    wh.update(1000);

    // Simulate the clock advanced by 1 minute
    time.set(61 * 1000 * 2);
    String json = generate(false, false, false, null);

    assertThat(json).isEqualTo("{\"test.metric\":{\"bins\":[{\"count\":2,\"startMillis\":0,\"durationMillis\":60000,\"means\":[10.0,100.0],\"counts\":[1,1]},{\"count\":1,\"startMillis\":60000,\"durationMillis\":60000,\"means\":[1000.0],\"counts\":[1]}]}}");
  }

  @Test
  public void testWavefrontHistogramPrunesOldBins() throws IOException {
    // We no longer prune old bins
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

    // Simulate the 1 minute has passed and we are ready to flush the histogram
    // (i.e. all the values prior to the current minute) over the wire...
    time.addAndGet(60001L);

    String json = generate(false, false, false, null);
    assertThat(json).isEqualTo("{\"test.metric\":{\"bins\":[{\"count\":1,\"startMillis\":0,\"durationMillis\":60000,\"means\":[10.0],\"counts\":[1]},{\"count\":1,\"startMillis\":60000,\"durationMillis\":60000,\"means\":[100.0],\"counts\":[1]},{\"count\":1,\"startMillis\":120000,\"durationMillis\":60000,\"means\":[1000.0],\"counts\":[1]},{\"count\":1,\"startMillis\":180000,\"durationMillis\":60000,\"means\":[10000.0],\"counts\":[1]},{\"count\":1,\"startMillis\":240000,\"durationMillis\":60000,\"means\":[100000.0],\"counts\":[1]},{\"count\":1,\"startMillis\":300000,\"durationMillis\":60000,\"means\":[100001.0],\"counts\":[1]},{\"count\":1,\"startMillis\":360000,\"durationMillis\":60000,\"means\":[100011.0],\"counts\":[1]},{\"count\":1,\"startMillis\":420000,\"durationMillis\":60000,\"means\":[100111.0],\"counts\":[1]},{\"count\":1,\"startMillis\":480000,\"durationMillis\":60000,\"means\":[101111.0],\"counts\":[1]},{\"count\":1,\"startMillis\":540000,\"durationMillis\":60000,\"means\":[111111.0],\"counts\":[1]},{\"count\":1,\"startMillis\":600000,\"durationMillis\":60000,\"means\":[111112.0],\"counts\":[1]}]}}");
  }

  @Test
  public void testWavefrontHistogramBulkUpdate() throws IOException {
    WavefrontHistogram wh = WavefrontHistogram.get(testRegistry, new MetricName("test", "", "metric"), time::get);

    wh.bulkUpdate(ImmutableList.of(15d, 30d, 45d), ImmutableList.of(1, 5, 1));

    // Simulate the 1 minute has passed and we are ready to flush the histogram
    // (i.e. all the values prior to the current minute) over the wire...
    time.addAndGet(60001L);

    String json = generate(false, false, false, null);

    assertThat(json).isEqualTo("{\"test.metric\":{\"bins\":[{\"count\":7,\"startMillis\":0,\"durationMillis\":60000,\"means\":[15.0,30.0,45.0],\"counts\":[1,5,1]}]}}");
  }

  @Test
  public void testWavefrontHistogramBulkUpdateHandlesMismatchedLengths() throws IOException {
    WavefrontHistogram wh = WavefrontHistogram.get(testRegistry, new MetricName("test", "", "metric"), time::get);

    wh.bulkUpdate(ImmutableList.of(15d, 30d, 45d, 100d), ImmutableList.of(1, 5, 1));
    wh.bulkUpdate(ImmutableList.of(1d, 2d, 3d), ImmutableList.of(1, 1, 1, 9));

    // Simulate the 1 minute has passed and we are ready to flush the histogram
    // (i.e. all the values prior to the current minute) over the wire...
    time.addAndGet(60001L);

    String json = generate(false, false, false, null);

    assertThat(json).isEqualTo("{\"test.metric\":{\"bins\":[{\"count\":10,\"startMillis\":0,\"durationMillis\":60000,\"means\":[1.0,2.0,3.0,15.0,30.0,45.0],\"counts\":[1,1,1,1,5,1]}]}}");
  }

  @Test
  public void testWavefrontHistogramBulkUpdateHandlesNullParams() throws IOException {
    WavefrontHistogram wh = WavefrontHistogram.get(testRegistry, new MetricName("test", "", "metric"), time::get);

    wh.bulkUpdate(null, ImmutableList.of(1, 5, 1));
    wh.bulkUpdate(ImmutableList.of(15d, 30d, 45d, 100d), null);
    wh.bulkUpdate(null, null);

    String json = generate(false, false, false, null);

    assertThat(json).isEqualTo("{\"test.metric\":{\"bins\":[]}}");
  }
}
