package com.wavefront.agent.sampler;

import com.wavefront.api.agent.BloomFilterDTO;
import com.wavefront.common.bloomfilter.ReadOnlyBloomFilter;
import org.junit.Test;
import wavefront.report.ReportPoint;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class MetricBloomFilterSamplerTest {

  @Test
  public void testShouldSampleOut() throws Exception {
    MetricBloomFilterSampler sampler = new MetricBloomFilterSampler();

    assertFalse(sampler.shouldSampleOut(null));

    long timeStamp = System.currentTimeMillis();
    ReportPoint pointWithTrackedTag = createPoint("testMetric", "myHost", mapOf("tag1", "testVal"), timeStamp);

    // No tracked tags configured means no sampling
    assertFalse(sampler.shouldSampleOut(pointWithTrackedTag));

    sampler.setTrackedTagKeys(new String[] {"tag1"});
    ReportPoint pointWithoutTrackedTag = createPoint("testMetric", "myHost", mapOf("cluster", "testVal"), timeStamp);

    // Without loaded bloom filter, does not sample points out
    assertFalse(sampler.shouldSampleOut(pointWithoutTrackedTag));

    // Queried series (bloom filter hits) should be kept
    BloomFilterDTO hitDto = createBloomFilterDto(createAlwaysHitBloomFilterBytes(), "tag1");
    sampler.updateBloomFilters(hitDto);
    sampler.setNonQueriedKeepPercentFromSamplingRate(1.0d);
    assertFalse(sampler.shouldSampleOut(pointWithTrackedTag));

    // Bloom filter miss and keep percentage at 0 should sample out all points
    BloomFilterDTO missDto = createBloomFilterDto(createEmptyBloomFilterBytes(), "tag1");
    sampler.updateBloomFilters(missDto);
    sampler.setNonQueriedKeepPercentFromSamplingRate(1.0d);
    assertTrue(sampler.shouldSampleOut(pointWithTrackedTag));

    // Bloom miss and keep at 100 percent should keep all points
    sampler.setNonQueriedKeepPercentFromSamplingRate(0.0d);
    assertFalse(sampler.shouldSampleOut(pointWithTrackedTag));
  }

  @Test
  public void testUpdateBloomFilters() throws Exception {
    MetricBloomFilterSampler sampler = new MetricBloomFilterSampler();

    BloomFilterDTO dtoWithBloomBytes =
        createBloomFilterDto(createEmptyBloomFilterBytes(), "tag1", "sampled");

    sampler.updateBloomFilters(dtoWithBloomBytes);

    List<String> trackedTagKeys = getTrackedTagKeys(sampler);
    List<ReadOnlyAbstractWindowingBloomFilters> bloomFilters = getBloomFilters(sampler);

    assertEquals(Arrays.asList("tag1", "sampled"), trackedTagKeys);
    assertEquals(1, bloomFilters.size());
    assertNotNull(bloomFilters.get(0));
  }

  @Test
  public void testClearBloomFilters() throws Exception {
    MetricBloomFilterSampler sampler = new MetricBloomFilterSampler();
    sampler.updateBloomFilters(createBloomFilterDto(createEmptyBloomFilterBytes(), "tag1"));

    assertFalse(getTrackedTagKeys(sampler).isEmpty());
    assertFalse(getBloomFilters(sampler).isEmpty());

    invokeClearBloomFilters(sampler);

    assertTrue(getTrackedTagKeys(sampler).isEmpty());
    assertTrue(getBloomFilters(sampler).isEmpty());
  }

  @Test
  public void testHasTrackedTag() throws Exception {
    Map<String, String> annotations = mapOf("tag1", "testVal");

    assertTrue(invokeHasTrackedTag(annotations, Collections.singletonList("tag1")));
    assertFalse(invokeHasTrackedTag(annotations, Collections.singletonList("cluster")));
    assertFalse(invokeHasTrackedTag(null, Collections.singletonList("tag1")));
    assertFalse(invokeHasTrackedTag(Collections.emptyMap(), Collections.singletonList("tag1")));
  }

  @Test
  public void testToBloomFilterLookupKeyBytes() {
    ReportPoint point =
        createPoint(
            "testMetric",
            "myHost",
            mapOf("tag1", "testVal", "cluster", "test"),
            System.currentTimeMillis());

    byte[] canonicalSeriesKey =
        MetricBloomFilterSampler.toBloomFilterLookupKeyBytes(point, Arrays.asList("tag1", "cluster"));

    assertArrayEquals("m|testMetric|tag1=testVal".getBytes(StandardCharsets.UTF_8), canonicalSeriesKey);
  }

  @Test
  public void testShouldKeepBySamplingModuloAndMinute() throws Exception {
    MetricBloomFilterSampler sampler = new MetricBloomFilterSampler();
    byte[] key = new byte[0]; // Arrays.hashCode(empty byte[]) == 1

    sampler.setNonQueriedKeepPercentFromSamplingRate(1.0d);
    assertFalse(invokeShouldKeepBySamplingModuloAndMinute(sampler, key, 60_000L));

    sampler.setNonQueriedKeepPercentFromSamplingRate(0.0d);
    assertTrue(invokeShouldKeepBySamplingModuloAndMinute(sampler, key, 60_000L));

    sampler.setNonQueriedKeepPercentFromSamplingRate(0.5d); // keepPercent=50, modulo=2
    assertTrue(invokeShouldKeepBySamplingModuloAndMinute(sampler, key, 60_000L)); // minute bucket 1
    assertFalse(invokeShouldKeepBySamplingModuloAndMinute(sampler, key, 0L)); // minute bucket 0
  }

  @Test
  public void testUpdateBloomFilterNullClearsState() throws Exception {
    MetricBloomFilterSampler sampler = new MetricBloomFilterSampler();
    sampler.updateBloomFilters(createBloomFilterDto(createEmptyBloomFilterBytes(), "tag1"));

    sampler.updateBloomFilters(null);

    assertTrue(getTrackedTagKeys(sampler).isEmpty());
    assertTrue(getBloomFilters(sampler).isEmpty());
  }

  @Test
  public void testUpdateBloomFiltersEmptyOrNullPayloadClearsStateAndIncrementsCounter() throws Exception {
    MetricBloomFilterSampler sampler = new MetricBloomFilterSampler();
    sampler.updateBloomFilters(createBloomFilterDto(createEmptyBloomFilterBytes(), "tag1"));

    BloomFilterDTO nullPayload = new BloomFilterDTO();
    nullPayload.bloomFilterShardToBytes = null;
    nullPayload.sampledTagKeys = new String[] {"tag1"};
    nullPayload.maxShardSize = 1;
    sampler.updateBloomFilters(nullPayload);

    assertTrue(getTrackedTagKeys(sampler).isEmpty());
    assertTrue(getBloomFilters(sampler).isEmpty());

    BloomFilterDTO emptyPayload = new BloomFilterDTO();
    emptyPayload.bloomFilterShardToBytes = new Map[0];
    emptyPayload.sampledTagKeys = new String[] {"tag1"};
    emptyPayload.maxShardSize = 1;
    sampler.updateBloomFilters(emptyPayload);

    assertTrue(getTrackedTagKeys(sampler).isEmpty());
    assertTrue(getBloomFilters(sampler).isEmpty());
  }

  @Test
  public void testAddTrackedTagKeys() throws Exception {
    MetricBloomFilterSampler sampler = new MetricBloomFilterSampler();
    List<String> allTagKeys = new ArrayList<>();

    invokeAddTrackedTagKeys(sampler, new String[] {null, "", "tag1", "tag2"}, allTagKeys);

    assertEquals(Arrays.asList("tag1", "tag2"), allTagKeys);
  }

  @Test
  public void testSetTrackedTagKeys() throws Exception {
    MetricBloomFilterSampler sampler = new MetricBloomFilterSampler();
    sampler.setTrackedTagKeys(new String[] {"tag1", "", "tag2"});

    assertEquals(Arrays.asList("tag1", "tag2"), getTrackedTagKeys(sampler));
  }

  @Test
  public void testSetNonQueriedKeepPercentFromSamplingRate() throws Exception {
    MetricBloomFilterSampler sampler = new MetricBloomFilterSampler();

    sampler.setNonQueriedKeepPercentFromSamplingRate(0.7d);
    assertEquals(30, getNonQueriedKeepPercent(sampler));

    sampler.setNonQueriedKeepPercentFromSamplingRate(-0.25d);
    assertEquals(100, getNonQueriedKeepPercent(sampler));

    sampler.setNonQueriedKeepPercentFromSamplingRate(1.5d);
    assertEquals(0, getNonQueriedKeepPercent(sampler));
  }

  @Test
  public void testPercentToModulo() {
    assertEquals(2, MetricBloomFilterSampler.percentToModulo(50));
    assertEquals(4, MetricBloomFilterSampler.percentToModulo(25));
    assertEquals(10, MetricBloomFilterSampler.percentToModulo(10));
    assertEquals(1, MetricBloomFilterSampler.percentToModulo(100));
  }

  private static BloomFilterDTO createBloomFilterDto(
      byte[] serializedBloomFilter, String... sampledTagKeys) {
    BloomFilterDTO dto = new BloomFilterDTO();
    dto.bloomFilterShardToBytes =
        new Map[] {Collections.singletonMap(0, toManagedBloomFilterBytes(serializedBloomFilter))};
    dto.sampledTagKeys = sampledTagKeys;
    dto.maxShardSize = 1;
    return dto;
  }

  private static byte[] createEmptyBloomFilterBytes() throws Exception {
    ReadOnlyBloomFilter bloomFilter = ReadOnlyBloomFilter.create(1000, 0.01d);
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    bloomFilter.writeTo(out);
    return out.toByteArray();
  }

  /**
   * Creates a serialized bloom filter payload where all bits are set, so mightContain() always hits.
   * Wire format: strategy byte, numHashFunctions byte, dataLength int, then N long words.
   */
  private static byte[] createAlwaysHitBloomFilterBytes() throws Exception {
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    DataOutputStream dataOut = new DataOutputStream(out);
    dataOut.writeByte(0); // BloomFilterStrategies ordinal
    dataOut.writeByte(1); // numHashFunctions
    dataOut.writeInt(1); // one 64-bit word
    dataOut.writeLong(-1L); // all bits set
    dataOut.flush();
    return out.toByteArray();
  }

  /**
   * ManagedBloomFilter bytes are tuple-encoded as:
   *   [byte[] serializedBloomFilter][long numInsertions ...]
   * ReadOnlyManagedBloomFilter only needs to see at least one trailing long marker.
   */
  private static byte[] toManagedBloomFilterBytes(byte[] serializedBloomFilter) {
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    writeTupleEncodedByteArray(out, serializedBloomFilter);
    out.write(20); // tuple-encoded long(0)
    return out.toByteArray();
  }

  private static void writeTupleEncodedByteArray(ByteArrayOutputStream out, byte[] value) {
    out.write(1); // tuple bytes type
    for (byte b : value) {
      if (b == 0) {
        out.write(0);
        out.write(0xFF); // escaped zero
      } else {
        out.write(b);
      }
    }
    out.write(0); // terminator
  }

  private static ReportPoint createPoint(String metric, String host, Map<String, String> annotations, long timestampMillis) {
    ReportPoint point = new ReportPoint();
    point.setMetric(metric);
    point.setHost(host);
    point.setAnnotations(annotations);
    point.setTimestamp(timestampMillis);
    point.setValue(1.0d);
    return point;
  }

  private static Map<String, String> mapOf(String key, String value) {
    Map<String, String> map = new HashMap<>();
    map.put(key, value);
    return map;
  }

  private static Map<String, String> mapOf(String key1, String value1, String key2, String value2) {
    Map<String, String> map = new HashMap<>();
    map.put(key1, value1);
    map.put(key2, value2);
    return map;
  }

  private static void invokeClearBloomFilters(MetricBloomFilterSampler sampler) throws Exception {
    Method clearMethod = MetricBloomFilterSampler.class.getDeclaredMethod("clearBloomFilters");
    clearMethod.setAccessible(true);
    clearMethod.invoke(sampler);
  }

  private static void invokeAddTrackedTagKeys(MetricBloomFilterSampler sampler, String[] sampledTagKeys,
                                              List<String> allTagKeys) throws Exception {
    Method addTrackedTagKeysMethod = MetricBloomFilterSampler.class.getDeclaredMethod("addTrackedTagKeys", String[].class, List.class);
    addTrackedTagKeysMethod.setAccessible(true);
    addTrackedTagKeysMethod.invoke(sampler, sampledTagKeys, allTagKeys);
  }

  private static boolean invokeHasTrackedTag(Map<String, String> annotations, List<String> trackedTagKeys) throws Exception {
    Method hasTrackedTagMethod = MetricBloomFilterSampler.class.getDeclaredMethod("hasTrackedTag", Map.class, List.class);
    hasTrackedTagMethod.setAccessible(true);
    return (Boolean) hasTrackedTagMethod.invoke(null, annotations, trackedTagKeys);
  }

  private static boolean invokeShouldKeepBySamplingModuloAndMinute(MetricBloomFilterSampler sampler, byte[] canonicalSeriesKey,
                                                                   long timestampMillis) throws Exception {
    Method shouldKeepByModuloAndMinuteMethod = MetricBloomFilterSampler.class.getDeclaredMethod("shouldKeepBySamplingModuloAndMinute", byte[].class, long.class);
    shouldKeepByModuloAndMinuteMethod.setAccessible(true);
    return (Boolean) shouldKeepByModuloAndMinuteMethod.invoke(sampler, canonicalSeriesKey, timestampMillis);
  }

  @SuppressWarnings("unchecked")
  private static List<String> getTrackedTagKeys(MetricBloomFilterSampler sampler) throws Exception {
    Field trackedTagKeysField = MetricBloomFilterSampler.class.getDeclaredField("trackedTagKeysRef");
    trackedTagKeysField.setAccessible(true);
    return ((AtomicReference<List<String>>) trackedTagKeysField.get(sampler)).get();
  }

  @SuppressWarnings("unchecked")
  private static List<ReadOnlyAbstractWindowingBloomFilters> getBloomFilters(MetricBloomFilterSampler sampler) throws Exception {
    Field bloomFiltersField = MetricBloomFilterSampler.class.getDeclaredField("bloomFiltersRef");
    bloomFiltersField.setAccessible(true);
    return ((AtomicReference<List<ReadOnlyAbstractWindowingBloomFilters>>) bloomFiltersField.get(sampler)).get();
  }

  private static int getNonQueriedKeepPercent(MetricBloomFilterSampler sampler) throws Exception {
    Field keepPercentField = MetricBloomFilterSampler.class.getDeclaredField("nonQueriedKeepPercent");
    keepPercentField.setAccessible(true);
    return ((AtomicInteger) keepPercentField.get(sampler)).get();
  }
}
