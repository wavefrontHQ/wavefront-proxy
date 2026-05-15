package com.wavefront.agent.handlers;

import com.wavefront.agent.api.APIContainer;
import com.wavefront.agent.data.QueueingReason;
import com.wavefront.agent.sampler.MetricBloomFilterSampler;
import com.wavefront.api.agent.ValidationConfiguration;
import com.wavefront.data.ReportableEntityType;
import org.junit.Test;
import wavefront.report.ReportPoint;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class ReportPointHandlerImplTest {

  @Test
  public void testReportInternalRejectsDeltaCounterWithNonPositiveValue() {
    RecordingSenderTask centralTask = new RecordingSenderTask();
    ReportPointHandlerImpl handler =
        createHandler(createSenderMap(centralTask), null);

    // create point beginning with delta symbol
    ReportPoint point = createPoint("\u2206deltaPoint", "myHost", 0, Collections.emptyMap());

    try {
      handler.reportInternal(point);
      assertTrue(centralTask.items.isEmpty());
    } finally {
      handler.shutdown();
    }
  }

  @Test
  public void testReportInternalDropsSampledOutPoints() {
    RecordingSenderTask centralTask = new RecordingSenderTask();
    ReportPointHandlerImpl handler =
        createHandler(createSenderMap(centralTask), new FixedDecisionSampler(true));

    ReportPoint point =
        createPoint("cpu.utilization", "myHost", 1, mapOf("tag", "val"));

    try {
      handler.reportInternal(point);
      assertTrue(centralTask.items.isEmpty());
    } finally {
      handler.shutdown();
    }
  }

  @Test
  public void testReportInternalMulticastsToConfiguredTenants() {
    RecordingSenderTask centralTask = new RecordingSenderTask();
    RecordingSenderTask tenantTask = new RecordingSenderTask();
    Map<String, Collection<SenderTask<String>>> senderTaskMap = new HashMap<>();
    senderTaskMap.put(APIContainer.CENTRAL_TENANT_NAME, Collections.singletonList(centralTask));
    senderTaskMap.put("tenant-a", Collections.singletonList(tenantTask));

    ReportPointHandlerImpl handler = createHandler(senderTaskMap, new FixedDecisionSampler(false));

    Map<String, String> annotations = new HashMap<>();
    annotations.put("env", "prod");
    annotations.put("multicastingTenantName", "tenant-a,tenant-b");
    ReportPoint point = createPoint("testMetric", "myHost", 1, annotations);

    try {
      handler.reportInternal(point);
      assertEquals(1, centralTask.items.size());
      assertEquals(1, tenantTask.items.size());
      // false because multicasting tag exists, tag is removed
      assertFalse(point.getAnnotations().containsKey("multicastingTenantName"));
      assertNotNull(centralTask.items.get(0));
      assertNotNull(tenantTask.items.get(0));
    } finally {
      handler.shutdown();
    }
  }

  private static ReportPointHandlerImpl createHandler(
      Map<String, Collection<SenderTask<String>>> senderTaskMap,
      MetricBloomFilterSampler metricBloomFilterSampler) {
    return new ReportPointHandlerImpl(
        HandlerKey.of(ReportableEntityType.POINT, "test-points"),
        0,
        senderTaskMap,
        new ValidationConfiguration(),
        false,
        null,
        null,
        null,
        null,
        metricBloomFilterSampler);
  }

  private static Map<String, Collection<SenderTask<String>>> createSenderMap(RecordingSenderTask centralTask) {
    Map<String, Collection<SenderTask<String>>> senderTaskMap = new HashMap<>();
    senderTaskMap.put(APIContainer.CENTRAL_TENANT_NAME, Collections.singletonList(centralTask));
    return senderTaskMap;
  }

  private static ReportPoint createPoint(
      String metric, String host, double value, Map<String, String> annotations) {
    ReportPoint point = new ReportPoint();
    point.setMetric(metric);
    point.setHost(host);
    point.setValue(value);
    point.setTimestamp(System.currentTimeMillis());
    point.setAnnotations(new HashMap<>(annotations));
    return point;
  }

  private static Map<String, String> mapOf(String key, String value) {
    Map<String, String> map = new HashMap<>();
    map.put(key, value);
    return map;
  }

  private static final class FixedDecisionSampler extends MetricBloomFilterSampler {
    private final boolean shouldSampleOut;

    private FixedDecisionSampler(boolean shouldSampleOut) {
      this.shouldSampleOut = shouldSampleOut;
    }

    @Override
    public boolean shouldSampleOut(ReportPoint point) {
      return shouldSampleOut;
    }
  }

  private static final class RecordingSenderTask implements SenderTask<String> {
    private final List<String> items = new ArrayList<>();

    @Override
    public void add(String item) {
      items.add(item);
    }

    @Override
    public long getTaskRelativeScore() {
      return 0;
    }

    @Override
    public void drainBuffersToQueue(QueueingReason reason) {}

    @Override
    public void start() {}

    @Override
    public void stop() {}
  }
}
