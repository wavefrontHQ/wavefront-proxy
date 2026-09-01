package com.wavefront.agent.handlers;

import com.wavefront.agent.TenantInfo;
import com.wavefront.agent.TokenManager;
import com.wavefront.agent.api.APIContainer;
import com.wavefront.agent.data.QueueingReason;
import com.wavefront.agent.sampler.MetricBloomFilterSampler;
import com.wavefront.api.agent.ValidationConfiguration;
import com.wavefront.data.ReportableEntityType;
import org.junit.After;
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

  /**
   * The annotation key injected by forward preprocessor rules. Mirrors
   * {@link com.wavefront.api.agent.preprocessor.PreprocessorConfigManager#FORWARD_ROUTING_KEY}.
   */
  private static final String FORWARD_KEY = "wf_forward_tenants";

  /** Reset TokenManager state between tests that mutate it. */
  @After
  public void tearDown() {
    TokenManager.reset();
  }

  /** Minimal TenantInfo stub for TokenManager registration in tests. */
  private static TenantInfo stubTenant(String serverUrl) {
    return new TenantInfo() {
      @Override public String getWFServer() { return serverUrl; }
      @Override public String getBearerToken() { return "test-token"; }
    };
  }

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

  // Forward-routing tests (multi-tenant proxy feature)

  /**
   * When the forward annotation names a single tenant, data must be sent only to that tenant's
   * task and must NOT be sent to the central tenant.
   */
  @Test
  public void testForwardAnnotationRoutesOnlyToNamedTenant() {
    RecordingSenderTask centralTask = new RecordingSenderTask();
    RecordingSenderTask tenantATask = new RecordingSenderTask();
    ReportPointHandlerImpl handler =
        createHandler(buildSenderMap(centralTask, "tenant-a", tenantATask), null);

    ReportPoint point = createPoint("cpu.usage", "host1", 42.0, mapOf(FORWARD_KEY, "tenant-a"));

    try {
      handler.reportInternal(point);
      assertEquals("Forward-target task must receive exactly 1 point", 1, tenantATask.items.size());
      assertTrue("Central task must not receive data when forward annotation is present",
          centralTask.items.isEmpty());
    } finally {
      handler.shutdown();
    }
  }

  /**
   * When the forward annotation lists two distinct tenants, each must receive exactly one copy and
   * the central tenant must receive nothing.
   */
  @Test
  public void testForwardAnnotationToMultipleTenants() {
    RecordingSenderTask centralTask = new RecordingSenderTask();
    RecordingSenderTask tenantATask = new RecordingSenderTask();
    RecordingSenderTask tenantBTask = new RecordingSenderTask();
    Map<String, Collection<SenderTask<String>>> senderTaskMap = new HashMap<>();
    senderTaskMap.put(APIContainer.CENTRAL_TENANT_NAME, Collections.singletonList(centralTask));
    senderTaskMap.put("tenant-a", Collections.singletonList(tenantATask));
    senderTaskMap.put("tenant-b", Collections.singletonList(tenantBTask));

    ReportPointHandlerImpl handler = createHandler(senderTaskMap, null);

    ReportPoint point = createPoint("cpu.usage", "host1", 42.0,
        mapOf(FORWARD_KEY, "tenant-a,tenant-b"));

    try {
      handler.reportInternal(point);
      assertEquals("tenant-a must receive exactly 1 point", 1, tenantATask.items.size());
      assertEquals("tenant-b must receive exactly 1 point", 1, tenantBTask.items.size());
      assertTrue("Central task must not receive data when forward annotation is present",
          centralTask.items.isEmpty());
    } finally {
      handler.shutdown();
    }
  }

  /**
   * When the same tenant name appears more than once in the comma-separated forward annotation,
   * the point must be delivered to that tenant exactly once (deduplication).
   */
  @Test
  public void testForwardAnnotationDeduplicatesRepeatedTenants() {
    RecordingSenderTask centralTask = new RecordingSenderTask();
    RecordingSenderTask tenantATask = new RecordingSenderTask();
    ReportPointHandlerImpl handler =
        createHandler(buildSenderMap(centralTask, "tenant-a", tenantATask), null);

    ReportPoint point = createPoint("cpu.usage", "host1", 42.0,
        mapOf(FORWARD_KEY, "tenant-a,tenant-a"));

    try {
      handler.reportInternal(point);
      assertEquals("Duplicate tenant names in forward annotation must be deduplicated to one send",
          1, tenantATask.items.size());
    } finally {
      handler.shutdown();
    }
  }

  /**
   * When the forward annotation names a tenant that has no registered sender task, the point must
   * be silently dropped — no exception, no delivery to any task.
   */
  @Test
  public void testForwardAnnotationToUnknownTenantDropsData() {
    RecordingSenderTask centralTask = new RecordingSenderTask();
    ReportPointHandlerImpl handler = createHandler(createSenderMap(centralTask), null);

    ReportPoint point = createPoint("cpu.usage", "host1", 42.0,
        mapOf(FORWARD_KEY, "ghost-tenant"));

    try {
      handler.reportInternal(point);
      assertTrue("No task should receive data when forward target is unknown",
          centralTask.items.isEmpty());
    } finally {
      handler.shutdown();
    }
  }

  /**
   * The {@code wf_forward_tenants} annotation must be stripped from the point before
   * serialization so it does not appear in the payload delivered to the tenant.
   */
  @Test
  public void testForwardAnnotationRemovedFromSerializedPoint() {
    RecordingSenderTask tenantATask = new RecordingSenderTask();
    Map<String, Collection<SenderTask<String>>> senderTaskMap = new HashMap<>();
    senderTaskMap.put(APIContainer.CENTRAL_TENANT_NAME,
        Collections.singletonList(new RecordingSenderTask()));
    senderTaskMap.put("tenant-a", Collections.singletonList(tenantATask));

    ReportPointHandlerImpl handler = createHandler(senderTaskMap, null);

    ReportPoint point = createPoint("cpu.usage", "host1", 42.0, mapOf(FORWARD_KEY, "tenant-a"));

    try {
      handler.reportInternal(point);
      assertEquals(1, tenantATask.items.size());
      assertFalse(
          "wf_forward_tenants must not appear in the serialized point payload",
          tenantATask.items.get(0).contains(FORWARD_KEY));
    } finally {
      handler.shutdown();
    }
  }

  /**
   * When a human-readable alias is configured as the {@code defaultTenant}, a forward annotation
   * referencing that alias must be resolved to the central tenant's sender task.
   */
  @Test
  public void testDefaultTenantAliasResolvesToCentralTask() {
    RecordingSenderTask centralTask = new RecordingSenderTask();
    // Handler configured with alias "MyCluster" → resolves to CENTRAL_TENANT_NAME
    ReportPointHandlerImpl handler =
        createHandler(createSenderMap(centralTask), null, "MyCluster");

    ReportPoint point = createPoint("cpu.usage", "host1", 42.0, mapOf(FORWARD_KEY, "MyCluster"));

    try {
      handler.reportInternal(point);
      assertEquals("Alias in forward annotation must resolve to the central tenant task",
          1, centralTask.items.size());
    } finally {
      handler.shutdown();
    }
  }

  // --------------------------------------------------------------------------
  // "central" multicasting tenant edge-case tests
  // --------------------------------------------------------------------------

  /**
   * When a forward rule explicitly says {@code tenants: [central]} AND a multicasting tenant
   * named "central" is also registered (on a different server), the point must go ONLY to the
   * multicasting "central" endpoint (synthetic key "central~2") — NOT to the primary cluster.
   *
   * <p>Rationale: "central" written in a forward rule unambiguously refers to the
   * user-configured multicasting tenant by that name. The primary cluster is always reachable
   * via the {@code defaultTenant} alias (e.g. "Localdev").
   */
  @Test
  public void testForwardAnnotationCentralNameGoesToMulticastingCentralTenant() {
    // Register primary cluster first (server1, key = "central"), then multicasting "central"
    // on a different server (server2 → synthetic key "central~2").
    TokenManager.addTenant(APIContainer.CENTRAL_TENANT_NAME, stubTenant("http://server1/api/"));
    TokenManager.addTenant(APIContainer.CENTRAL_TENANT_NAME, stubTenant("http://server2/api/"));

    RecordingSenderTask primaryTask = new RecordingSenderTask();
    RecordingSenderTask multicastingCentralTask = new RecordingSenderTask();

    Map<String, Collection<SenderTask<String>>> senderMap = new HashMap<>();
    senderMap.put(APIContainer.CENTRAL_TENANT_NAME, Collections.singletonList(primaryTask));
    senderMap.put("central~2", Collections.singletonList(multicastingCentralTask));

    ReportPointHandlerImpl handler = createHandler(senderMap, null, "Localdev");
    ReportPoint point = createPoint("cpu.usage", "host1", 1.0, mapOf(FORWARD_KEY, "central"));

    try {
      handler.reportInternal(point);
      assertEquals(
          "Forward rule 'central' must route to multicasting central tenant only",
          1, multicastingCentralTask.items.size());
      assertTrue(
          "Primary cluster must NOT receive data when forward rule says 'central' and a "
              + "multicasting tenant named 'central' exists",
          primaryTask.items.isEmpty());
    } finally {
      handler.shutdown();
    }
  }

  /**
   * When the defaultTenant alias is used in a forward rule (e.g. "Localdev") and a multicasting
   * tenant named "central" also exists, the point must go ONLY to the primary cluster —
   * never to the multicasting "central~2" endpoint.
   */
  @Test
  public void testDefaultTenantAliasGoesToPrimaryNotMulticastingCentral() {
    TokenManager.addTenant(APIContainer.CENTRAL_TENANT_NAME, stubTenant("http://server1/api/"));
    TokenManager.addTenant(APIContainer.CENTRAL_TENANT_NAME, stubTenant("http://server2/api/"));

    RecordingSenderTask primaryTask = new RecordingSenderTask();
    RecordingSenderTask multicastingCentralTask = new RecordingSenderTask();

    Map<String, Collection<SenderTask<String>>> senderMap = new HashMap<>();
    senderMap.put(APIContainer.CENTRAL_TENANT_NAME, Collections.singletonList(primaryTask));
    senderMap.put("central~2", Collections.singletonList(multicastingCentralTask));

    // defaultTenant="Localdev" so "Localdev" in forward annotation resolves to "central".
    ReportPointHandlerImpl handler = createHandler(senderMap, null, "Localdev");
    ReportPoint point = createPoint("cpu.usage", "host1", 1.0, mapOf(FORWARD_KEY, "Localdev"));

    try {
      handler.reportInternal(point);
      assertEquals(
          "Forward rule 'Localdev' (defaultTenant alias) must route to primary cluster only",
          1, primaryTask.items.size());
      assertTrue(
          "Multicasting 'central~2' endpoint must NOT receive data when 'Localdev' alias is used",
          multicastingCentralTask.items.isEmpty());
    } finally {
      handler.shutdown();
    }
  }

  /**
   * When no multicasting tenant named "central" exists, {@code tenants: [central]} in a forward
   * rule falls through to the primary cluster via the normal fallback path.
   */
  @Test
  public void testForwardAnnotationCentralNameGoesToPrimaryWhenNoMulticastingCentral() {
    // Only the primary cluster is registered — no "central~2".
    TokenManager.addTenant(APIContainer.CENTRAL_TENANT_NAME, stubTenant("http://server1/api/"));

    RecordingSenderTask primaryTask = new RecordingSenderTask();
    ReportPointHandlerImpl handler = createHandler(createSenderMap(primaryTask), null, "Localdev");
    ReportPoint point = createPoint("cpu.usage", "host1", 1.0, mapOf(FORWARD_KEY, "central"));

    try {
      handler.reportInternal(point);
      assertEquals(
          "Forward rule 'central' with no multicasting central tenant must route to primary cluster",
          1, primaryTask.items.size());
    } finally {
      handler.shutdown();
    }
  }

  /**
   * When no forward annotation is present, the point must be delivered to the central tenant via
   * the legacy routing path, regardless of whether other tenant tasks are registered.
   */
  @Test
  public void testNoForwardAnnotationUsesLegacyRoutingToCentral() {
    RecordingSenderTask centralTask = new RecordingSenderTask();
    RecordingSenderTask tenantATask = new RecordingSenderTask();
    ReportPointHandlerImpl handler =
        createHandler(buildSenderMap(centralTask, "tenant-a", tenantATask), null);

    ReportPoint point = createPoint("cpu.usage", "host1", 42.0, Collections.emptyMap());

    try {
      handler.reportInternal(point);
      assertEquals("Central task must receive point when no forward annotation is present",
          1, centralTask.items.size());
      assertTrue("Tenant task must not receive data via legacy routing",
          tenantATask.items.isEmpty());
    } finally {
      handler.shutdown();
    }
  }

  /**
   * In single-tenant mode (only the central task exists, no {@code defaultTenant} alias set),
   * a point with no forward annotation must be delivered to the central task without errors.
   */
  @Test
  public void testSingleTenantModeRoutesToCentral() {
    RecordingSenderTask centralTask = new RecordingSenderTask();
    ReportPointHandlerImpl handler = createHandler(createSenderMap(centralTask), null);

    ReportPoint point = createPoint("jvm.heap", "host1", 512.0, Collections.emptyMap());

    try {
      handler.reportInternal(point);
      assertEquals("Single-tenant: central task must receive the point", 1, centralTask.items.size());
    } finally {
      handler.shutdown();
    }
  }

  /**
   * When multicasting is active (legacy path) and the point carries whitespace-padded tenant names
   * in the forward annotation, the names must be trimmed before lookup.
   */
  @Test
  public void testForwardAnnotationTrimsTenantNames() {
    RecordingSenderTask centralTask = new RecordingSenderTask();
    RecordingSenderTask tenantATask = new RecordingSenderTask();
    ReportPointHandlerImpl handler =
        createHandler(buildSenderMap(centralTask, "tenant-a", tenantATask), null);

    // deliberately padded with spaces
    ReportPoint point = createPoint("cpu.usage", "host1", 42.0,
        mapOf(FORWARD_KEY, " tenant-a , tenant-a "));

    try {
      handler.reportInternal(point);
      assertEquals("Whitespace-padded tenant names must be trimmed and deduplicated",
          1, tenantATask.items.size());
      assertTrue(centralTask.items.isEmpty());
    } finally {
      handler.shutdown();
    }
  }

  /** Creates a handler without a {@code defaultTenant} alias (single-tenant or no-alias mode). */
  private static ReportPointHandlerImpl createHandler(
      Map<String, Collection<SenderTask<String>>> senderTaskMap,
      MetricBloomFilterSampler metricBloomFilterSampler) {
    return createHandler(senderTaskMap, metricBloomFilterSampler, null);
  }

  /** Creates a handler with an optional {@code defaultTenant} alias for forward-rule resolution. */
  private static ReportPointHandlerImpl createHandler(
      Map<String, Collection<SenderTask<String>>> senderTaskMap,
      MetricBloomFilterSampler metricBloomFilterSampler,
      String defaultTenant) {
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
        metricBloomFilterSampler,
        defaultTenant);
  }

  /** Builds a sender map with the central task plus one additional named tenant task. */
  private static Map<String, Collection<SenderTask<String>>> buildSenderMap(
      RecordingSenderTask centralTask, String tenantName, RecordingSenderTask tenantTask) {
    Map<String, Collection<SenderTask<String>>> senderTaskMap = new HashMap<>();
    senderTaskMap.put(APIContainer.CENTRAL_TENANT_NAME, Collections.singletonList(centralTask));
    senderTaskMap.put(tenantName, Collections.singletonList(tenantTask));
    return senderTaskMap;
  }

  private static Map<String, Collection<SenderTask<String>>> createSenderMap(
      RecordingSenderTask centralTask) {
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
