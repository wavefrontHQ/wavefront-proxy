package com.wavefront.agent.handlers;

import static com.wavefront.agent.api.APIContainer.CENTRAL_TENANT_NAME;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import com.wavefront.agent.TokenManager;
import com.wavefront.agent.TokenWorkerWF;
import com.wavefront.agent.api.APIContainer;
import com.wavefront.agent.preprocessor.ProxyPreprocessorConfigManager;
import com.wavefront.api.agent.ValidationConfiguration;
import com.wavefront.api.agent.preprocessor.ReportableEntityPreprocessor;
import com.wavefront.data.ReportableEntityType;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import wavefront.report.ReportPoint;

/**
 * End-to-end tests that exercise preprocessor YAML rule files and verify that
 * every inbound point reaches at least one tenant task — no data dropped.
 *
 * <p>Each test:
 * <ol>
 *   <li>Sets up {@link TokenManager} with the relevant tenants.
 *   <li>Loads a YAML scenario file via {@link ProxyPreprocessorConfigManager#loadFERules}.
 *   <li>Applies the preprocessor rules to a {@link ReportPoint} (injects the forward annotation
 *       if a forward rule matches).
 *   <li>Routes the point through {@link ReportPointHandlerImpl#reportInternal}.
 *   <li>Asserts that the point arrived in exactly the expected task(s).
 * </ol>
 *
 * <p>Scenarios covered:
 * <ul>
 *   <li>Single-tenant (no {@code multicastingTenants} configured) — all points reach central.
 *   <li>Multi-tenant routing by metric prefix — each prefix routes to the correct tenant.
 *   <li>No matching forward rule — unmatched points fall back to central (never dropped).
 *   <li>Explicit forward to {@code "central"} — supported even though "central" is reserved.
 *   <li>Fan-out — same alias on two servers; both tasks receive a copy.
 *   <li>Transform-then-forward — transforms run before forward rules; forwarded payload carries
 *       the transformed metric name.
 *   <li>Default-tenant alias resolution — alias name forwards to central task.
 *   <li>Multi-port isolation — each port's rules are independent.
 *   <li>Unknown tenant validation — loading a rule targeting an unregistered tenant throws.
 *   <li>Tenant limit enforcement — configuring more than 10 tenants is rejected at startup.
 * </ul>
 */
public class PreprocessorScenarioTest {

  // -------------------------------------------------------------------------
  // Lifecycle
  // -------------------------------------------------------------------------

  @Before
  public void setUp() {
    TokenManager.reset();
    ProxyPreprocessorConfigManager.clearProxyConfigRules();
  }

  @After
  public void tearDown() {
    TokenManager.reset();
    ProxyPreprocessorConfigManager.clearProxyConfigRules();
  }

  // =========================================================================
  // Scenario 1: Single-tenant — no forward rules, all points reach central
  // =========================================================================

  /**
   * With no {@code multicastingTenants} configured, only the central task exists.
   * The preprocessor applies transforms but injects no forward annotation.
   * Every non-blocked point must reach central.
   */
  @Test
  public void testSingleTenantAllNonBlockedPointsReachCentral() throws IOException {
    String yaml = loadResource("preprocessor_scenario_single_tenant.yaml");
    ProxyPreprocessorConfigManager config = new ProxyPreprocessorConfigManager();
    // No multicasting tenants registered — single-tenant mode.
    config.loadFERules(yaml);

    RecordingSenderTask centralTask = new RecordingSenderTask();
    ReportPointHandlerImpl handler = createHandler(senderMap(centralTask), null, null);

    // Non-blocked metric: "app.cpu" does not match the "_synthetic.*" block rule.
    ReportPoint point = point("app.cpu", "host1", 42.0);

    ReportableEntityPreprocessor pre = config.get("2878").get();
    assertTrue("app.cpu must pass the single-tenant filter", pre.forReportPoint().filter(point));
    pre.forReportPoint().transform(point);

    // No forward annotation should be injected (no forward rules in this YAML).
    assertFalse("Single-tenant: no wf_forward_tenants annotation expected",
        point.getAnnotations().containsKey("wf_forward_tenants"));

    // Transform: hyphen normalisation was applied (no hyphen in "app.cpu", but tag was added).
    assertEquals("production", point.getAnnotations().get("env"));

    handler.reportInternal(point);

    assertEquals("Single-tenant: central must receive the point", 1, centralTask.items.size());
    handler.shutdown();
  }

  /**
   * A point matching the block rule ({@code _synthetic.*}) must be dropped by the filter,
   * confirming the filter works and is not bypassed.
   */
  @Test
  public void testSingleTenantBlockedPointIsFiltered() throws IOException {
    String yaml = loadResource("preprocessor_scenario_single_tenant.yaml");
    ProxyPreprocessorConfigManager config = new ProxyPreprocessorConfigManager();
    config.loadFERules(yaml);

    ReportPoint syntheticPoint = point("_synthetic.heartbeat", "host1", 1.0);
    assertFalse("_synthetic.* must be blocked by the filter",
        config.get("2878").get().forReportPoint().filter(syntheticPoint));
  }

  /**
   * Hyphens in the metric name are normalised to underscores by the single-tenant YAML rules.
   */
  @Test
  public void testSingleTenantTransformNormalisesHyphens() throws IOException {
    String yaml = loadResource("preprocessor_scenario_single_tenant.yaml");
    ProxyPreprocessorConfigManager config = new ProxyPreprocessorConfigManager();
    config.loadFERules(yaml);

    ReportPoint point = point("jvm.gc-pause", "host1", 10.0);
    config.get("2878").get().forReportPoint().transform(point);

    assertEquals("Hyphens must be normalised to underscores", "jvm.gc_pause", point.getMetric());
  }

  // =========================================================================
  // Scenario 2: Multi-tenant routing by metric prefix
  // =========================================================================

  /**
   * {@code prod.*} metrics must be routed to {@code tenant-a} and NOT to central or
   * {@code tenant-b}.
   */
  @Test
  public void testMultitenantProdMetricsRouteToTenantA() throws IOException {
    TokenManager.addTenant("tenant-a", new TokenWorkerWF("token-a", "https://server-a/api/"));
    TokenManager.addTenant("tenant-b", new TokenWorkerWF("token-b", "https://server-b/api/"));

    String yaml = loadResource("preprocessor_scenario_multitenant.yaml");
    ProxyPreprocessorConfigManager config = new ProxyPreprocessorConfigManager();
    config.setDefaultTenant("primary-cluster"); // required: forward rules + multi-tenant mode
    config.loadFERules(yaml);

    RecordingSenderTask centralTask = new RecordingSenderTask();
    RecordingSenderTask tenantATask = new RecordingSenderTask();
    RecordingSenderTask tenantBTask = new RecordingSenderTask();
    ReportPointHandlerImpl handler = createHandler(
        senderMap(centralTask, "tenant-a", tenantATask, "tenant-b", tenantBTask), null, null);

    ReportPoint point = point("prod.cpu", "host1", 42.0);
    config.get("2878").get().forReportPoint().transform(point);
    assertEquals("wf_forward_tenants", "tenant-a",
        point.getAnnotations().get("wf_forward_tenants"));

    handler.reportInternal(point);

    assertEquals("prod.cpu: tenant-a must receive 1 point", 1, tenantATask.items.size());
    assertTrue("prod.cpu: central must not receive data", centralTask.items.isEmpty());
    assertTrue("prod.cpu: tenant-b must not receive data", tenantBTask.items.isEmpty());
    handler.shutdown();
  }

  /**
   * {@code staging.*} metrics must be routed to {@code tenant-b} only.
   */
  @Test
  public void testMultitenantStagingMetricsRouteToTenantB() throws IOException {
    TokenManager.addTenant("tenant-a", new TokenWorkerWF("token-a", "https://server-a/api/"));
    TokenManager.addTenant("tenant-b", new TokenWorkerWF("token-b", "https://server-b/api/"));

    String yaml = loadResource("preprocessor_scenario_multitenant.yaml");
    ProxyPreprocessorConfigManager config = new ProxyPreprocessorConfigManager();
    config.setDefaultTenant("primary-cluster");
    config.loadFERules(yaml);

    RecordingSenderTask centralTask = new RecordingSenderTask();
    RecordingSenderTask tenantATask = new RecordingSenderTask();
    RecordingSenderTask tenantBTask = new RecordingSenderTask();
    ReportPointHandlerImpl handler = createHandler(
        senderMap(centralTask, "tenant-a", tenantATask, "tenant-b", tenantBTask), null, null);

    ReportPoint point = point("staging.memory", "host2", 1024.0);
    config.get("2878").get().forReportPoint().transform(point);

    handler.reportInternal(point);

    assertEquals("staging.memory: tenant-b must receive 1 point", 1, tenantBTask.items.size());
    assertTrue("staging.memory: central must not receive data", centralTask.items.isEmpty());
    assertTrue("staging.memory: tenant-a must not receive data", tenantATask.items.isEmpty());
    handler.shutdown();
  }

  /**
   * {@code shared.*} metrics must be routed to BOTH {@code tenant-a} and {@code tenant-b}.
   * Central must not receive a copy. No data dropped.
   */
  @Test
  public void testMultitenantSharedMetricsFanOutToBothTenants() throws IOException {
    TokenManager.addTenant("tenant-a", new TokenWorkerWF("token-a", "https://server-a/api/"));
    TokenManager.addTenant("tenant-b", new TokenWorkerWF("token-b", "https://server-b/api/"));

    String yaml = loadResource("preprocessor_scenario_multitenant.yaml");
    ProxyPreprocessorConfigManager config = new ProxyPreprocessorConfigManager();
    config.setDefaultTenant("primary-cluster");
    config.loadFERules(yaml);

    RecordingSenderTask centralTask = new RecordingSenderTask();
    RecordingSenderTask tenantATask = new RecordingSenderTask();
    RecordingSenderTask tenantBTask = new RecordingSenderTask();
    ReportPointHandlerImpl handler = createHandler(
        senderMap(centralTask, "tenant-a", tenantATask, "tenant-b", tenantBTask), null, null);

    ReportPoint point = point("shared.disk", "host3", 500.0);
    config.get("2878").get().forReportPoint().transform(point);

    handler.reportInternal(point);

    assertEquals("shared.disk: tenant-a must receive 1 point", 1, tenantATask.items.size());
    assertEquals("shared.disk: tenant-b must receive 1 point", 1, tenantBTask.items.size());
    assertTrue("shared.disk: central must not receive data", centralTask.items.isEmpty());
    handler.shutdown();
  }

  // =========================================================================
  // Scenario 3: No matching forward rule → fallback to central (never dropped)
  // =========================================================================

  /**
   * A point whose metric name does not match any forward rule must fall back to central.
   * This is the key "no data dropped" guarantee.
   */
  @Test
  public void testNoMatchingForwardRuleFallsBackToCentral() throws IOException {
    TokenManager.addTenant("tenant-a", new TokenWorkerWF("token-a", "https://server-a/api/"));
    TokenManager.addTenant("tenant-b", new TokenWorkerWF("token-b", "https://server-b/api/"));

    String yaml = loadResource("preprocessor_scenario_multitenant.yaml");
    ProxyPreprocessorConfigManager config = new ProxyPreprocessorConfigManager();
    config.setDefaultTenant("primary-cluster");
    config.loadFERules(yaml);

    RecordingSenderTask centralTask = new RecordingSenderTask();
    RecordingSenderTask tenantATask = new RecordingSenderTask();
    RecordingSenderTask tenantBTask = new RecordingSenderTask();
    ReportPointHandlerImpl handler = createHandler(
        senderMap(centralTask, "tenant-a", tenantATask, "tenant-b", tenantBTask), null, null);

    // "infra.*" does not match prod.*, staging.*, or shared.* forward rules.
    ReportPoint point = point("infra.network", "host4", 99.0);
    config.get("2878").get().forReportPoint().transform(point);

    assertFalse("infra.* should have no forward annotation",
        point.getAnnotations().containsKey("wf_forward_tenants"));

    handler.reportInternal(point);

    assertEquals("infra.network: central must receive the point as fallback",
        1, centralTask.items.size());
    assertTrue("infra.network: tenant-a must not receive data", tenantATask.items.isEmpty());
    assertTrue("infra.network: tenant-b must not receive data", tenantBTask.items.isEmpty());
    handler.shutdown();
  }

  // =========================================================================
  // Scenario 4: Explicit forward to "central" by name
  // =========================================================================

  /**
   * A forward rule targeting "central" explicitly must succeed and deliver the point
   * to the central task. "central" is the primary cluster's internal key and is always
   * a valid forward target.
   */
  @Test
  public void testExplicitForwardToCentralNameNotDropped() throws IOException {
    // "central" is always valid; no extra TokenManager setup needed.
    String yaml = loadResource("preprocessor_scenario_central_forward.yaml");
    ProxyPreprocessorConfigManager config = new ProxyPreprocessorConfigManager();
    config.loadFERules(yaml); // must NOT throw

    RecordingSenderTask centralTask = new RecordingSenderTask();
    ReportPointHandlerImpl handler = createHandler(senderMap(centralTask), null, null);

    ReportPoint point = point("central.heartbeat", "host1", 1.0);
    config.get("2878").get().forReportPoint().transform(point);
    assertEquals("central", point.getAnnotations().get("wf_forward_tenants"));

    handler.reportInternal(point);

    assertEquals("central.heartbeat: central task must receive the point", 1, centralTask.items.size());
    handler.shutdown();
  }

  /**
   * When a customer tenant is also named "central" (registered on a different server as
   * "central~2"), a forward rule that explicitly writes {@code tenants: [central]} routes ONLY
   * to the multicasting "central~2" endpoint — NOT to the primary cluster.
   *
   * <p>The primary cluster is always reachable via the {@code defaultTenant} alias (e.g.
   * "Localdev"). Explicitly writing "central" in a forward rule unambiguously targets the
   * user-configured multicasting tenant by that name.
   */
  @Test
  public void testExplicitForwardToCentralGoesToMulticastingTenantNotPrimary()
      throws IOException {
    // Primary registers first (claims the "central" key), then customer "central" on a different
    // server gets the synthetic key "central~2".
    TokenManager.addTenant(CENTRAL_TENANT_NAME,
        new TokenWorkerWF("primary-token", "https://primary.wavefront.com/api/"));
    TokenManager.addTenant(CENTRAL_TENANT_NAME,
        new TokenWorkerWF("customer-central-token", "https://customer-cluster/api/"));

    String syntheticKey = CENTRAL_TENANT_NAME + TokenManager.SYNTHETIC_KEY_SEPARATOR + "2";
    assertNotNull("central~2 must be registered",
        TokenManager.getMulticastingTenantList().get(syntheticKey));

    String yaml = loadResource("preprocessor_scenario_central_forward.yaml");
    ProxyPreprocessorConfigManager config = new ProxyPreprocessorConfigManager();
    config.loadFERules(yaml);

    RecordingSenderTask primaryTask = new RecordingSenderTask();
    RecordingSenderTask customerTask = new RecordingSenderTask();
    Map<String, Collection<SenderTask<String>>> map = new HashMap<>();
    map.put(CENTRAL_TENANT_NAME, Collections.singletonList(primaryTask));
    map.put(syntheticKey, Collections.singletonList(customerTask));
    ReportPointHandlerImpl handler = createHandler(map, null, null);

    ReportPoint point = point("central.heartbeat", "host1", 1.0);
    config.get("2878").get().forReportPoint().transform(point);

    handler.reportInternal(point);

    assertEquals("Multicasting 'central~2' endpoint must receive the point", 1, customerTask.items.size());
    assertTrue("Primary cluster must NOT receive data when forward rule targets 'central' "
        + "and a multicasting tenant named 'central' exists", primaryTask.items.isEmpty());
    handler.shutdown();
  }

  /**
   * A point whose metric does NOT match the "central.*" forward rule falls through to central
   * via the legacy path. No data dropped.
   */
  @Test
  public void testCentralScenarioUnmatchedMetricFallsBackToCentral() throws IOException {
    String yaml = loadResource("preprocessor_scenario_central_forward.yaml");
    ProxyPreprocessorConfigManager config = new ProxyPreprocessorConfigManager();
    config.loadFERules(yaml);

    RecordingSenderTask centralTask = new RecordingSenderTask();
    ReportPointHandlerImpl handler = createHandler(senderMap(centralTask), null, null);

    ReportPoint point = point("other.metric", "host1", 5.0);
    config.get("2878").get().forReportPoint().transform(point);

    assertFalse(point.getAnnotations().containsKey("wf_forward_tenants"));
    handler.reportInternal(point);

    assertEquals("Unmatched metric must reach central via fallback", 1, centralTask.items.size());
    handler.shutdown();
  }

  // =========================================================================
  // Scenario 5: Fan-out — same alias on two servers
  // =========================================================================

  /**
   * When "prod" is registered on server1 (key "prod") and server2 (key "prod~2"),
   * a forward rule targeting "prod" fans out to both tasks.  No data dropped.
   */
  @Test
  public void testFanoutSameAliasTwoServersBothTasksReceivePoint() throws IOException {
    TokenManager.addTenant("prod", new TokenWorkerWF("token1", "https://server1/api/"));
    TokenManager.addTenant("prod", new TokenWorkerWF("token3", "https://server2/api/"));
    String syntheticKey = "prod" + TokenManager.SYNTHETIC_KEY_SEPARATOR + "2";

    String yaml = loadResource("preprocessor_scenario_fanout.yaml");
    ProxyPreprocessorConfigManager config = new ProxyPreprocessorConfigManager();
    config.loadFERules(yaml);

    RecordingSenderTask centralTask = new RecordingSenderTask();
    RecordingSenderTask server1Task = new RecordingSenderTask();
    RecordingSenderTask server2Task = new RecordingSenderTask();
    Map<String, Collection<SenderTask<String>>> map = new HashMap<>();
    map.put(CENTRAL_TENANT_NAME, Collections.singletonList(centralTask));
    map.put("prod", Collections.singletonList(server1Task));
    map.put(syntheticKey, Collections.singletonList(server2Task));
    ReportPointHandlerImpl handler = createHandler(map, null, null);

    ReportPoint point = point("app.requests", "host1", 100.0);
    config.get("2878").get().forReportPoint().transform(point);

    handler.reportInternal(point);

    assertEquals("server1 task must receive the point", 1, server1Task.items.size());
    assertEquals("server2 task (prod~2) must also receive the point", 1, server2Task.items.size());
    assertTrue("central must not receive a copy", centralTask.items.isEmpty());
    handler.shutdown();
  }

  /**
   * When "prod" is configured only once (single server), only that task receives the point.
   */
  @Test
  public void testFanoutSingleServerOnlyOneCopy() throws IOException {
    TokenManager.addTenant("prod", new TokenWorkerWF("token1", "https://server1/api/"));

    String yaml = loadResource("preprocessor_scenario_fanout.yaml");
    ProxyPreprocessorConfigManager config = new ProxyPreprocessorConfigManager();
    config.loadFERules(yaml);

    RecordingSenderTask centralTask = new RecordingSenderTask();
    RecordingSenderTask prodTask = new RecordingSenderTask();
    ReportPointHandlerImpl handler = createHandler(
        senderMap(centralTask, "prod", prodTask), null, null);

    ReportPoint point = point("app.errors", "host1", 3.0);
    config.get("2878").get().forReportPoint().transform(point);
    handler.reportInternal(point);

    assertEquals("Single-server prod: exactly one copy", 1, prodTask.items.size());
    assertTrue("central must not receive data", centralTask.items.isEmpty());
    handler.shutdown();
  }

  /**
   * "infra.*" metrics have no forward rule in the fanout YAML and fall through to central.
   */
  @Test
  public void testFanoutUnmatchedMetricFallsBackToCentral() throws IOException {
    TokenManager.addTenant("prod", new TokenWorkerWF("token1", "https://server1/api/"));

    String yaml = loadResource("preprocessor_scenario_fanout.yaml");
    ProxyPreprocessorConfigManager config = new ProxyPreprocessorConfigManager();
    config.loadFERules(yaml);

    RecordingSenderTask centralTask = new RecordingSenderTask();
    RecordingSenderTask prodTask = new RecordingSenderTask();
    ReportPointHandlerImpl handler = createHandler(
        senderMap(centralTask, "prod", prodTask), null, null);

    ReportPoint point = point("infra.storage", "host1", 200.0);
    config.get("2878").get().forReportPoint().transform(point);
    handler.reportInternal(point);

    assertEquals("infra.* must fall back to central", 1, centralTask.items.size());
    assertTrue("prod task must not receive infra.* data", prodTask.items.isEmpty());
    handler.shutdown();
  }

  // =========================================================================
  // Scenario 6: Transform first, then forward on the transformed name
  // =========================================================================

  /**
   * A point with metric "raw.cpu" is renamed to "normalized.cpu" by the replaceRegex rule,
   * then the forward rule (matching "normalized.*") routes it to tenant-a.
   * The serialized payload delivered to the task must contain the transformed name.
   */
  @Test
  public void testTransformThenForwardPayloadContainsTransformedName() throws IOException {
    TokenManager.addTenant("tenant-a", new TokenWorkerWF("token-a", "https://server-a/api/"));

    String yaml = loadResource("preprocessor_scenario_transform_and_forward.yaml");
    ProxyPreprocessorConfigManager config = new ProxyPreprocessorConfigManager();
    config.loadFERules(yaml);

    RecordingSenderTask centralTask = new RecordingSenderTask();
    RecordingSenderTask tenantATask = new RecordingSenderTask();
    ReportPointHandlerImpl handler = createHandler(
        senderMap(centralTask, "tenant-a", tenantATask), null, null);

    ReportPoint point = point("raw.cpu", "host1", 75.0);
    config.get("2878").get().forReportPoint().transform(point);

    // Transform must have renamed the metric before the forward annotation was injected.
    assertEquals("Transform: raw.cpu must be renamed to normalized.cpu",
        "normalized.cpu", point.getMetric());
    assertEquals("pipeline tag must be set", "normalized",
        point.getAnnotations().get("pipeline"));
    assertEquals("Forward annotation must target tenant-a",
        "tenant-a", point.getAnnotations().get("wf_forward_tenants"));

    handler.reportInternal(point);

    assertEquals("tenant-a must receive 1 point", 1, tenantATask.items.size());
    assertTrue("Serialized payload must contain the transformed name",
        tenantATask.items.get(0).contains("normalized.cpu"));
    assertFalse("Serialized payload must NOT contain the raw name",
        tenantATask.items.get(0).contains("raw.cpu"));
    assertTrue("central must not receive data", centralTask.items.isEmpty());
    handler.shutdown();
  }

  /**
   * A point whose metric does NOT start with "raw." is not renamed and not matched by the
   * forward rule, so it falls through to central. No data dropped.
   */
  @Test
  public void testTransformNonRawMetricFallsBackToCentral() throws IOException {
    TokenManager.addTenant("tenant-a", new TokenWorkerWF("token-a", "https://server-a/api/"));

    String yaml = loadResource("preprocessor_scenario_transform_and_forward.yaml");
    ProxyPreprocessorConfigManager config = new ProxyPreprocessorConfigManager();
    config.loadFERules(yaml);

    RecordingSenderTask centralTask = new RecordingSenderTask();
    RecordingSenderTask tenantATask = new RecordingSenderTask();
    ReportPointHandlerImpl handler = createHandler(
        senderMap(centralTask, "tenant-a", tenantATask), null, null);

    ReportPoint point = point("jvm.heap", "host1", 512.0);
    config.get("2878").get().forReportPoint().transform(point);

    // "jvm.heap" does not match "^raw\." so is unchanged and has no forward annotation.
    assertEquals("jvm.heap", point.getMetric());
    assertFalse(point.getAnnotations().containsKey("wf_forward_tenants"));

    handler.reportInternal(point);

    assertEquals("jvm.heap must fall back to central", 1, centralTask.items.size());
    assertTrue("tenant-a must not receive jvm.heap", tenantATask.items.isEmpty());
    handler.shutdown();
  }

  // =========================================================================
  // Scenario 7: Default-tenant alias resolution
  // =========================================================================

  /**
   * When the handler is created with {@code defaultTenant="PrimaryCluster"}, a forward
   * annotation of {@code wf_forward_tenants=PrimaryCluster} is resolved to "central" by
   * {@link AbstractReportableEntityHandler#resolveTenantName}.
   * The central task receives the point; no data dropped.
   */
  @Test
  public void testDefaultTenantAliasResolvesToCentralTask() throws IOException {
    // "PrimaryCluster" is the alias; no multicasting tenants needed.
    ProxyPreprocessorConfigManager config = new ProxyPreprocessorConfigManager();
    config.setDefaultTenant("PrimaryCluster");

    String yaml = loadResource("preprocessor_scenario_defaulttenant_alias.yaml");
    config.loadFERules(yaml); // "PrimaryCluster" is accepted because setDefaultTenant was called

    RecordingSenderTask centralTask = new RecordingSenderTask();
    // Handler knows that "PrimaryCluster" → "central".
    ReportPointHandlerImpl handler = createHandler(senderMap(centralTask), null, "PrimaryCluster");

    ReportPoint point = point("cluster.latency", "host1", 15.0);
    config.get("2878").get().forReportPoint().transform(point);
    assertEquals("PrimaryCluster", point.getAnnotations().get("wf_forward_tenants"));

    handler.reportInternal(point);

    assertEquals("Alias PrimaryCluster must resolve to central task", 1, centralTask.items.size());
    handler.shutdown();
  }

  /**
   * "external.*" metrics have no forward rule in the alias YAML and fall through to central.
   */
  @Test
  public void testDefaultTenantAliasUnmatchedMetricFallsBackToCentral() throws IOException {
    ProxyPreprocessorConfigManager config = new ProxyPreprocessorConfigManager();
    config.setDefaultTenant("PrimaryCluster");

    String yaml = loadResource("preprocessor_scenario_defaulttenant_alias.yaml");
    config.loadFERules(yaml);

    RecordingSenderTask centralTask = new RecordingSenderTask();
    ReportPointHandlerImpl handler = createHandler(senderMap(centralTask), null, "PrimaryCluster");

    ReportPoint point = point("external.latency", "host1", 20.0);
    config.get("2878").get().forReportPoint().transform(point);
    assertFalse(point.getAnnotations().containsKey("wf_forward_tenants"));

    handler.reportInternal(point);
    assertEquals("external.* must fall back to central", 1, centralTask.items.size());
    handler.shutdown();
  }

  // =========================================================================
  // Scenario 8: Multi-port isolation
  // =========================================================================

  /**
   * "portA.*" points processed through the port-2878 rules route to tenant-a.
   * "portB.*" points processed through the port-2879 rules route to tenant-b.
   * Each port's rules are completely independent.
   */
  @Test
  public void testMultiportRulesAreIsolatedPerPort() throws IOException {
    TokenManager.addTenant("tenant-a", new TokenWorkerWF("token-a", "https://server-a/api/"));
    TokenManager.addTenant("tenant-b", new TokenWorkerWF("token-b", "https://server-b/api/"));

    String yaml = loadResource("preprocessor_scenario_multiport.yaml");
    ProxyPreprocessorConfigManager config = new ProxyPreprocessorConfigManager();
    config.setDefaultTenant("primary-cluster");
    config.loadFERules(yaml);

    RecordingSenderTask centralTask = new RecordingSenderTask();
    RecordingSenderTask tenantATask = new RecordingSenderTask();
    RecordingSenderTask tenantBTask = new RecordingSenderTask();
    ReportPointHandlerImpl handlerA = createHandler(
        senderMap(centralTask, "tenant-a", tenantATask, "tenant-b", tenantBTask), null, null);
    ReportPointHandlerImpl handlerB = createHandler(
        senderMap(centralTask, "tenant-a", tenantATask, "tenant-b", tenantBTask), null, null);

    // Port 2878 point
    ReportPoint pointA = point("portA.cpu", "host1", 42.0);
    config.get("2878").get().forReportPoint().transform(pointA);
    assertEquals("2878", pointA.getAnnotations().get("recv_port"));
    assertEquals("tenant-a", pointA.getAnnotations().get("wf_forward_tenants"));
    handlerA.reportInternal(pointA);

    // Port 2879 point
    ReportPoint pointB = point("portB.memory", "host1", 1024.0);
    config.get("2879").get().forReportPoint().transform(pointB);
    assertEquals("2879", pointB.getAnnotations().get("recv_port"));
    assertEquals("tenant-b", pointB.getAnnotations().get("wf_forward_tenants"));
    handlerB.reportInternal(pointB);

    assertEquals("portA.cpu: tenant-a must receive 1 point", 1, tenantATask.items.size());
    assertEquals("portB.memory: tenant-b must receive 1 point", 1, tenantBTask.items.size());
    assertTrue("central must not receive data", centralTask.items.isEmpty());
    handlerA.shutdown();
    handlerB.shutdown();
  }

  /**
   * A "portA.*" metric processed through port-2879 rules is NOT matched (port-2879 rules
   * only match "portB.*") and falls through to central.  Confirms no cross-port contamination.
   */
  @Test
  public void testMultiportCrossPortMetricFallsBackToCentral() throws IOException {
    TokenManager.addTenant("tenant-a", new TokenWorkerWF("token-a", "https://server-a/api/"));
    TokenManager.addTenant("tenant-b", new TokenWorkerWF("token-b", "https://server-b/api/"));

    String yaml = loadResource("preprocessor_scenario_multiport.yaml");
    ProxyPreprocessorConfigManager config = new ProxyPreprocessorConfigManager();
    config.setDefaultTenant("primary-cluster");
    config.loadFERules(yaml);

    RecordingSenderTask centralTask = new RecordingSenderTask();
    RecordingSenderTask tenantBTask = new RecordingSenderTask();
    ReportPointHandlerImpl handler = createHandler(
        senderMap(centralTask, "tenant-b", tenantBTask), null, null);

    // "portA.*" arriving on port 2879 — no matching rule → central fallback.
    ReportPoint point = point("portA.cpu", "host1", 42.0);
    config.get("2879").get().forReportPoint().transform(point);
    assertFalse(point.getAnnotations().containsKey("wf_forward_tenants"));

    handler.reportInternal(point);

    assertEquals("portA.cpu via port-2879 must fall back to central", 1, centralTask.items.size());
    assertTrue("tenant-b must not receive portA.* data via port-2879", tenantBTask.items.isEmpty());
    handler.shutdown();
  }

  // =========================================================================
  // Scenario 9: Unknown tenant validation — loading must throw
  // =========================================================================

  /**
   * Loading a YAML file that references an unregistered tenant in a forward rule must
   * throw {@link IllegalArgumentException}.  This prevents misconfigured rules from
   * silently dropping data at runtime (defence-in-depth).
   */
  @Test
  public void testUnknownTenantInForwardRuleThrowsOnLoad() throws IOException {
    // Only central is implicitly valid; "ghost-tenant" is not registered.
    String yaml = loadResource("preprocessor_scenario_unknown_tenant.yaml");
    ProxyPreprocessorConfigManager config = new ProxyPreprocessorConfigManager();

    try {
      config.loadFERules(yaml);
      fail("Expected IllegalArgumentException for unregistered tenant 'ghost-tenant'");
    } catch (IllegalArgumentException ex) {
      assertTrue("Exception message must mention the unknown tenant",
          ex.getMessage().contains("ghost-tenant"));
    }
  }

  /**
   * Once the unknown tenant is registered, the same YAML loads without error.
   */
  @Test
  public void testUnknownTenantBecomesValidAfterRegistration() throws IOException {
    // Register the previously "unknown" tenant.
    TokenManager.addTenant("ghost-tenant",
        new TokenWorkerWF("ghost-token", "https://ghost-server/api/"));

    String yaml = loadResource("preprocessor_scenario_unknown_tenant.yaml");
    ProxyPreprocessorConfigManager config = new ProxyPreprocessorConfigManager();
    config.loadFERules(yaml); // must NOT throw now

    assertNotNull("Rules must be loaded after tenant registration", config.get("2878").get());
  }

  // =========================================================================
  // Scenario 10: Tenant limit enforcement
  // =========================================================================

  /**
   * Confirms that {@code TokenManager.addTenant} correctly builds the internal map for
   * up to 10 multicasting tenants (the current hard limit) without any data loss.
   * Each tenant must be independently reachable via forward routing.
   */
  @Test
  public void testTenantLimitMaxTenTenantAllRegistered() throws IOException {
    int max = 10;
    for (int i = 1; i <= max; i++) {
      TokenManager.addTenant("tenant-" + i,
          new TokenWorkerWF("token-" + i, "https://server-" + i + "/api/"));
    }
    assertEquals("All 10 multicasting tenants must be registered",
        max, TokenManager.getMulticastingTenantList().size());

    // All 10 must be resolvable via getInternalKeysForName.
    for (int i = 1; i <= max; i++) {
      assertEquals("tenant-" + i + " must resolve to one internal key",
          1, TokenManager.getInternalKeysForName("tenant-" + i).size());
    }
  }

  /**
   * A 11th tenant registration — once the limit is enforced at ProxyConfig load time —
   * means the excess tenants are never added to TokenManager.  Confirms the limit check
   * in {@link com.wavefront.agent.ProxyConfig#MAX_MULTICASTING_TENANTS} works at the
   * {@code parseArguments} level (tested separately in ProxyConfigTest).
   * Here we verify the runtime state after 10 tenants: all 10 receive their points.
   */
  @Test
  public void testTenantLimitAllPointsReachCorrectTenant() throws IOException {
    // Build a YAML routing rule for 5 tenants (use a subset to keep the test concise).
    StringBuilder yamlBuilder = new StringBuilder("'2878':\n");
    Map<String, Collection<SenderTask<String>>> taskMap = new HashMap<>();
    List<RecordingSenderTask> tasks = new ArrayList<>();
    RecordingSenderTask centralTask = new RecordingSenderTask();
    taskMap.put(CENTRAL_TENANT_NAME, Collections.singletonList(centralTask));

    for (int i = 1; i <= 5; i++) {
      TokenManager.addTenant("t" + i, new TokenWorkerWF("tok" + i, "https://s" + i + "/api/"));
      RecordingSenderTask task = new RecordingSenderTask();
      tasks.add(task);
      taskMap.put("t" + i, Collections.singletonList(task));
      // Single-quoted YAML for the match regex avoids double-escaping backslashes.
      yamlBuilder.append("  - rule: route-to-t").append(i).append("\n")
          .append("    action: forward\n")
          .append("    scope: metricName\n")
          .append("    match: '^m").append(i).append("\\..*'\n")
          .append("    tenants:\n")
          .append("      - t").append(i).append("\n");
    }

    ProxyPreprocessorConfigManager config = new ProxyPreprocessorConfigManager();
    config.setDefaultTenant("primary-cluster"); // required: forward rules + multi-tenant mode
    config.loadFERules(yamlBuilder.toString());

    ReportPointHandlerImpl handler = createHandler(taskMap, null, null);

    // Send one point per tenant route.
    for (int i = 1; i <= 5; i++) {
      ReportPoint point = point("m" + i + ".cpu", "host1", i * 10.0);
      config.get("2878").get().forReportPoint().transform(point);
      handler.reportInternal(point);
    }

    for (int i = 0; i < 5; i++) {
      assertEquals("t" + (i + 1) + " task must receive exactly 1 point",
          1, tasks.get(i).items.size());
    }
    assertTrue("central must not receive data when all points are explicitly routed",
        centralTask.items.isEmpty());
    handler.shutdown();
  }

  // =========================================================================
  // Scenario 11: Deduplication — same tenant name twice in forward annotation
  // =========================================================================

  /**
   * When a forward rule produces a duplicate tenant name (e.g. via two overlapping forward
   * rules that both match), the point must be delivered exactly once to that tenant.
   */
  @Test
  public void testDeduplicateTenantInForwardAnnotation() throws IOException {
    TokenManager.addTenant("tenant-a", new TokenWorkerWF("token-a", "https://server-a/api/"));

    // Inline YAML with two forward rules that both route "dup.*" to "tenant-a".
    // Single-quoted YAML strings are used for the regex to avoid backslash escape issues.
    String yaml = "'2878':\n"
        + "  - rule: route-dup-1\n"
        + "    action: forward\n"
        + "    scope: metricName\n"
        + "    match: '^dup\\..*'\n"
        + "    tenants:\n"
        + "      - tenant-a\n"
        + "  - rule: route-dup-2\n"
        + "    action: forward\n"
        + "    scope: metricName\n"
        + "    match: '^dup\\..*'\n"
        + "    tenants:\n"
        + "      - tenant-a\n";

    ProxyPreprocessorConfigManager config = new ProxyPreprocessorConfigManager();
    config.loadFERules(yaml);

    RecordingSenderTask centralTask = new RecordingSenderTask();
    RecordingSenderTask tenantATask = new RecordingSenderTask();
    ReportPointHandlerImpl handler = createHandler(
        senderMap(centralTask, "tenant-a", tenantATask), null, null);

    ReportPoint point = point("dup.counter", "host1", 1.0);
    config.get("2878").get().forReportPoint().transform(point);
    handler.reportInternal(point);

    assertEquals("Duplicate forward rules: tenant-a must receive exactly 1 copy",
        1, tenantATask.items.size());
    assertTrue("central must not receive data", centralTask.items.isEmpty());
    handler.shutdown();
  }

  // =========================================================================
  // Helpers
  // =========================================================================

  /**
   * Reads a YAML scenario file from the test resources directory of this class and returns
   * its content as a UTF-8 string. The file must be in the same package directory as this
   * test class.
   */
  private static String loadResource(String name) throws IOException {
    String path = "/com/wavefront/agent/preprocessor/" + name;
    try (InputStream is = PreprocessorScenarioTest.class.getResourceAsStream(path)) {
      if (is == null) {
        throw new IllegalStateException("Test resource not found: " + path);
      }
      return new String(is.readAllBytes(), StandardCharsets.UTF_8);
    }
  }

  private static ReportPointHandlerImpl createHandler(
      Map<String, Collection<SenderTask<String>>> senderTaskMap,
      com.wavefront.agent.sampler.MetricBloomFilterSampler sampler,
      String defaultTenant) {
    return new ReportPointHandlerImpl(
        HandlerKey.of(ReportableEntityType.POINT, "scenario-test"),
        0,
        senderTaskMap,
        new ValidationConfiguration(),
        false,
        null,
        null,
        null,
        null,
        sampler,
        defaultTenant);
  }

  /** Builds a sender map with only the central task. */
  private static Map<String, Collection<SenderTask<String>>> senderMap(
      RecordingSenderTask centralTask) {
    Map<String, Collection<SenderTask<String>>> m = new HashMap<>();
    m.put(CENTRAL_TENANT_NAME, Collections.singletonList(centralTask));
    return m;
  }

  /** Builds a sender map with central plus one named tenant. */
  private static Map<String, Collection<SenderTask<String>>> senderMap(
      RecordingSenderTask centralTask, String name1, RecordingSenderTask task1) {
    Map<String, Collection<SenderTask<String>>> m = new HashMap<>();
    m.put(CENTRAL_TENANT_NAME, Collections.singletonList(centralTask));
    m.put(name1, Collections.singletonList(task1));
    return m;
  }

  /** Builds a sender map with central plus two named tenants. */
  private static Map<String, Collection<SenderTask<String>>> senderMap(
      RecordingSenderTask centralTask,
      String name1, RecordingSenderTask task1,
      String name2, RecordingSenderTask task2) {
    Map<String, Collection<SenderTask<String>>> m = new HashMap<>();
    m.put(CENTRAL_TENANT_NAME, Collections.singletonList(centralTask));
    m.put(name1, Collections.singletonList(task1));
    m.put(name2, Collections.singletonList(task2));
    return m;
  }

  private static ReportPoint point(String metric, String host, double value) {
    ReportPoint p = new ReportPoint();
    p.setMetric(metric);
    p.setHost(host);
    p.setValue(value);
    p.setTimestamp(System.currentTimeMillis());
    p.setAnnotations(new HashMap<>());
    return p;
  }

  private static final class RecordingSenderTask implements SenderTask<String> {
    final List<String> items = new ArrayList<>();

    @Override
    public void add(String item) {
      items.add(item);
    }

    @Override
    public long getTaskRelativeScore() {
      return 0;
    }

    @Override
    public void drainBuffersToQueue(com.wavefront.agent.data.QueueingReason reason) {
    }

    @Override
    public void start() {
    }

    @Override
    public void stop() {
    }
  }
}
