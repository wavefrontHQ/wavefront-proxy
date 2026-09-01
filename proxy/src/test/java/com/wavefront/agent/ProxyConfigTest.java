package com.wavefront.agent;

import static com.wavefront.agent.ProxyConfig.MAX_MULTICASTING_TENANTS;
import static com.wavefront.agent.api.APIContainer.CENTRAL_TENANT_NAME;
import static org.junit.Assert.*;

import com.beust.jcommander.ParameterException;
import com.wavefront.agent.auth.TokenValidationMethod;
import com.wavefront.agent.data.TaskQueueLevel;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Properties;
import java.util.UUID;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class ProxyConfigTest {

  @Before
  public void setUp() {
    TokenManager.reset();
  }

  @After
  public void tearDown() {
    TokenManager.reset();
  }

  @Test
  public void testArgsAndFile() throws IOException {
    File cfgFile = File.createTempFile("proxy", ".cfg");
    cfgFile.deleteOnExit();

    Properties props = new Properties();
    props.setProperty("pushListenerPorts", "1234");

    FileOutputStream out = new FileOutputStream(cfgFile);
    props.store(out, "");
    out.close();

    String[] args =
        new String[] {
          "-f",
          cfgFile.getAbsolutePath(),
          "--pushListenerPorts",
          "4321",
          "--proxyname",
          "proxyname",
          "--token",
          UUID.randomUUID().toString()
        };

    ProxyConfig cfg = new ProxyConfig();
    assertTrue(cfg.parseArguments(args, ""));
    assertEquals(cfg.getProxyname(), "proxyname");
    assertEquals(cfg.getPushListenerPorts(), "1234");
  }

  @Test
  public void testBadConfig() {
    String[] args =
        new String[] {
          "--token", UUID.randomUUID().toString(),
          "--cspAppId", UUID.randomUUID().toString()
        };
    assertThrows(IllegalArgumentException.class, () -> new ProxyConfig().parseArguments(args, ""));

    String[] args2 =
        new String[] {
          "--token", UUID.randomUUID().toString(),
          "--cspAppSecret", UUID.randomUUID().toString()
        };
    assertThrows(IllegalArgumentException.class, () -> new ProxyConfig().parseArguments(args2, ""));

    String[] args3 =
        new String[] {
          "--token", UUID.randomUUID().toString(),
          "--cspAppId", UUID.randomUUID().toString(),
          "--cspAppSecret", UUID.randomUUID().toString()
        };
    assertThrows(IllegalArgumentException.class, () -> new ProxyConfig().parseArguments(args3, ""));

    String[] args4 =
        new String[] {
          "--cspAPIToken", UUID.randomUUID().toString(),
          "--cspAppId", UUID.randomUUID().toString(),
          "--cspAppSecret", UUID.randomUUID().toString()
        };

    assertThrows(IllegalArgumentException.class, () -> new ProxyConfig().parseArguments(args4, ""));

    String[] args5 =
        new String[] {
          "--token", UUID.randomUUID().toString(),
          "--cspAPIToken", UUID.randomUUID().toString()
        };
    assertThrows(IllegalArgumentException.class, () -> new ProxyConfig().parseArguments(args5, ""));
  }

  @Test
  public void testBadCSPOAuthConfig() {
    String[] args = new String[] {"--cspAppId", UUID.randomUUID().toString()};
    assertThrows(IllegalArgumentException.class, () -> new ProxyConfig().parseArguments(args, ""));

    String[] args2 = new String[] {"--cspAppSecret", UUID.randomUUID().toString()};
    assertThrows(IllegalArgumentException.class, () -> new ProxyConfig().parseArguments(args2, ""));

    String[] args3 =
        new String[] {
          "--token", UUID.randomUUID().toString(),
          "--cspAppId", UUID.randomUUID().toString(),
          "--cspAppSecret", UUID.randomUUID().toString()
        };

    assertThrows(IllegalArgumentException.class, () -> new ProxyConfig().parseArguments(args3, ""));
  }

  @Test
  public void testGoodCSPOAuthConfig() {
    String[] args =
        new String[] {
          "--cspAppId", UUID.randomUUID().toString(),
          "--cspAppSecret", UUID.randomUUID().toString()
        };

    assertTrue(new ProxyConfig().parseArguments(args, ""));
  }

  @Test
  public void testGoodCSPUserConfig() {
    String[] args = new String[] {"--cspAPIToken", UUID.randomUUID().toString()};

    assertTrue(new ProxyConfig().parseArguments(args, ""));
  }

  @Test
  public void testGoodWfTokenConfig() {
    String[] args = new String[] {"--token", UUID.randomUUID().toString()};

    assertTrue(new ProxyConfig().parseArguments(args, ""));
  }

  @Test
  public void testMultiTennat() throws IOException {
    File cfgFile = File.createTempFile("proxy", ".cfg");
    cfgFile.deleteOnExit();

    Properties props = new Properties();
    props.setProperty("pushListenerPorts", "1234");

    props.setProperty("multicastingTenants", "2");

    props.setProperty("multicastingTenantName_1", "name1");
    props.setProperty("multicastingServer_1", "server1");
    props.setProperty("multicastingToken_1", "token1");

    props.setProperty("multicastingTenantName_2", "name2");
    props.setProperty("multicastingServer_2", "server2");
    props.setProperty("multicastingToken_2", "token2");

    FileOutputStream out = new FileOutputStream(cfgFile);
    props.store(out, "");
    out.close();

    String token = UUID.randomUUID().toString();
    String[] args =
        new String[] {
          "-f",
          cfgFile.getAbsolutePath(),
          "--pushListenerPorts",
          "4321",
          "--proxyname",
          "proxyname",
          "--token",
          token
        };

    ProxyConfig cfg = new ProxyConfig();
    assertTrue(cfg.parseArguments(args, ""));

    // default values
    TenantInfo info = TokenManager.getMulticastingTenantList().get(CENTRAL_TENANT_NAME);
    assertNotNull(info);
    assertEquals("http://localhost:8080/api/", info.getWFServer());
    assertEquals(token, info.getBearerToken());

    info = TokenManager.getMulticastingTenantList().get("name1");
    assertNotNull(info);
    assertEquals("server1", info.getWFServer());
    assertEquals("token1", info.getBearerToken());

    info = TokenManager.getMulticastingTenantList().get("name2");
    assertNotNull(info);
    assertEquals("server2", info.getWFServer());
    assertEquals("token2", info.getBearerToken());

    assertNull(TokenManager.getMulticastingTenantList().get("fake"));
  }

  @Test
  public void testVersionOrHelpReturnFalse() {
    assertFalse(new ProxyConfig().parseArguments(new String[] {"--version"}, "PushAgentTest"));
    assertFalse(new ProxyConfig().parseArguments(new String[] {"--help"}, "PushAgentTest"));
    assertTrue(
        new ProxyConfig()
            .parseArguments(
                new String[] {"--token", UUID.randomUUID().toString()}, "PushAgentTest"));
  }

  @Test
  public void testTokenValidationMethodParsing() {
    ProxyConfig proxyConfig = new ProxyConfig();
    proxyConfig.parseArguments(
        new String[] {"--token", UUID.randomUUID().toString()}, "PushAgentTest");

    proxyConfig.parseArguments(new String[] {"--authMethod", "NONE"}, "PushAgentTest");
    assertEquals(proxyConfig.authMethod, TokenValidationMethod.NONE);

    proxyConfig.parseArguments(new String[] {"--authMethod", "STATIC_TOKEN"}, "PushAgentTest");
    assertEquals(proxyConfig.authMethod, TokenValidationMethod.STATIC_TOKEN);

    proxyConfig.parseArguments(new String[] {"--authMethod", "HTTP_GET"}, "PushAgentTest");
    assertEquals(proxyConfig.authMethod, TokenValidationMethod.HTTP_GET);

    proxyConfig.parseArguments(new String[] {"--authMethod", "OAUTH2"}, "PushAgentTest");
    assertEquals(proxyConfig.authMethod, TokenValidationMethod.OAUTH2);

    try {
      proxyConfig.parseArguments(new String[] {"--authMethod", "OTHER"}, "PushAgentTest");
      fail();
    } catch (ParameterException e) {
      // noop
    }

    try {
      proxyConfig.parseArguments(new String[] {"--authMethod", ""}, "PushAgentTest");
      fail();
    } catch (ParameterException e) {
      // noop
    }
  }

  @Test
  public void testTaskQueueLevelParsing() {
    ProxyConfig proxyConfig = new ProxyConfig();
    proxyConfig.parseArguments(
        new String[] {"--token", UUID.randomUUID().toString()}, "PushAgentTest");

    proxyConfig.parseArguments(new String[] {"--taskQueueLevel", "NEVER"}, "PushAgentTest");
    assertEquals(proxyConfig.taskQueueLevel, TaskQueueLevel.NEVER);

    proxyConfig.parseArguments(new String[] {"--taskQueueLevel", "MEMORY"}, "PushAgentTest");
    assertEquals(proxyConfig.taskQueueLevel, TaskQueueLevel.MEMORY);

    proxyConfig.parseArguments(new String[] {"--taskQueueLevel", "PUSHBACK"}, "PushAgentTest");
    assertEquals(proxyConfig.taskQueueLevel, TaskQueueLevel.PUSHBACK);

    proxyConfig.parseArguments(new String[] {"--taskQueueLevel", "ANY_ERROR"}, "PushAgentTest");
    assertEquals(proxyConfig.taskQueueLevel, TaskQueueLevel.ANY_ERROR);

    proxyConfig.parseArguments(new String[] {"--taskQueueLevel", "ALWAYS"}, "PushAgentTest");
    assertEquals(proxyConfig.taskQueueLevel, TaskQueueLevel.ALWAYS);

    try {
      proxyConfig.parseArguments(new String[] {"--taskQueueLevel", "OTHER"}, "PushAgentTest");
      fail();
    } catch (ParameterException e) {
      // noop
    }

    try {
      proxyConfig.parseArguments(new String[] {"--taskQueueLevel", ""}, "PushAgentTest");
      fail();
    } catch (ParameterException e) {
      // noop
    }
  }

  @Test
  public void testOtlpResourceAttrsOnMetricsIncluded() {
    ProxyConfig config = new ProxyConfig();
    config.parseArguments(new String[] {"--token", UUID.randomUUID().toString()}, "PushAgentTest");

    // do not include OTLP resource attributes by default on metrics
    // TODO: find link from OTel GH PR where this choice was made
    assertFalse(config.isOtlpResourceAttrsOnMetricsIncluded());

    // include OTLP resource attributes
    config.parseArguments(
        new String[] {"--otlpResourceAttrsOnMetricsIncluded", String.valueOf(true)},
        "PushAgentTest");
    assertTrue(config.isOtlpResourceAttrsOnMetricsIncluded());
  }

  @Test
  public void testOtlpAppTagsOnMetricsIncluded() {
    ProxyConfig config = new ProxyConfig();
    config.parseArguments(new String[] {"--token", UUID.randomUUID().toString()}, "PushAgentTest");

    // include application, shard, cluster, service.name resource attributes by default on
    // metrics
    assertTrue(config.isOtlpAppTagsOnMetricsIncluded());

    // do not include the above-mentioned resource attributes
    config.parseArguments(
        new String[] {"--otlpAppTagsOnMetricsIncluded", String.valueOf(false)}, "PushAgentTest");
    assertFalse(config.isOtlpAppTagsOnMetricsIncluded());
  }

  @Test
  public void testMetricQuerySamplingDryRun() {
    ProxyConfig config = new ProxyConfig();

    // disabled by default when the parameter is not present
    config.parseArguments(new String[] {"--token", UUID.randomUUID().toString()}, "PushAgentTest");
    assertFalse(config.getMetricQuerySamplingDryRunEnabled());

    // explicitly enable
    config.parseArguments(
        new String[] {"--metricQuerySamplingDryRun", String.valueOf(true)}, "PushAgentTest");
    assertTrue(config.getMetricQuerySamplingDryRunEnabled());

    // explicitly disable
    config.parseArguments(
        new String[] {"--metricQuerySamplingDryRun", String.valueOf(false)}, "PushAgentTest");
    assertFalse(config.getMetricQuerySamplingDryRunEnabled());

    // arity = 1 requires an explicit value; the bare flag is no longer a valid no-arg switch
    try {
      config.parseArguments(new String[] {"--metricQuerySamplingDryRun"}, "PushAgentTest");
      fail();
    } catch (ParameterException e) {
      // noop
    }
  }

  @Test
  public void testDefaultTenantIsNullByDefault() {
    ProxyConfig cfg = new ProxyConfig();
    cfg.parseArguments(new String[] {"--token", UUID.randomUUID().toString()}, "");
    assertNull(
        "defaultTenant must be null when not configured",
        cfg.getDefaultTenant());
  }

  @Test
  public void testDefaultTenantConfiguredViaArgument() {
    ProxyConfig cfg = new ProxyConfig();
    cfg.parseArguments(
        new String[] {"--token", UUID.randomUUID().toString(), "--defaultTenant", "MyCluster"},
        "");
    assertEquals("MyCluster", cfg.getDefaultTenant());
  }

  @Test
  public void testDefaultTenantConfiguredViaFile() throws IOException {
    File cfgFile = File.createTempFile("proxy", ".cfg");
    cfgFile.deleteOnExit();

    Properties props = new Properties();
    props.setProperty("defaultTenant", "PrimaryCluster");

    try (FileOutputStream out = new FileOutputStream(cfgFile)) {
      props.store(out, "");
    }

    ProxyConfig cfg = new ProxyConfig();
    cfg.parseArguments(
        new String[] {"-f", cfgFile.getAbsolutePath(), "--token", UUID.randomUUID().toString()},
        "");
    assertEquals("PrimaryCluster", cfg.getDefaultTenant());
  }

  @Test
  public void testMultiTenantExceedsMaxLimitThrows() throws IOException {
    File cfgFile = File.createTempFile("proxy", ".cfg");
    cfgFile.deleteOnExit();

    Properties props = new Properties();
    props.setProperty("multicastingTenants", String.valueOf(MAX_MULTICASTING_TENANTS + 1));
    for (int i = 1; i <= MAX_MULTICASTING_TENANTS + 1; i++) {
      props.setProperty("multicastingTenantName_" + i, "tenant" + i);
      props.setProperty("multicastingServer_" + i, "https://server" + i + "/api/");
      props.setProperty("multicastingToken_" + i, "token" + i);
    }

    try (FileOutputStream out = new FileOutputStream(cfgFile)) {
      props.store(out, "");
    }

    ProxyConfig cfg = new ProxyConfig();
    assertThrows(
        IllegalArgumentException.class,
        () -> cfg.parseArguments(
            new String[] {"-f", cfgFile.getAbsolutePath(), "--token", UUID.randomUUID().toString()},
            ""));
  }

  @Test
  public void testMultiTenantAtMaxLimitSucceeds() throws IOException {
    File cfgFile = File.createTempFile("proxy", ".cfg");
    cfgFile.deleteOnExit();

    Properties props = new Properties();
    props.setProperty("multicastingTenants", String.valueOf(MAX_MULTICASTING_TENANTS));
    for (int i = 1; i <= MAX_MULTICASTING_TENANTS; i++) {
      props.setProperty("multicastingTenantName_" + i, "tenant" + i);
      props.setProperty("multicastingServer_" + i, "https://server" + i + "/api/");
      props.setProperty("multicastingToken_" + i, "token" + i);
    }

    try (FileOutputStream out = new FileOutputStream(cfgFile)) {
      props.store(out, "");
    }

    ProxyConfig cfg = new ProxyConfig();
    assertTrue(cfg.parseArguments(
        new String[] {"-f", cfgFile.getAbsolutePath(), "--token", UUID.randomUUID().toString()},
        ""));
    // All MAX_MULTICASTING_TENANTS non-central tenants should be registered, plus central.
    assertEquals(MAX_MULTICASTING_TENANTS + 1, TokenManager.getMulticastingTenantList().size());
  }

  /**
   * Same tenant name on DIFFERENT servers = valid cross-cluster scenario.
   * Both endpoints must be registered (under the original name and a synthetic key).
   * Forward routing to "sharedName" fans out to both.
   */
  @Test
  public void testSameTenantNameDifferentServerBothRegistered() throws IOException {
    File cfgFile = File.createTempFile("proxy", ".cfg");
    cfgFile.deleteOnExit();

    Properties props = new Properties();
    props.setProperty("multicastingTenants", "2");
    props.setProperty("multicastingTenantName_1", "sharedName");
    props.setProperty("multicastingServer_1", "https://cluster-a/api/");
    props.setProperty("multicastingToken_1", "token-cluster-a");
    // Same name, different server — should be registered as a second endpoint.
    props.setProperty("multicastingTenantName_2", "sharedName");
    props.setProperty("multicastingServer_2", "https://cluster-b/api/");
    props.setProperty("multicastingToken_2", "token-cluster-b");

    try (FileOutputStream out = new FileOutputStream(cfgFile)) {
      props.store(out, "");
    }

    ProxyConfig cfg = new ProxyConfig();
    assertTrue(cfg.parseArguments(
        new String[] {"-f", cfgFile.getAbsolutePath(), "--token", UUID.randomUUID().toString()},
        ""));

    // central + sharedName (cluster-a) + sharedName~2 (cluster-b) = 3 entries in internal map.
    assertEquals(3, TokenManager.getMulticastingTenantList().size());

    // Original key points to cluster-a.
    TenantInfo clusterA = TokenManager.getMulticastingTenantList().get("sharedName");
    assertNotNull(clusterA);
    assertEquals("token-cluster-a", clusterA.getBearerToken());
    assertEquals("https://cluster-a/api/", clusterA.getWFServer());

    // Synthetic key points to cluster-b.
    String syntheticKey = "sharedName" + TokenManager.SYNTHETIC_KEY_SEPARATOR + "2";
    TenantInfo clusterB = TokenManager.getMulticastingTenantList().get(syntheticKey);
    assertNotNull(clusterB);
    assertEquals("token-cluster-b", clusterB.getBearerToken());
    assertEquals("https://cluster-b/api/", clusterB.getWFServer());

    // Both internal keys are returned for fan-out routing.
    assertEquals(java.util.List.of("sharedName", syntheticKey),
        TokenManager.getInternalKeysForName("sharedName"));

    // Registered aliases contain the original name only, not the synthetic key.
    assertTrue(TokenManager.getRegisteredAliases().contains("sharedName"));
    assertFalse(TokenManager.getRegisteredAliases().contains(syntheticKey));
  }

  /**
   * Same tenant name, SAME server (regardless of token) = duplicate — first wins, second skipped.
   */
  @Test
  public void testSameTenantNameSameServerDuplicateSkipped() throws IOException {
    File cfgFile = File.createTempFile("proxy", ".cfg");
    cfgFile.deleteOnExit();

    Properties props = new Properties();
    props.setProperty("multicastingTenants", "2");
    // Both entries share the same server+name → true duplicate.
    props.setProperty("multicastingTenantName_1", "dupTenant");
    props.setProperty("multicastingServer_1", "https://server-x/api/");
    props.setProperty("multicastingToken_1", "token-a");
    props.setProperty("multicastingTenantName_2", "dupTenant");
    props.setProperty("multicastingServer_2", "https://server-x/api/");
    props.setProperty("multicastingToken_2", "token-b");

    try (FileOutputStream out = new FileOutputStream(cfgFile)) {
      props.store(out, "");
    }

    ProxyConfig cfg = new ProxyConfig();
    assertTrue(cfg.parseArguments(
        new String[] {"-f", cfgFile.getAbsolutePath(), "--token", UUID.randomUUID().toString()},
        ""));

    // Only 2 entries: central + dupTenant (second registration skipped).
    assertEquals(2, TokenManager.getMulticastingTenantList().size());
    TenantInfo kept = TokenManager.getMulticastingTenantList().get("dupTenant");
    assertNotNull(kept);
    assertEquals("token-a", kept.getBearerToken());

    // Only one internal key for "dupTenant".
    assertEquals(java.util.List.of("dupTenant"), TokenManager.getInternalKeysForName("dupTenant"));
  }

  /**
   * Full scenario: same name across two different servers, then a true duplicate on each server.
   * Verifies the exact user-described scenario.
   */
  @Test
  public void testFullMultiServerScenario() throws IOException {
    File cfgFile = File.createTempFile("proxy", ".cfg");
    cfgFile.deleteOnExit();

    Properties props = new Properties();
    props.setProperty("multicastingTenants", "4");
    // server1, tenant1, token1 → REGISTER
    props.setProperty("multicastingTenantName_1", "tenant1");
    props.setProperty("multicastingServer_1", "https://server1/api/");
    props.setProperty("multicastingToken_1", "token1");
    // server1, tenant1, token2 → SKIP (same server+name)
    props.setProperty("multicastingTenantName_2", "tenant1");
    props.setProperty("multicastingServer_2", "https://server1/api/");
    props.setProperty("multicastingToken_2", "token2");
    // server2, tenant1, token3 → REGISTER (different server)
    props.setProperty("multicastingTenantName_3", "tenant1");
    props.setProperty("multicastingServer_3", "https://server2/api/");
    props.setProperty("multicastingToken_3", "token3");
    // server2, tenant1, token1 → SKIP (server2+tenant1 already registered in step 3)
    props.setProperty("multicastingTenantName_4", "tenant1");
    props.setProperty("multicastingServer_4", "https://server2/api/");
    props.setProperty("multicastingToken_4", "token1");

    try (FileOutputStream out = new FileOutputStream(cfgFile)) {
      props.store(out, "");
    }

    ProxyConfig cfg = new ProxyConfig();
    assertTrue(cfg.parseArguments(
        new String[] {"-f", cfgFile.getAbsolutePath(), "--token", UUID.randomUUID().toString()},
        ""));

    // central + tenant1 (server1) + tenant1~2 (server2) = 3 entries; 2 duplicates skipped.
    assertEquals(3, TokenManager.getMulticastingTenantList().size());

    TenantInfo server1Entry = TokenManager.getMulticastingTenantList().get("tenant1");
    assertNotNull(server1Entry);
    assertEquals("https://server1/api/", server1Entry.getWFServer());
    assertEquals("token1", server1Entry.getBearerToken());

    String syntheticKey = "tenant1" + TokenManager.SYNTHETIC_KEY_SEPARATOR + "2";
    TenantInfo server2Entry = TokenManager.getMulticastingTenantList().get(syntheticKey);
    assertNotNull(server2Entry);
    assertEquals("https://server2/api/", server2Entry.getWFServer());
    assertEquals("token3", server2Entry.getBearerToken());

    // Fan-out: "tenant1" resolves to both endpoints.
    assertEquals(java.util.List.of("tenant1", syntheticKey),
        TokenManager.getInternalKeysForName("tenant1"));
  }

  /**
   * A multicasting tenant named "central" must not be rejected even though "central" is the
   * proxy's internal key for the primary cluster.  When the customer's cluster is on a
   * different server, TokenManager registers it under a synthetic key ("central~2") and
   * getInternalKeysForName("central") returns both, enabling fan-out routing.
   */
  @Test
  public void testTenantNamedCentralRegisteredViaSyntheticKey() throws IOException {
    File cfgFile = File.createTempFile("proxy", ".cfg");
    cfgFile.deleteOnExit();

    Properties props = new Properties();
    props.setProperty("multicastingTenants", "1");
    props.setProperty("multicastingTenantName_1", "central"); // same name as the reserved key
    props.setProperty("multicastingServer_1", "https://customer-cluster/api/"); // different server
    props.setProperty("multicastingToken_1", "customer-central-token");

    try (FileOutputStream out = new FileOutputStream(cfgFile)) {
      props.store(out, "");
    }

    String primaryToken = UUID.randomUUID().toString();
    ProxyConfig cfg = new ProxyConfig();
    // Must NOT throw — "central" as a tenant name is now permitted.
    assertTrue(cfg.parseArguments(
        new String[] {"-f", cfgFile.getAbsolutePath(), "--token", primaryToken}, ""));

    // Internal map has: "central" (primary cluster) + "central~2" (customer cluster).
    assertEquals(2, TokenManager.getMulticastingTenantList().size());

    // Original "central" key still points to the primary cluster (registered from --token).
    TenantInfo primaryCluster = TokenManager.getMulticastingTenantList().get(CENTRAL_TENANT_NAME);
    assertNotNull(primaryCluster);
    assertEquals(primaryToken, primaryCluster.getBearerToken());

    // Synthetic key "central~2" points to the customer's cluster.
    String syntheticKey = "central" + TokenManager.SYNTHETIC_KEY_SEPARATOR + "2";
    TenantInfo customerCluster = TokenManager.getMulticastingTenantList().get(syntheticKey);
    assertNotNull(customerCluster);
    assertEquals("customer-central-token", customerCluster.getBearerToken());
    assertEquals("https://customer-cluster/api/", customerCluster.getWFServer());

    // Forward routing to "central" fans out to both endpoints.
    assertEquals(java.util.List.of(CENTRAL_TENANT_NAME, syntheticKey),
        TokenManager.getInternalKeysForName("central"));

    // getRegisteredAliases() returns "central" once (not the synthetic key).
    assertTrue(TokenManager.getRegisteredAliases().contains("central"));
    assertFalse(TokenManager.getRegisteredAliases().contains(syntheticKey));
  }

  @Test
  public void testMultiTenantWithDefaultTenant() throws IOException {
    File cfgFile = File.createTempFile("proxy", ".cfg");
    cfgFile.deleteOnExit();

    Properties props = new Properties();
    props.setProperty("multicastingTenants", "1");
    props.setProperty("multicastingTenantName_1", "sidecar");
    props.setProperty("multicastingServer_1", "https://sidecar.corp/api/");
    props.setProperty("multicastingToken_1", "sidecar-token");
    props.setProperty("defaultTenant", "primary");

    try (FileOutputStream out = new FileOutputStream(cfgFile)) {
      props.store(out, "");
    }

    String token = UUID.randomUUID().toString();
    ProxyConfig cfg = new ProxyConfig();
    assertTrue(
        cfg.parseArguments(
            new String[] {"-f", cfgFile.getAbsolutePath(), "--token", token}, ""));

    assertEquals("primary", cfg.getDefaultTenant());

    // Verify multi-tenant registration is not broken by the defaultTenant field.
    assertNotNull(TokenManager.getMulticastingTenantList().get(CENTRAL_TENANT_NAME));
    assertNotNull(TokenManager.getMulticastingTenantList().get("sidecar"));
    assertEquals("sidecar-token",
        TokenManager.getMulticastingTenantList().get("sidecar").getBearerToken());
  }
}
