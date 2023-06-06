package com.wavefront.agent;

import static org.easymock.EasyMock.*;
import static org.junit.Assert.*;

import com.beust.jcommander.ParameterException;
import com.wavefront.agent.api.APIContainer;
import com.wavefront.agent.auth.TokenValidationMethod;
import com.wavefront.agent.data.TaskQueueLevel;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Properties;
import java.util.UUID;
import org.easymock.EasyMock;
import org.junit.Test;

public class ProxyConfigTest {

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

    ProxyConfig cfg = new ProxyConfig();
    assertThrows(IllegalArgumentException.class, () -> cfg.parseArguments(args, ""));
  }

  public void testBadCSPOAuthConfig() {
    String[] args = new String[] {"--cspAppId", UUID.randomUUID().toString()};

    ProxyConfig cfg = new ProxyConfig();
    assertThrows(IllegalArgumentException.class, () -> cfg.parseArguments(args, ""));
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
    TenantInfo info =
        cfg.getTenantInfoManager()
            .getMulticastingTenantList()
            .get(APIContainer.CENTRAL_TENANT_NAME);
    assertNotNull(info);
    assertEquals("http://localhost:8080/api/", info.getWFServer());
    assertEquals(token, info.getBearerToken());

    info = cfg.getTenantInfoManager().getMulticastingTenantList().get("name1");
    assertNotNull(info);
    assertEquals("server1", info.getWFServer());
    assertEquals("token1", info.getBearerToken());

    info = cfg.getTenantInfoManager().getMulticastingTenantList().get("name2");
    assertNotNull(info);
    assertEquals("server2", info.getWFServer());
    assertEquals("token2", info.getBearerToken());

    assertNull(cfg.getTenantInfoManager().getMulticastingTenantList().get("fake"));
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
  public void testMultiTenantUsingCSPAPIToken() throws IOException {
    TenantInfoManager tenantInfoManager = EasyMock.createMock(TenantInfoManager.class);
    File cfgFile = File.createTempFile("proxy", ".cfg");
    cfgFile.deleteOnExit();

    Properties props = new Properties();
    props.setProperty("pushListenerPorts", "1234");

    props.setProperty("multicastingTenants", "2");

    props.setProperty("multicastingTenantName_1", "name1");
    props.setProperty("multicastingServer_1", "server1");
    props.setProperty("multicastingCSPAPIToken_1", "csp-api-token1");

    props.setProperty("multicastingTenantName_2", "name2");
    props.setProperty("multicastingServer_2", "server2");
    props.setProperty("multicastingCSPAPIToken_2", "csp-api-token2");

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

    ProxyConfig proxyConfig = new ProxyConfig();
    proxyConfig.setTenantInfoManager(tenantInfoManager);

    tenantInfoManager.constructTenantInfoObject(
        "", "", "", "csp-api-token1", "", "server1", "name1");
    expectLastCall();

    tenantInfoManager.constructTenantInfoObject(
        "", "", "", "csp-api-token2", "", "server2", "name2");
    expectLastCall();

    tenantInfoManager.constructTenantInfoObject(
        null, null, null, null, token, proxyConfig.server, APIContainer.CENTRAL_TENANT_NAME);
    expectLastCall();
    replay(tenantInfoManager);

    assertTrue(proxyConfig.parseArguments(args, ""));
    verify(tenantInfoManager);
  }

  @Test
  public void testMultiTenantUsingCSPOAuthApp() throws IOException {
    TenantInfoManager tenantInfoManager = EasyMock.createMock(TenantInfoManager.class);
    File cfgFile = File.createTempFile("proxy", ".cfg");
    cfgFile.deleteOnExit();

    Properties props = new Properties();
    props.setProperty("pushListenerPorts", "1234");

    props.setProperty("multicastingTenants", "2");

    props.setProperty("multicastingTenantName_1", "name1");
    props.setProperty("multicastingServer_1", "server1");
    props.setProperty("multicastingCSPAppId_1", "app-id1");
    props.setProperty("multicastingCSPAppSecret_1", "app-secret1");
    props.setProperty("multicastingCSPOrgId_1", "org-id1");

    props.setProperty("multicastingTenantName_2", "name2");
    props.setProperty("multicastingServer_2", "server2");
    props.setProperty("multicastingCSPAppId_2", "app-id2");
    props.setProperty("multicastingCSPAppSecret_2", "app-secret2");
    props.setProperty("multicastingCSPOrgId_2", "org-id2");

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

    ProxyConfig proxyConfig = new ProxyConfig();
    proxyConfig.setTenantInfoManager(tenantInfoManager);

    tenantInfoManager.constructTenantInfoObject(
        "app-id1", "app-secret1", "org-id1", "", "", "server1", "name1");
    expectLastCall();

    tenantInfoManager.constructTenantInfoObject(
        "app-id2", "app-secret2", "org-id2", "", "", "server2", "name2");
    expectLastCall();

    tenantInfoManager.constructTenantInfoObject(
        null, null, null, null, token, proxyConfig.server, APIContainer.CENTRAL_TENANT_NAME);
    expectLastCall();
    replay(tenantInfoManager);

    assertTrue(proxyConfig.parseArguments(args, ""));
    verify(tenantInfoManager);
  }
}
