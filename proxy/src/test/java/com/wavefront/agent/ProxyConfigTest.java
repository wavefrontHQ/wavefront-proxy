package com.wavefront.agent;

import static org.junit.Assert.*;

import com.beust.jcommander.ParameterException;
import com.wavefront.agent.api.APIContainer;
import com.wavefront.agent.auth.TokenValidationMethod;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Map;
import java.util.Properties;
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
          "-f", cfgFile.getAbsolutePath(),
          "--pushListenerPorts", "4321",
          "--proxyname", "proxyname"
        };

    ProxyConfig cfg = new ProxyConfig();
    assertTrue(cfg.parseArguments(args, ""));
    assertEquals(cfg.getProxyname(), "proxyname");
    assertEquals(cfg.getPushListenerPorts(), "1234");
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

    String[] args =
        new String[] {
          "-f", cfgFile.getAbsolutePath(),
          "--pushListenerPorts", "4321",
          "--proxyname", "proxyname"
        };

    ProxyConfig cfg = new ProxyConfig();
    assertTrue(cfg.parseArguments(args, ""));

    // default values
    Map<String, String> info =
        cfg.getMulticastingTenantList().get(APIContainer.CENTRAL_TENANT_NAME);
    assertNotNull(info);
    assertEquals("http://localhost:8080/api/", info.get(APIContainer.API_SERVER));
    assertEquals("undefined", info.get(APIContainer.API_TOKEN));

    info = cfg.getMulticastingTenantList().get("name1");
    assertNotNull(info);
    assertEquals("server1", info.get(APIContainer.API_SERVER));
    assertEquals("token1", info.get(APIContainer.API_TOKEN));

    info = cfg.getMulticastingTenantList().get("name2");
    assertNotNull(info);
    assertEquals("server2", info.get(APIContainer.API_SERVER));
    assertEquals("token2", info.get(APIContainer.API_TOKEN));

    assertNull(cfg.getMulticastingTenantList().get("fake"));
  }

  @Test
  public void testVersionOrHelpReturnFalse() {
    assertFalse(new ProxyConfig().parseArguments(new String[] {"--version"}, "PushAgentTest"));
    assertFalse(new ProxyConfig().parseArguments(new String[] {"--help"}, "PushAgentTest"));
    assertTrue(new ProxyConfig().parseArguments(new String[] {"--host", "host"}, "PushAgentTest"));
  }

  @Test
  public void testTokenValidationMethodParsing() {
    ProxyConfig proxyConfig = new ProxyConfig();
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
  public void testOtlpResourceAttrsOnMetricsIncluded() {
    ProxyConfig config = new ProxyConfig();

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

    // include application, shard, cluster, service.name resource attributes by default on
    // metrics
    assertTrue(config.isOtlpAppTagsOnMetricsIncluded());

    // do not include the above-mentioned resource attributes
    config.parseArguments(
        new String[] {"--otlpAppTagsOnMetricsIncluded", String.valueOf(false)}, "PushAgentTest");
    assertFalse(config.isOtlpAppTagsOnMetricsIncluded());
  }
}
