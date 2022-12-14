package com.wavefront.agent;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import com.beust.jcommander.ParameterException;
import com.wavefront.agent.auth.TokenValidationMethod;
import com.wavefront.agent.data.TaskQueueLevel;
import org.junit.Test;

/** @author vasily@wavefront.com */
public class ProxyConfigTest {

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
  public void testTaskQueueLevelParsing() {
    ProxyConfig proxyConfig = new ProxyConfig();
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
