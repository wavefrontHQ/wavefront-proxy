package com.wavefront.agent;

import com.beust.jcommander.ParameterException;
import com.wavefront.agent.auth.TokenValidationMethod;
import com.wavefront.agent.data.TaskQueueLevel;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * @author vasily@wavefront.com
 */
public class ProxyConfigTest {

  @Test
  public void testVersionOrHelpReturnFalse() {
    assertFalse(new ProxyConfig().parseArguments(new String[]{"--version"}, "PushAgentTest"));
    assertFalse(new ProxyConfig().parseArguments(new String[]{"--help"}, "PushAgentTest"));
    assertTrue(new ProxyConfig().parseArguments(new String[]{"--host", "host"}, "PushAgentTest"));
  }

  @Test
  public void testTokenValidationMethodParsing() {
    ProxyConfig proxyConfig = new ProxyConfig();
    proxyConfig.parseArguments(new String[]{"--authMethod", "NONE"}, "PushAgentTest");
    assertEquals(proxyConfig.authMethod, TokenValidationMethod.NONE);

    proxyConfig.parseArguments(new String[]{"--authMethod", "STATIC_TOKEN"}, "PushAgentTest");
    assertEquals(proxyConfig.authMethod, TokenValidationMethod.STATIC_TOKEN);

    proxyConfig.parseArguments(new String[]{"--authMethod", "HTTP_GET"}, "PushAgentTest");
    assertEquals(proxyConfig.authMethod, TokenValidationMethod.HTTP_GET);

    proxyConfig.parseArguments(new String[]{"--authMethod", "OAUTH2"}, "PushAgentTest");
    assertEquals(proxyConfig.authMethod, TokenValidationMethod.OAUTH2);

    try {
      proxyConfig.parseArguments(new String[]{"--authMethod", "OTHER"}, "PushAgentTest");
      fail();
    } catch (ParameterException e) {
      // noop
    }

    try {
      proxyConfig.parseArguments(new String[]{"--authMethod", ""}, "PushAgentTest");
      fail();
    } catch (ParameterException e) {
      // noop
    }
  }

  @Test
  public void testTaskQueueLevelParsing() {
    ProxyConfig proxyConfig = new ProxyConfig();
    proxyConfig.parseArguments(new String[]{"--taskQueueLevel", "NEVER"}, "PushAgentTest");
    assertEquals(proxyConfig.taskQueueLevel, TaskQueueLevel.NEVER);

    proxyConfig.parseArguments(new String[]{"--taskQueueLevel", "MEMORY"}, "PushAgentTest");
    assertEquals(proxyConfig.taskQueueLevel, TaskQueueLevel.MEMORY);

    proxyConfig.parseArguments(new String[]{"--taskQueueLevel", "PUSHBACK"}, "PushAgentTest");
    assertEquals(proxyConfig.taskQueueLevel, TaskQueueLevel.PUSHBACK);

    proxyConfig.parseArguments(new String[]{"--taskQueueLevel", "ANY_ERROR"}, "PushAgentTest");
    assertEquals(proxyConfig.taskQueueLevel, TaskQueueLevel.ANY_ERROR);

    proxyConfig.parseArguments(new String[]{"--taskQueueLevel", "ALWAYS"}, "PushAgentTest");
    assertEquals(proxyConfig.taskQueueLevel, TaskQueueLevel.ALWAYS);

    try {
      proxyConfig.parseArguments(new String[]{"--taskQueueLevel", "OTHER"}, "PushAgentTest");
      fail();
    } catch (ParameterException e) {
      // noop
    }

    try {
      proxyConfig.parseArguments(new String[]{"--taskQueueLevel", ""}, "PushAgentTest");
      fail();
    } catch (ParameterException e) {
      // noop
    }
  }
}
