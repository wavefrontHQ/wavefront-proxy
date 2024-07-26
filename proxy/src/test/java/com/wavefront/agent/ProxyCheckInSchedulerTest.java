package com.wavefront.agent;

import static com.wavefront.agent.api.APIContainer.CENTRAL_TENANT_NAME;
import static com.wavefront.common.Utils.getBuildVersion;
import static org.easymock.EasyMock.*;
import static org.junit.Assert.*;

import com.wavefront.agent.api.APIContainer;
import com.wavefront.api.ProxyV2API;
import com.wavefront.api.agent.AgentConfiguration;
import com.wavefront.api.agent.ValidationConfiguration;
import java.net.ConnectException;
import java.net.SocketTimeoutException;
import java.net.UnknownHostException;
import java.util.Collections;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import javax.ws.rs.ClientErrorException;
import javax.ws.rs.ProcessingException;
import javax.ws.rs.ServerErrorException;
import javax.ws.rs.core.Response;
import org.easymock.EasyMock;
import org.junit.Test;

/** @author vasily@wavefront.com */
public class ProxyCheckInSchedulerTest {

  @Test
  public void testNormalCheckin() {
    ProxyConfig proxyConfig = EasyMock.createMock(ProxyConfig.class);
    ProxyV2API proxyV2API = EasyMock.createMock(ProxyV2API.class);
    APIContainer apiContainer = EasyMock.createMock(APIContainer.class);
    reset(proxyConfig, proxyV2API, proxyConfig);
    expect(proxyConfig.getHostname()).andReturn("proxyHost").anyTimes();
    expect(proxyConfig.isEphemeral()).andReturn(true).anyTimes();
    expect(proxyConfig.getAgentMetricsPointTags()).andReturn(Collections.emptyMap()).anyTimes();
    expect(proxyConfig.getProxyname()).andReturn("proxyName").anyTimes();
    expect(proxyConfig.getJsonConfig()).andReturn(null).anyTimes();
    apiContainer.updateLogServerEndpointURLandToken(anyObject(), anyObject());
    expectLastCall().anyTimes();
    String authHeader = "Bearer abcde12345";
    AgentConfiguration returnConfig = new AgentConfiguration();
    returnConfig.setPointsPerBatch(1234567L);
    returnConfig.currentTime = System.currentTimeMillis();
    replay(proxyConfig);
    UUID proxyId = ProxyUtil.getOrCreateProxyId(proxyConfig);
    expect(
            proxyV2API.proxyCheckin(
                eq(proxyId),
                eq(authHeader),
                eq("proxyHost"),
                eq("proxyName"),
                eq(getBuildVersion()),
                anyLong(),
                anyObject(),
                eq(true)))
        .andReturn(returnConfig)
        .once();
    expect(apiContainer.getProxyV2APIForTenant(EasyMock.anyObject(String.class)))
        .andReturn(proxyV2API)
        .anyTimes();
    proxyV2API.proxySaveConfig(eq(proxyId), anyObject());
    expectLastCall().anyTimes();
    replay(proxyV2API, apiContainer);
    ProxyCheckInScheduler scheduler =
        new ProxyCheckInScheduler(
            proxyId,
            proxyConfig,
            apiContainer,
            (tenantName, config) -> assertEquals(1234567L, config.getPointsPerBatch().longValue()),
            () -> {},
            () -> {});
    scheduler.scheduleCheckins();
    verify(proxyConfig, proxyV2API, apiContainer);
    assertEquals(1, scheduler.getSuccessfulCheckinCount());
    scheduler.shutdown();
  }

//  @Test
//  public void testNormalCheckinWithRemoteShutdown() {
//    ProxyConfig proxyConfig = EasyMock.createMock(ProxyConfig.class);
//    ProxyV2API proxyV2API = EasyMock.createMock(ProxyV2API.class);
//    APIContainer apiContainer = EasyMock.createMock(APIContainer.class);
//    TenantInfo token = new TokenWorkerWF("abcde12345", "https://acme.corp/api");
//    TokenManager.addTenant(CENTRAL_TENANT_NAME, token);
//    reset(proxyConfig, proxyV2API, proxyConfig);
//    expect(proxyConfig.getHostname()).andReturn("proxyHost").anyTimes();
//    expect(proxyConfig.isEphemeral()).andReturn(true).anyTimes();
//    expect(proxyConfig.getAgentMetricsPointTags()).andReturn(Collections.emptyMap()).anyTimes();
//    expect(proxyConfig.getProxyname()).andReturn("proxyName").anyTimes();
//    expect(proxyConfig.getJsonConfig()).andReturn(null).anyTimes();
//    apiContainer.updateLogServerEndpointURLandToken(anyObject(), anyObject());
//    expectLastCall().anyTimes();
//    String authHeader = "Bearer abcde12345";
//    AgentConfiguration returnConfig = new AgentConfiguration();
//    returnConfig.setShutOffAgents(true);
//    replay(proxyConfig);
//    UUID proxyId = ProxyUtil.getOrCreateProxyId(proxyConfig);
//    expect(
//            proxyV2API.proxyCheckin(
//                eq(proxyId),
//                eq(authHeader),
//                eq("proxyHost"),
//                eq("proxyName"),
//                eq(getBuildVersion()),
//                anyLong(),
//                anyObject(),
//                eq(true)))
//        .andReturn(returnConfig)
//        .anyTimes();
//    expect(apiContainer.getProxyV2APIForTenant(EasyMock.anyObject(String.class)))
//        .andReturn(proxyV2API)
//        .anyTimes();
//    proxyV2API.proxySaveConfig(eq(proxyId), anyObject());
//    proxyV2API.proxySavePreprocessorRules(eq(proxyId), anyObject());
//    expectLastCall().anyTimes();
//    replay(proxyV2API, apiContainer);
//    AtomicBoolean shutdown = new AtomicBoolean(false);
//    ProxyCheckInScheduler.preprocessorRulesNeedUpdate.set(true);
//    ProxyCheckInScheduler scheduler =
//        new ProxyCheckInScheduler(
//            proxyId,
//            proxyConfig,
//            apiContainer,
//            (tenantName, config) -> {},
//            () -> shutdown.set(true),
//            () -> {});
//    scheduler.updateProxyMetrics();
//    scheduler.updateConfiguration();
//    verify(proxyConfig, proxyV2API, apiContainer);
//    assertTrue(shutdown.get());
//  }

  @Test
  public void testNormalCheckinWithBadConsumer() {
    ProxyConfig proxyConfig = EasyMock.createMock(ProxyConfig.class);
    ProxyV2API proxyV2API = EasyMock.createMock(ProxyV2API.class);
    APIContainer apiContainer = EasyMock.createMock(APIContainer.class);
    TenantInfo token = new TokenWorkerWF("abcde12345", "https://acme.corp/api");
    TokenManager.addTenant(CENTRAL_TENANT_NAME, token);
    reset(proxyConfig, proxyV2API, proxyConfig);
    expect(proxyConfig.getHostname()).andReturn("proxyHost").anyTimes();
    expect(proxyConfig.isEphemeral()).andReturn(true).anyTimes();
    expect(proxyConfig.getAgentMetricsPointTags()).andReturn(Collections.emptyMap()).anyTimes();
    expect(proxyConfig.getProxyname()).andReturn("proxyName").anyTimes();
    expect(proxyConfig.getJsonConfig()).andReturn(null).anyTimes();
    expect(proxyConfig.getLogServerIngestionURL()).andReturn(null);
    expect(proxyConfig.getLogServerIngestionToken()).andReturn(null);
    apiContainer.updateLogServerEndpointURLandToken(anyObject(), anyObject());
    proxyConfig.setEnableHyperlogsConvergedCsp(true);
    proxyConfig.setReceivedLogServerDetails(false);
    expectLastCall().anyTimes();
    String authHeader = "Bearer abcde12345";
    AgentConfiguration returnConfig = new AgentConfiguration();
    replay(proxyConfig);
    UUID proxyId = ProxyUtil.getOrCreateProxyId(proxyConfig);
    expect(
            proxyV2API.proxyCheckin(
                eq(proxyId),
                eq(authHeader),
                eq("proxyHost"),
                eq("proxyName"),
                eq(getBuildVersion()),
                anyLong(),
                anyObject(),
                eq(true)))
        .andReturn(returnConfig)
        .anyTimes();
    expect(apiContainer.getProxyV2APIForTenant(EasyMock.anyObject(String.class)))
        .andReturn(proxyV2API)
        .anyTimes();
    proxyV2API.proxySaveConfig(eq(proxyId), anyObject());
    expectLastCall().anyTimes();
    replay(proxyV2API, apiContainer);
    try {
      ProxyCheckInScheduler scheduler =
          new ProxyCheckInScheduler(
              proxyId,
              proxyConfig,
              apiContainer,
              (tenantName, config) -> {
                throw new NullPointerException("gotcha!");
              },
              () -> {},
              () -> {});
      scheduler.updateProxyMetrics();
      scheduler.updateConfiguration();
      verify(proxyConfig, proxyV2API, apiContainer);
      fail("We're not supposed to get here");
    } catch (NullPointerException e) {
      // NPE caught, we're good
    }
  }

  @Test
  public void testNetworkErrors() {
    ProxyConfig proxyConfig = EasyMock.createMock(ProxyConfig.class);
    ProxyV2API proxyV2API = EasyMock.createMock(ProxyV2API.class);
    APIContainer apiContainer = EasyMock.createMock(APIContainer.class);
    TenantInfo token = new TokenWorkerWF("abcde12345", "https://acme.corp/zzz");
    reset(proxyConfig, proxyV2API, proxyConfig);
    TokenManager.addTenant(CENTRAL_TENANT_NAME, token);
    expect(proxyConfig.getHostname()).andReturn("proxyHost").anyTimes();
    expect(proxyConfig.isEphemeral()).andReturn(true).anyTimes();
    expect(proxyConfig.getAgentMetricsPointTags()).andReturn(Collections.emptyMap()).anyTimes();
    expect(proxyConfig.getProxyname()).andReturn("proxyName").anyTimes();
    expect(proxyConfig.getJsonConfig()).andReturn(null).anyTimes();
    String authHeader = "Bearer abcde12345";
    AgentConfiguration returnConfig = new AgentConfiguration();
    returnConfig.setPointsPerBatch(1234567L);
    replay(proxyConfig);
    UUID proxyId = ProxyUtil.getOrCreateProxyId(proxyConfig);
    expect(apiContainer.getProxyV2APIForTenant(EasyMock.anyObject(String.class)))
        .andReturn(proxyV2API)
        .anyTimes();
    proxyV2API.proxySavePreprocessorRules(eq(proxyId), anyObject());
    expectLastCall().anyTimes();
    expect(
            proxyV2API.proxyCheckin(
                eq(proxyId),
                eq(authHeader),
                eq("proxyHost"),
                eq("proxyName"),
                eq(getBuildVersion()),
                anyLong(),
                anyObject(),
                eq(true)))
        .andThrow(new ProcessingException(new UnknownHostException()))
        .once();
    expect(
            proxyV2API.proxyCheckin(
                eq(proxyId),
                eq(authHeader),
                eq("proxyHost"),
                eq("proxyName"),
                eq(getBuildVersion()),
                anyLong(),
                anyObject(),
                eq(true)))
        .andThrow(new ProcessingException(new SocketTimeoutException()))
        .once();
    expect(
            proxyV2API.proxyCheckin(
                eq(proxyId),
                eq(authHeader),
                eq("proxyHost"),
                eq("proxyName"),
                eq(getBuildVersion()),
                anyLong(),
                anyObject(),
                eq(true)))
        .andThrow(new ProcessingException(new ConnectException()))
        .once();
    expect(
            proxyV2API.proxyCheckin(
                eq(proxyId),
                eq(authHeader),
                eq("proxyHost"),
                eq("proxyName"),
                eq(getBuildVersion()),
                anyLong(),
                anyObject(),
                eq(true)))
        .andThrow(new ProcessingException(new NullPointerException()))
        .once();
    expect(
            proxyV2API.proxyCheckin(
                eq(proxyId),
                eq(authHeader),
                eq("proxyHost"),
                eq("proxyName"),
                eq(getBuildVersion()),
                anyLong(),
                anyObject(),
                eq(true)))
        .andThrow(new NullPointerException())
        .once();
    proxyV2API.proxySaveConfig(eq(proxyId), anyObject());
    expectLastCall().anyTimes();
    replay(proxyV2API, apiContainer);
    ProxyCheckInScheduler scheduler =
        new ProxyCheckInScheduler(
            proxyId,
            proxyConfig,
            apiContainer,
            (tenantName, config) -> fail("We are not supposed to get here"),
            () -> {},
            () -> {});
    scheduler.updateConfiguration();
    scheduler.updateConfiguration();
    scheduler.updateConfiguration();
    scheduler.updateConfiguration();
    verify(proxyConfig, proxyV2API, apiContainer);
  }

  @Test
  public void testHttpErrors() {
    ProxyConfig proxyConfig = EasyMock.createMock(ProxyConfig.class);
    ProxyV2API proxyV2API = EasyMock.createMock(ProxyV2API.class);
    APIContainer apiContainer = EasyMock.createMock(APIContainer.class);
    TenantInfo token = new TokenWorkerWF("abcde12345", "https://acme.corp/zzz");
    TokenManager.addTenant(CENTRAL_TENANT_NAME, token);
    reset(proxyConfig, proxyV2API, proxyConfig);
    expect(proxyConfig.getHostname()).andReturn("proxyHost").anyTimes();
    expect(proxyConfig.isEphemeral()).andReturn(true).anyTimes();
    expect(proxyConfig.getAgentMetricsPointTags()).andReturn(Collections.emptyMap()).anyTimes();
    expect(proxyConfig.getProxyname()).andReturn("proxyName").anyTimes();
    expect(proxyConfig.getJsonConfig()).andReturn(null).anyTimes();
    apiContainer.updateLogServerEndpointURLandToken(anyObject(), anyObject());
    expectLastCall().anyTimes();
    String authHeader = "Bearer abcde12345";
    AgentConfiguration returnConfig = new AgentConfiguration();
    replay(proxyConfig);
    UUID proxyId = ProxyUtil.getOrCreateProxyId(proxyConfig);
    expect(apiContainer.getProxyV2APIForTenant(EasyMock.anyObject(String.class)))
        .andReturn(proxyV2API)
        .anyTimes();
    // we need to allow 1 successful checking to prevent early termination
    expect(
            proxyV2API.proxyCheckin(
                eq(proxyId),
                eq(authHeader),
                eq("proxyHost"),
                eq("proxyName"),
                eq(getBuildVersion()),
                anyLong(),
                anyObject(),
                eq(true)))
        .andReturn(returnConfig)
        .once();
    expect(
            proxyV2API.proxyCheckin(
                eq(proxyId),
                eq(authHeader),
                eq("proxyHost"),
                eq("proxyName"),
                eq(getBuildVersion()),
                anyLong(),
                anyObject(),
                eq(true)))
        .andThrow(new ClientErrorException(Response.status(401).build()))
        .once();
    expect(
            proxyV2API.proxyCheckin(
                eq(proxyId),
                eq(authHeader),
                eq("proxyHost"),
                eq("proxyName"),
                eq(getBuildVersion()),
                anyLong(),
                anyObject(),
                eq(true)))
        .andThrow(new ClientErrorException(Response.status(403).build()))
        .once();
    expect(
            proxyV2API.proxyCheckin(
                eq(proxyId),
                eq(authHeader),
                eq("proxyHost"),
                eq("proxyName"),
                eq(getBuildVersion()),
                anyLong(),
                anyObject(),
                eq(true)))
        .andThrow(new ClientErrorException(Response.status(407).build()))
        .once();
    expect(
            proxyV2API.proxyCheckin(
                eq(proxyId),
                eq(authHeader),
                eq("proxyHost"),
                eq("proxyName"),
                eq(getBuildVersion()),
                anyLong(),
                anyObject(),
                eq(true)))
        .andThrow(new ClientErrorException(Response.status(408).build()))
        .once();
    expect(
            proxyV2API.proxyCheckin(
                eq(proxyId),
                eq(authHeader),
                eq("proxyHost"),
                eq("proxyName"),
                eq(getBuildVersion()),
                anyLong(),
                anyObject(),
                eq(true)))
        .andThrow(new ClientErrorException(Response.status(429).build()))
        .once();
    expect(
            proxyV2API.proxyCheckin(
                eq(proxyId),
                eq(authHeader),
                eq("proxyHost"),
                eq("proxyName"),
                eq(getBuildVersion()),
                anyLong(),
                anyObject(),
                eq(true)))
        .andThrow(new ServerErrorException(Response.status(500).build()))
        .once();
    proxyV2API.proxySavePreprocessorRules(eq(proxyId), anyObject());
    expectLastCall().anyTimes();
    expect(
            proxyV2API.proxyCheckin(
                eq(proxyId),
                eq(authHeader),
                eq("proxyHost"),
                eq("proxyName"),
                eq(getBuildVersion()),
                anyLong(),
                anyObject(),
                eq(true)))
        .andThrow(new ServerErrorException(Response.status(502).build()))
        .once();
    proxyV2API.proxySaveConfig(eq(proxyId), anyObject());
    expectLastCall().anyTimes();
    replay(proxyV2API, apiContainer);
    ProxyCheckInScheduler scheduler =
        new ProxyCheckInScheduler(
            proxyId,
            proxyConfig,
            apiContainer,
            (tenantName, config) -> assertNull(config.getPointsPerBatch()),
            () -> {},
            () -> {});
    scheduler.updateProxyMetrics();
    scheduler.updateConfiguration();
    scheduler.updateProxyMetrics();
    scheduler.updateConfiguration();
    scheduler.updateProxyMetrics();
    scheduler.updateConfiguration();
    scheduler.updateProxyMetrics();
    scheduler.updateConfiguration();
    scheduler.updateProxyMetrics();
    scheduler.updateConfiguration();
    scheduler.updateProxyMetrics();
    scheduler.updateConfiguration();
    scheduler.updateProxyMetrics();
    scheduler.updateConfiguration();
    verify(proxyConfig, proxyV2API, apiContainer);
  }

  @Test
  public void testRetryCheckinOnMisconfiguredUrl() {
    ProxyConfig proxyConfig = EasyMock.createMock(ProxyConfig.class);
    ProxyV2API proxyV2API = EasyMock.createMock(ProxyV2API.class);
    APIContainer apiContainer = EasyMock.createMock(APIContainer.class);
    TenantInfo token = new TokenWorkerWF("abcde12345", "https://acme.corp/zzz");
    TokenManager.addTenant(CENTRAL_TENANT_NAME, token);
    reset(proxyConfig, proxyV2API, proxyConfig);
    expect(proxyConfig.getHostname()).andReturn("proxyHost").anyTimes();
    expect(proxyConfig.isEphemeral()).andReturn(true).anyTimes();
    expect(proxyConfig.getAgentMetricsPointTags()).andReturn(Collections.emptyMap()).anyTimes();
    apiContainer.updateLogServerEndpointURLandToken(anyObject(), anyObject());
    expectLastCall().anyTimes();
    expect(proxyConfig.getProxyname()).andReturn("proxyName").anyTimes();
    expect(proxyConfig.getJsonConfig()).andReturn(null).anyTimes();
    String authHeader = "Bearer abcde12345";
    AgentConfiguration returnConfig = new AgentConfiguration();
    returnConfig.setPointsPerBatch(1234567L);
    replay(proxyConfig);
    UUID proxyId = ProxyUtil.getOrCreateProxyId(proxyConfig);
    expect(
            proxyV2API.proxyCheckin(
                eq(proxyId),
                eq(authHeader),
                eq("proxyHost"),
                eq("proxyName"),
                eq(getBuildVersion()),
                anyLong(),
                anyObject(),
                eq(true)))
        .andThrow(new ClientErrorException(Response.status(404).build()))
        .once();
    expect(apiContainer.getProxyV2APIForTenant(EasyMock.anyObject(String.class)))
        .andReturn(proxyV2API)
        .anyTimes();
    apiContainer.updateServerEndpointURL(CENTRAL_TENANT_NAME, "https://acme.corp/zzz/api/");
    expectLastCall().once();
    expect(
            proxyV2API.proxyCheckin(
                eq(proxyId),
                eq(authHeader),
                eq("proxyHost"),
                eq("proxyName"),
                eq(getBuildVersion()),
                anyLong(),
                anyObject(),
                eq(true)))
        .andReturn(returnConfig)
        .once();
    proxyV2API.proxySaveConfig(eq(proxyId), anyObject());
    proxyV2API.proxySavePreprocessorRules(eq(proxyId), anyObject());
    expectLastCall().anyTimes();
    replay(proxyV2API, apiContainer);
    ProxyCheckInScheduler.preprocessorRulesNeedUpdate.set(true);
    ProxyCheckInScheduler scheduler =
        new ProxyCheckInScheduler(
            proxyId,
            proxyConfig,
            apiContainer,
            (tenantName, config) -> assertEquals(1234567L, config.getPointsPerBatch().longValue()),
            () -> {},
            () -> {});
    verify(proxyConfig, proxyV2API, apiContainer);
  }

  @Test
  public void testRetryCheckinOnMisconfiguredUrlFailsTwiceTerminates() {
    ProxyConfig proxyConfig = EasyMock.createMock(ProxyConfig.class);
    ProxyV2API proxyV2API = EasyMock.createMock(ProxyV2API.class);
    APIContainer apiContainer = EasyMock.createMock(APIContainer.class);
    TenantInfo token = new TokenWorkerWF("abcde12345", "https://acme.corp/zzz");
    TokenManager.addTenant(CENTRAL_TENANT_NAME, token);
    reset(proxyConfig, proxyV2API, proxyConfig);
    expect(proxyConfig.getHostname()).andReturn("proxyHost").anyTimes();
    expect(proxyConfig.isEphemeral()).andReturn(true).anyTimes();
    expect(proxyConfig.getAgentMetricsPointTags()).andReturn(Collections.emptyMap()).anyTimes();
    expect(proxyConfig.getProxyname()).andReturn("proxyName").anyTimes();
    expect(proxyConfig.getJsonConfig()).andReturn(null).anyTimes();
    String authHeader = "Bearer abcde12345";
    AgentConfiguration returnConfig = new AgentConfiguration();
    returnConfig.setPointsPerBatch(1234567L);
    replay(proxyConfig);
    UUID proxyId = ProxyUtil.getOrCreateProxyId(proxyConfig);
    expect(
            proxyV2API.proxyCheckin(
                eq(proxyId),
                eq(authHeader),
                eq("proxyHost"),
                eq("proxyName"),
                eq(getBuildVersion()),
                anyLong(),
                anyObject(),
                eq(true)))
        .andThrow(new ClientErrorException(Response.status(404).build()))
        .times(2);
    expect(apiContainer.getProxyV2APIForTenant(EasyMock.anyObject(String.class)))
        .andReturn(proxyV2API)
        .anyTimes();
    proxyV2API.proxySaveConfig(eq(proxyId), anyObject());
    expectLastCall().anyTimes();
    apiContainer.updateServerEndpointURL(CENTRAL_TENANT_NAME, "https://acme.corp/zzz/api/");
    expectLastCall().once();
    replay(proxyV2API, apiContainer);
    try {
      ProxyCheckInScheduler scheduler =
          new ProxyCheckInScheduler(
              proxyId,
              proxyConfig,
              apiContainer,
              (tenantName, config) -> fail("We are not supposed to get here"),
              () -> {},
              () -> {});
      fail();
    } catch (RuntimeException e) {
      //
    }
    verify(proxyConfig, proxyV2API, apiContainer);
  }

  @Test
  public void testDontRetryCheckinOnMisconfiguredUrlThatEndsWithApi() {
    ProxyConfig proxyConfig = EasyMock.createMock(ProxyConfig.class);
    ProxyV2API proxyV2API = EasyMock.createMock(ProxyV2API.class);
    APIContainer apiContainer = EasyMock.createMock(APIContainer.class);
    TenantInfo token = new TokenWorkerWF("abcde12345", "https://acme.corp/api");
    TokenManager.addTenant(CENTRAL_TENANT_NAME, token);
    reset(proxyConfig, proxyV2API, proxyConfig);
    expect(proxyConfig.getHostname()).andReturn("proxyHost").anyTimes();
    expect(proxyConfig.isEphemeral()).andReturn(true).anyTimes();
    expect(proxyConfig.getAgentMetricsPointTags()).andReturn(Collections.emptyMap()).anyTimes();
    expect(proxyConfig.getProxyname()).andReturn("proxyName").anyTimes();
    expect(proxyConfig.getJsonConfig()).andReturn(null).anyTimes();
    String authHeader = "Bearer abcde12345";
    AgentConfiguration returnConfig = new AgentConfiguration();
    returnConfig.setPointsPerBatch(1234567L);
    replay(proxyConfig);
    UUID proxyId = ProxyUtil.getOrCreateProxyId(proxyConfig);
    expect(apiContainer.getProxyV2APIForTenant(EasyMock.anyObject(String.class)))
        .andReturn(proxyV2API)
        .anyTimes();
    expect(
            proxyV2API.proxyCheckin(
                eq(proxyId),
                eq(authHeader),
                eq("proxyHost"),
                eq("proxyName"),
                eq(getBuildVersion()),
                anyLong(),
                anyObject(),
                eq(true)))
        .andThrow(new ClientErrorException(Response.status(404).build()))
        .once();
    replay(proxyV2API, apiContainer);
    try {
      ProxyCheckInScheduler scheduler =
          new ProxyCheckInScheduler(
              proxyId,
              proxyConfig,
              apiContainer,
              (tenantName, config) -> fail("We are not supposed to get here"),
              () -> {},
              () -> {});
      fail();
    } catch (RuntimeException e) {
      //
    }
    verify(proxyConfig, proxyV2API, apiContainer);
  }

  @Test
  public void testDontRetryCheckinOnBadCredentials() {
    ProxyConfig proxyConfig = EasyMock.createMock(ProxyConfig.class);
    ProxyV2API proxyV2API = EasyMock.createMock(ProxyV2API.class);
    APIContainer apiContainer = EasyMock.createMock(APIContainer.class);
    TenantInfo token = new TokenWorkerWF("abcde12345", "https://acme.corp/api");
    TokenManager.addTenant(CENTRAL_TENANT_NAME, token);
    reset(proxyConfig, proxyV2API, proxyConfig);
    expect(proxyConfig.getHostname()).andReturn("proxyHost").anyTimes();
    expect(proxyConfig.isEphemeral()).andReturn(true).anyTimes();
    expect(proxyConfig.getAgentMetricsPointTags()).andReturn(Collections.emptyMap()).anyTimes();
    expect(proxyConfig.getProxyname()).andReturn("proxyName").anyTimes();
    expect(proxyConfig.getJsonConfig()).andReturn(null).anyTimes();
    String authHeader = "Bearer abcde12345";
    AgentConfiguration returnConfig = new AgentConfiguration();
    returnConfig.setPointsPerBatch(1234567L);
    replay(proxyConfig);
    UUID proxyId = ProxyUtil.getOrCreateProxyId(proxyConfig);
    expect(apiContainer.getProxyV2APIForTenant(EasyMock.anyObject(String.class)))
        .andReturn(proxyV2API)
        .anyTimes();
    expect(
            proxyV2API.proxyCheckin(
                eq(proxyId),
                eq(authHeader),
                eq("proxyHost"),
                eq("proxyName"),
                eq(getBuildVersion()),
                anyLong(),
                anyObject(),
                eq(true)))
        .andThrow(new ClientErrorException(Response.status(401).build()))
        .once();
    replay(proxyV2API, apiContainer);
    try {
      ProxyCheckInScheduler scheduler =
          new ProxyCheckInScheduler(
              proxyId,
              proxyConfig,
              apiContainer,
              (tenantName, config) -> fail("We are not supposed to get here"),
              () -> {},
              () -> {});
      fail("We're not supposed to get here");
    } catch (RuntimeException e) {
      //
    }
    verify(proxyConfig, proxyV2API, apiContainer);
  }

  @Test
  public void testCheckinConvergedCSPWithLogServerConfiguration() {
    ProxyConfig proxyConfig = EasyMock.createMock(ProxyConfig.class);
    ProxyV2API proxyV2API = EasyMock.createMock(ProxyV2API.class);
    APIContainer apiContainer = EasyMock.createMock(APIContainer.class);
    TenantInfo token = new TokenWorkerWF("abcde12345", "https://acme.corp/api");
    TokenManager.addTenant(CENTRAL_TENANT_NAME, token);
    reset(proxyConfig, proxyV2API, proxyConfig);
    expect(proxyConfig.getHostname()).andReturn("proxyHost").anyTimes();
    expect(proxyConfig.isEphemeral()).andReturn(true).anyTimes();
    expect(proxyConfig.getAgentMetricsPointTags()).andReturn(Collections.emptyMap()).anyTimes();
    expect(proxyConfig.getProxyname()).andReturn("proxyName").anyTimes();
    expect(proxyConfig.getJsonConfig()).andReturn(null).anyTimes();
    expect(proxyConfig.getLogServerIngestionURL()).andReturn("vRLIC-URL");
    expect(proxyConfig.getLogServerIngestionToken()).andReturn("vRLIC-token");
    apiContainer.updateLogServerEndpointURLandToken(anyObject(), anyObject());
    proxyConfig.setEnableHyperlogsConvergedCsp(true);
    expectLastCall().anyTimes();
    String authHeader = "Bearer abcde12345";
    AgentConfiguration returnConfig = new AgentConfiguration();
    returnConfig.setPointsPerBatch(1234567L);
    returnConfig.currentTime = System.currentTimeMillis();
    ValidationConfiguration validationConfiguration = new ValidationConfiguration();
    validationConfiguration.setEnableHyperlogsConvergedCsp(true);
    returnConfig.setValidationConfiguration(validationConfiguration);
    replay(proxyConfig);
    UUID proxyId = ProxyUtil.getOrCreateProxyId(proxyConfig);
    expect(
            proxyV2API.proxyCheckin(
                eq(proxyId),
                eq(authHeader),
                eq("proxyHost"),
                eq("proxyName"),
                eq(getBuildVersion()),
                anyLong(),
                anyObject(),
                eq(true)))
        .andReturn(returnConfig)
        .once();
    expect(apiContainer.getProxyV2APIForTenant(EasyMock.anyObject(String.class)))
        .andReturn(proxyV2API)
        .anyTimes();
    proxyV2API.proxySaveConfig(eq(proxyId), anyObject());
    expectLastCall().anyTimes();
    replay(proxyV2API, apiContainer);
    ProxyCheckInScheduler scheduler =
        new ProxyCheckInScheduler(
            proxyId,
            proxyConfig,
            apiContainer,
            (tenantName, config) -> assertEquals(1234567L, config.getPointsPerBatch().longValue()),
            () -> {},
            () -> {});
    scheduler.scheduleCheckins();
    verify(proxyConfig, proxyV2API, apiContainer);
    assertEquals(1, scheduler.getSuccessfulCheckinCount());
    scheduler.shutdown();
  }

  @Test
  public void testCheckinConvergedCSPWithoutLogServerConfiguration() {
    ProxyConfig proxyConfig = EasyMock.createMock(ProxyConfig.class);
    ProxyV2API proxyV2API = EasyMock.createMock(ProxyV2API.class);
    APIContainer apiContainer = EasyMock.createMock(APIContainer.class);
    TenantInfo token = new TokenWorkerWF("abcde12345", "https://acme.corp/api");
    TokenManager.addTenant(CENTRAL_TENANT_NAME, token);
    reset(proxyConfig, proxyV2API, proxyConfig);
    expect(proxyConfig.getHostname()).andReturn("proxyHost").anyTimes();
    expect(proxyConfig.isEphemeral()).andReturn(true).anyTimes();
    expect(proxyConfig.getAgentMetricsPointTags()).andReturn(Collections.emptyMap()).anyTimes();
    expect(proxyConfig.getProxyname()).andReturn("proxyName").anyTimes();
    expect(proxyConfig.getJsonConfig()).andReturn(null).anyTimes();
    expect(proxyConfig.getLogServerIngestionURL()).andReturn(null);
    expect(proxyConfig.getLogServerIngestionToken()).andReturn(null);
    apiContainer.updateLogServerEndpointURLandToken(anyObject(), anyObject());
    proxyConfig.setEnableHyperlogsConvergedCsp(true);
    proxyConfig.setReceivedLogServerDetails(false);
    expectLastCall().anyTimes();
    String authHeader = "Bearer abcde12345";
    AgentConfiguration returnConfig = new AgentConfiguration();
    returnConfig.setPointsPerBatch(1234567L);
    returnConfig.currentTime = System.currentTimeMillis();
    ValidationConfiguration validationConfiguration = new ValidationConfiguration();
    validationConfiguration.setEnableHyperlogsConvergedCsp(true);
    returnConfig.setValidationConfiguration(validationConfiguration);
    replay(proxyConfig);
    UUID proxyId = ProxyUtil.getOrCreateProxyId(proxyConfig);
    expect(
            proxyV2API.proxyCheckin(
                eq(proxyId),
                eq(authHeader),
                eq("proxyHost"),
                eq("proxyName"),
                eq(getBuildVersion()),
                anyLong(),
                anyObject(),
                eq(true)))
        .andReturn(returnConfig)
        .once();
    expect(apiContainer.getProxyV2APIForTenant(EasyMock.anyObject(String.class)))
        .andReturn(proxyV2API)
        .anyTimes();
    proxyV2API.proxySaveConfig(eq(proxyId), anyObject());
    expectLastCall().anyTimes();
    replay(proxyV2API, apiContainer);
    ProxyCheckInScheduler scheduler =
        new ProxyCheckInScheduler(
            proxyId,
            proxyConfig,
            apiContainer,
            (tenantName, config) -> assertEquals(1234567L, config.getPointsPerBatch().longValue()),
            () -> {},
            () -> {});
    scheduler.scheduleCheckins();
    verify(proxyConfig, proxyV2API, apiContainer);
    assertEquals(1, scheduler.getSuccessfulCheckinCount());
    scheduler.shutdown();
  }
}
