package com.wavefront.agent.api;

import static org.junit.Assert.*;

import com.wavefront.agent.ProxyConfig;
import com.wavefront.agent.TokenManager;
import com.wavefront.agent.TokenWorkerCSP;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

public class APIContainerTest {
  private final int NUM_TENANTS = 5;
  private ProxyConfig proxyConfig;

  @Before
  public void setup() {
    this.proxyConfig = new ProxyConfig();
    TokenWorkerCSP tokenWorkerCSP = new TokenWorkerCSP("fake-token", "fake-url");
    TokenManager.addTenant(APIContainer.CENTRAL_TENANT_NAME, tokenWorkerCSP);
    for (int i = 0; i < NUM_TENANTS; i++) {
      TokenWorkerCSP tokenWorkerCSP1 = new TokenWorkerCSP("fake-token" + i, "fake-url" + i);
      TokenManager.addTenant("tenant-" + i, tokenWorkerCSP1);
    }
  }

  @Test
  public void testAPIContainerInitiationWithDiscardData() {
    APIContainer apiContainer = new APIContainer(this.proxyConfig, true);
    assertEquals(apiContainer.getTenantNameList().size(), 1);
    assertTrue(apiContainer.getProxyV2APIForTenant("central") instanceof NoopProxyV2API);
    assertTrue(apiContainer.getSourceTagAPIForTenant("central") instanceof NoopSourceTagAPI);
    assertTrue(apiContainer.getEventAPIForTenant("central") instanceof NoopEventAPI);
  }

  @Test(expected = IllegalStateException.class)
  public void testUpdateServerEndpointURLWithNullProxyConfig() {
    APIContainer apiContainer = new APIContainer(null, null, null, null, null);
    apiContainer.updateServerEndpointURL("central", "fake-url");
  }

  @Ignore
  @Test
  public void testUpdateServerEndpointURLWithValidProxyConfig() {
    APIContainer apiContainer = new APIContainer(this.proxyConfig, false);
    assertEquals(apiContainer.getTenantNameList().size(), NUM_TENANTS + 1);
    apiContainer.updateServerEndpointURL("central", "another-fake-url");
    assertEquals(apiContainer.getTenantNameList().size(), NUM_TENANTS + 1);
    assertNotNull(apiContainer.getProxyV2APIForTenant("central"));

    apiContainer = new APIContainer(this.proxyConfig, true);
    assertEquals(apiContainer.getTenantNameList().size(), 1);
    apiContainer.updateServerEndpointURL("central", "another-fake-url");
    assertEquals(apiContainer.getTenantNameList().size(), 1);
    assertNotNull(apiContainer.getProxyV2APIForTenant("central"));
    assertTrue(apiContainer.getProxyV2APIForTenant("central") instanceof NoopProxyV2API);
    assertTrue(apiContainer.getSourceTagAPIForTenant("central") instanceof NoopSourceTagAPI);
    assertTrue(apiContainer.getEventAPIForTenant("central") instanceof NoopEventAPI);
  }

  @Test
  public void testUpdateLogServerEndpointURLandToken() {
    APIContainer apiContainer = new APIContainer(this.proxyConfig, false);

    apiContainer.updateLogServerEndpointURLandToken(null, null);
    assertEquals("NOT_SET", apiContainer.getLogServerToken());
    assertEquals("NOT_SET", apiContainer.getLogServerEndpointUrl());

    apiContainer.updateLogServerEndpointURLandToken("", "");
    assertEquals("NOT_SET", apiContainer.getLogServerToken());
    assertEquals("NOT_SET", apiContainer.getLogServerEndpointUrl());

    apiContainer.updateLogServerEndpointURLandToken("testURL", "");
    assertEquals("NOT_SET", apiContainer.getLogServerToken());
    assertEquals("NOT_SET", apiContainer.getLogServerEndpointUrl());

    apiContainer.updateLogServerEndpointURLandToken("testURL", "testToken");
    assertEquals("testToken", apiContainer.getLogServerToken());
    assertEquals("testURL", apiContainer.getLogServerEndpointUrl());

    apiContainer.updateLogServerEndpointURLandToken(
        "https://data.lint-be.symphony-dev.com/le-mans/v1/streams/ingestion-pipeline-stream",
        "testToken");
    assertEquals("testToken", apiContainer.getLogServerToken());
    assertEquals("https://data.lint-be.symphony-dev.com/", apiContainer.getLogServerEndpointUrl());
  }
}
