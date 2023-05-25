package com.wavefront.agent.api;

import static com.wavefront.agent.ProxyConfig.ProxyAuthMethod.WAVEFRONT_API_TOKEN;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import com.wavefront.agent.ProxyConfig;
import com.wavefront.agent.TenantInfo;
import org.junit.Before;
import org.junit.Test;

/** @author Xiaochen Wang (xiaochenw@vmware.com). */
public class APIContainerTest {
  private final int NUM_TENANTS = 5;
  private ProxyConfig proxyConfig;

  @Before
  public void setup() {
    this.proxyConfig = new ProxyConfig();
    TenantInfo tenantInfo = new TenantInfo("fake-token", "fake-url", WAVEFRONT_API_TOKEN);
    this.proxyConfig.getMulticastingTenantList().put("central", tenantInfo);
    for (int i = 0; i < NUM_TENANTS; i++) {
      TenantInfo tenantInfo1 =
          new TenantInfo("fake-token" + i, "fake-url" + i, WAVEFRONT_API_TOKEN);

      this.proxyConfig.getMulticastingTenantList().put("tenant-" + i, tenantInfo1);
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
    APIContainer apiContainer = new APIContainer(null, null, null, null);
    apiContainer.updateServerEndpointURL("central", "fake-url");
  }

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
