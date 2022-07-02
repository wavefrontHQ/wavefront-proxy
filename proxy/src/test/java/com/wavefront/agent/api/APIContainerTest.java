package com.wavefront.agent.api;

import static org.junit.Assert.*;

import com.google.common.collect.ImmutableMap;
import com.wavefront.agent.ProxyConfig;
import org.junit.Before;
import org.junit.Test;

/** @author Xiaochen Wang (xiaochenw@vmware.com). */
public class APIContainerTest {
  private final int NUM_TENANTS = 5;
  private ProxyConfig proxyConfig;

  @Before
  public void setup() {
    this.proxyConfig = new ProxyConfig();
    this.proxyConfig
        .getMulticastingTenantList()
        .put("central", ImmutableMap.of("token", "fake-token", "server", "fake-url"));
    for (int i = 0; i < NUM_TENANTS; i++) {
      this.proxyConfig
          .getMulticastingTenantList()
          .put("tenant-" + i, ImmutableMap.of("token", "fake-token" + i, "server", "fake-url" + i));
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
}
