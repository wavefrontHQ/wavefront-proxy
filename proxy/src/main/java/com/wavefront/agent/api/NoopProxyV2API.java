package com.wavefront.agent.api;

import javax.ws.rs.core.Response;
import java.util.UUID;

import com.fasterxml.jackson.databind.JsonNode;
import com.wavefront.api.ProxyV2API;
import com.wavefront.api.agent.AgentConfiguration;

/**
 * Partial ProxyV2API wrapper stub that passed proxyCheckin/proxyConfigProcessed calls to the
 * delegate and replaces proxyReport/proxyError with a no-op.
 *
 * @author vasily@wavefront.com
 */
public class NoopProxyV2API implements ProxyV2API {
  private final ProxyV2API wrapped;

  public NoopProxyV2API(ProxyV2API wrapped) {
    this.wrapped = wrapped;
  }

  @Override
  public AgentConfiguration proxyCheckin(UUID proxyId, String authorization, String hostname,
                                         String version, Long currentMillis, JsonNode agentMetrics,
                                         Boolean ephemeral) {
    return wrapped.proxyCheckin(proxyId, authorization, hostname, version, currentMillis,
        agentMetrics, ephemeral);
  }

  @Override
  public Response proxyReport(UUID uuid, String s, String s1) {
    return Response.ok().build();
  }

  @Override
  public void proxyConfigProcessed(UUID uuid) {
    wrapped.proxyConfigProcessed(uuid);
  }

  @Override
  public void proxyError(UUID uuid, String s) {
  }
}
