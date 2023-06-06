package com.wavefront.agent.api;

import com.google.common.net.HttpHeaders;
import javax.ws.rs.Consumes;
import javax.ws.rs.HeaderParam;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

public interface CSPAPI {
  @POST
  @Consumes(MediaType.APPLICATION_FORM_URLENCODED)
  @Path("/csp/gateway/am/api/auth/api-tokens/authorize")
  Response getTokenByAPIToken(final String grantType, final String apiToken);

  @POST
  @Consumes(MediaType.APPLICATION_FORM_URLENCODED)
  @Path("csp/gateway/am/api/auth/authorize")
  Response getTokenByClientCredentials(
      @HeaderParam(HttpHeaders.AUTHORIZATION) final String agentProxyId, final String orgId);
}
