package com.wavefront.agent.api;

import com.google.common.net.HttpHeaders;

import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

public interface CSPAPI {
  @POST
  @Consumes(MediaType.APPLICATION_FORM_URLENCODED)
  @Path("/csp/gateway/am/api/auth/api-tokens/authorize")
  Response getTokenByAPIToken(
          @FormParam("grant_type") final String grantType,
          @FormParam("api_token") final String apiToken);

  @POST
  @Consumes(MediaType.APPLICATION_FORM_URLENCODED)
  @Path("csp/gateway/am/api/auth/authorize")
  Response getTokenByClientCredentials(
      @HeaderParam(HttpHeaders.AUTHORIZATION) final String auth,
      @FormParam("grant_type") final String grantType,
      @FormParam("orgId") final String orgId);
}
