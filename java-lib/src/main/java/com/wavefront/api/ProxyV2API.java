package com.wavefront.api;

import com.fasterxml.jackson.databind.JsonNode;
import com.wavefront.api.agent.AgentConfiguration;

import java.util.UUID;

import javax.ws.rs.Consumes;
import javax.ws.rs.FormParam;
import javax.ws.rs.HeaderParam;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

/**
 * v2 API for the proxy.
 *
 * @author vasily@wavefront.com
 */
@Path("/")
public interface ProxyV2API {

  /**
   * Register the proxy and transmit proxy metrics to Wavefront servers.
   *
   * @param proxyId       ID of the proxy.
   * @param authorization Authorization token.
   * @param hostname      Host name of the proxy.
   * @param version       Build version of the proxy.
   * @param currentMillis Current time at the proxy (used to calculate clock drift).
   * @param agentMetrics  Proxy metrics.
   * @param ephemeral     If true, proxy is removed from the UI after 24 hours of inactivity.
   * @return Proxy configuration.
   */
  @POST
  @Path("v2/wfproxy/checkin")
  @Consumes(MediaType.APPLICATION_JSON)
  @Produces(MediaType.APPLICATION_JSON)
  AgentConfiguration proxyCheckin(@HeaderParam("X-WF-PROXY-ID") final UUID proxyId,
                                  @HeaderParam("Authorization") String authorization,
                                  @QueryParam("hostname") String hostname,
                                  @QueryParam("version") String version,
                                  @QueryParam("currentMillis") final Long currentMillis,
                                  JsonNode agentMetrics,
                                  @QueryParam("ephemeral") Boolean ephemeral);

  /**
   * Report batched data (metrics, histograms, spans, etc) to Wavefront servers.
   *
   * @param proxyId       Proxy Id reporting the result.
   * @param format        The format of the data (wavefront, histogram, trace, spanLogs)
   * @param pushData      Push data batch (newline-delimited)
   */
  @POST
  @Consumes(MediaType.TEXT_PLAIN)
  @Path("v2/wfproxy/report")
  Response proxyReport(@HeaderParam("X-WF-PROXY-ID") final UUID proxyId,
                       @QueryParam("format") final String format,
                       final String pushData);

  /**
   * Reports confirmation that the proxy has processed and accepted the configuration sent from the back-end.
   *
   * @param proxyId ID of the proxy.
   */
  @POST
  @Path("v2/wfproxy/config/processed")
  void proxyConfigProcessed(@HeaderParam("X-WF-PROXY-ID") final UUID proxyId);

  /**
   * Reports an error that occurred in the proxy.
   *
   * @param proxyId ID of the proxy reporting the error.
   * @param details Details of the error.
   */
  @POST
  @Path("v2/wfproxy/error")
  void proxyError(@HeaderParam("X-WF-PROXY-ID") final UUID proxyId,
                  @FormParam("details") String details);
}
