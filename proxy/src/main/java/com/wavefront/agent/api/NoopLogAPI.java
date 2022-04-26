package com.wavefront.agent.api;

import com.wavefront.api.LogAPI;
import com.wavefront.dto.Log;

import java.util.List;

import javax.ws.rs.core.Response;

/**
 * A no-op LogAPI stub.
 *
 * @author amitw@vmware.com
 */
public class NoopLogAPI implements LogAPI {
  @Override
  public Response proxyLogs(String agentProxyId, List<Log> logs) {
    return Response.ok().build();
  }
}
