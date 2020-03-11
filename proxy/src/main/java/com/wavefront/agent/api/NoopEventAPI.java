package com.wavefront.agent.api;

import javax.ws.rs.core.Response;
import java.util.List;
import java.util.UUID;

import com.wavefront.api.EventAPI;
import com.wavefront.dto.Event;

/**
 * A no-op SourceTagAPI stub.
 *
 * @author vasily@wavefront.com
 */
public class NoopEventAPI implements EventAPI {
  @Override
  public Response proxyEvents(UUID uuid, List<Event> list) {
    return Response.ok().build();
  }
}
