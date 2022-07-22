package com.wavefront.agent.api;

import com.wavefront.api.EventAPI;
import com.wavefront.dto.Event;
import java.util.List;
import java.util.UUID;
import javax.ws.rs.core.Response;

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

  @Override
  public Response proxyEventsString(UUID uuid, String s) {
    return null;
  }
}
