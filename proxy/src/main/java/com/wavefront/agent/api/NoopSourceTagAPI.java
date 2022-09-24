package com.wavefront.agent.api;

import com.wavefront.api.SourceTagAPI;
import java.util.List;
import javax.ws.rs.core.Response;

/**
 * A no-op SourceTagAPI stub.
 *
 * @author vasily@wavefront.com
 */
public class NoopSourceTagAPI implements SourceTagAPI {

  @Override
  public Response appendTag(String id, String tagValue) {
    return Response.ok().build();
  }

  @Override
  public Response removeTag(String id, String tagValue) {
    return Response.ok().build();
  }

  @Override
  public Response setTags(String id, List<String> tagValuesToSet) {
    return Response.ok().build();
  }

  @Override
  public Response setDescription(String id, String description) {
    return Response.ok().build();
  }

  @Override
  public Response removeDescription(String id) {
    return Response.ok().build();
  }
}
