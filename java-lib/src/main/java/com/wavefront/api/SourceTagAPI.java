package com.wavefront.api;

import java.util.List;

import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

/**
 * API for source tag operations.
 *
 * @author vasily@wavefront.com
 */
@Path("/v2/")
public interface SourceTagAPI {

  /**
   * Add a single tag to a source.
   *
   * @param id       source ID.
   * @param token    authentication token.
   * @param tagValue tag to add.
   */
  @PUT
  @Path("source/{id}/tag/{tagValue}")
  @Produces(MediaType.APPLICATION_JSON)
  Response appendTag(@PathParam("id") String id,
                     @QueryParam("t") String token,
                     @PathParam("tagValue") String tagValue);

  /**
   * Remove a single tag from a source.
   *
   * @param id       source ID.
   * @param token    authentication token.
   * @param tagValue tag to remove.
   */
  @DELETE
  @Path("source/{id}/tag/{tagValue}")
  @Produces(MediaType.APPLICATION_JSON)
  Response removeTag(@PathParam("id") String id,
                     @QueryParam("t") String token,
                     @PathParam("tagValue") String tagValue);

  /**
   * Sets tags for a host, overriding existing tags.
   *
   * @param id             source ID.
   * @param token          authentication token.
   * @param tagValuesToSet tags to set.
   */
  @POST
  @Path("source/{id}/tag")
  @Consumes(MediaType.APPLICATION_JSON)
  @Produces(MediaType.APPLICATION_JSON)
  Response setTags(@PathParam ("id") String id,
                   @QueryParam("t") String token,
                   List<String> tagValuesToSet);


  /**
   * Set description for a source.
   *
   * @param id          source ID.
   * @param token       authentication token.
   * @param description description.
   */
  @POST
  @Path("source/{id}/description")
  @Consumes(MediaType.APPLICATION_JSON)
  @Produces(MediaType.APPLICATION_JSON)
  Response setDescription(@PathParam("id") String id,
                          @QueryParam("t") String token,
                          String description);

  /**
   * Remove description from a source.
   *
   * @param id    source ID.
   * @param token authentication token.
   */
  @DELETE
  @Path("source/{id}/description")
  @Produces(MediaType.APPLICATION_JSON)
  Response removeDescription(@PathParam("id") String id,
                             @QueryParam("t") String token);
}
