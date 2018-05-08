package com.wavefront.api;

import java.io.IOException;
import java.io.InputStream;

import javax.ws.rs.Consumes;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

/**
 * The API for reporting points directly to a Wavefront server.
 *
 * @author Vikram Raman
 */
@Path("/")
public interface DataIngesterAPI {

  @POST
  @Path("report")
  @Consumes({MediaType.APPLICATION_OCTET_STREAM, MediaType.APPLICATION_FORM_URLENCODED,
      MediaType.TEXT_PLAIN})
  Response report(@QueryParam("f") String format, InputStream stream) throws IOException;
}
