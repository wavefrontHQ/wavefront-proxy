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
 *
 * @author Vikram Raman
 */
@Path("/")
public interface DataIngestorAPI {

  @POST
  @Path("report")
  @Consumes({MediaType.APPLICATION_OCTET_STREAM, MediaType.APPLICATION_FORM_URLENCODED,
      MediaType.TEXT_PLAIN})
  Response report(@QueryParam("f") String format, InputStream stream) throws IOException;

  /*
  @POST
  @Path("report")
  @Consumes(MediaType.MULTIPART_FORM_DATA)
  Response uploadFile(@QueryParam("f") String format,
                      @FormDataParam("file") InputStream uploadedInputStream,
                      @FormDataParam("file") FormDataContentDisposition fileDetails);*/
}
