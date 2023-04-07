//
// Source code recreated from a .class file by IntelliJ IDEA
// (powered by FernFlower decompiler)
//

package com.wavefront.common;

import java.util.UUID;
import javax.ws.rs.*;
import javax.ws.rs.core.Response;

@Path("/le-mans/v1/streams")
public interface LeMansAPI {

  @POST
  @Consumes({"text/plain"})
  @Path("/{metrics-stream-name}")
  Response leMansProxyReport(
      @PathParam("metrics-stream-name") String coolName,
      @HeaderParam("X-WF-PROXY-ID") UUID var1,
      @QueryParam("format") String var2,
      String var3);
}
