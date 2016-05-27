package com.wavefront.agent;

import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.ext.ReaderInterceptor;
import javax.ws.rs.ext.ReaderInterceptorContext;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.zip.GZIPInputStream;

/**
 * lifted from docs:
 *
 * @see <a href="https://jersey.java.net/documentation/latest/filters-and-interceptors.html#d0e9818">Documentation</a>
 * @see <a href="https://github.com/leifoolsen/jaxrs2-workshop/blob/master/jaxrs-hateoas/src/main/java/no/javabin/jaxrs/hateoas/rest/interceptor/GZIPReaderInterceptor.java">Full
 * implementation</a>
 */
class GZIPReaderInterceptor implements ReaderInterceptor {
  @Override
  public Object aroundReadFrom(ReaderInterceptorContext context) throws IOException, WebApplicationException {

    MultivaluedMap<String, String> headers = context.getHeaders();
    List<String> contentEncoding = headers.get("Content-Encoding");

    if (contentEncoding != null) {
      if (contentEncoding.contains("deflate") || contentEncoding.contains("gzip")) {
        final InputStream originalInputStream = context.getInputStream();
        context.setInputStream(new GZIPInputStream(originalInputStream));
      }
    }
    return context.proceed();
  }
}
