package com.wavefront.agent;

import java.util.logging.Logger;
import java.util.logging.Level;

import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.ext.ReaderInterceptor;
import javax.ws.rs.ext.ReaderInterceptorContext;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.nio.ByteBuffer;
import java.util.zip.InflaterInputStream;

/**
 * lifted from docs:
 *
 * @see <a href="https://jersey.java.net/documentation/latest/filters-and-interceptors.html#d0e9818">Documentation</a>
 * @see <a href="https://github.com/leifoolsen/jaxrs2-workshop/blob/master/jaxrs-hateoas/src/main/java/no/javabin/jaxrs/hateoas/rest/interceptor/GZIPReaderInterceptor.java">Full
 * implementation</a>
 */
class ZLibReaderInterceptor implements ReaderInterceptor {
  private static final Logger LOG = Logger.getLogger(ZLibReaderInterceptor.class.getCanonicalName());
  @Override
  public Object aroundReadFrom(ReaderInterceptorContext context) throws IOException, WebApplicationException {

    MultivaluedMap<String, String> headers = context.getHeaders();
    List<String> contentEncoding = headers.get("Content-Encoding");

    if (contentEncoding != null) {
      if (contentEncoding.contains("deflate")) {
        try {
          final InputStream is = context.getInputStream();
          context.setInputStream(new InflaterInputStream(is));
        } catch (final Exception e) {
          LOG.log(Level.WARNING, "Failed to decompress message", e);
          throw new IllegalArgumentException("Unable to decompress", e);
        }
      }
    }
    return context.proceed();
  }
}
