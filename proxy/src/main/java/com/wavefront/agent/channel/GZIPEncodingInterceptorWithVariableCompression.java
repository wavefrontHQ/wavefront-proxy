package com.wavefront.agent.channel;

import org.jboss.resteasy.util.CommitHeaderOutputStream;

import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.ext.WriterInterceptor;
import javax.ws.rs.ext.WriterInterceptorContext;
import java.io.IOException;
import java.io.OutputStream;
import java.util.zip.GZIPOutputStream;

/**
 * An alternative to
 * {@link org.jboss.resteasy.plugins.interceptors.encoding.GZIPEncodingInterceptor} that allows
 * changing the GZIP deflater's compression level.
 *
 * @author vasily@wavefront.com
 * @author <a href="mailto:bill@burkecentral.com">Bill Burke</a>
 */
public class GZIPEncodingInterceptorWithVariableCompression implements WriterInterceptor {
  private final int level;
  public GZIPEncodingInterceptorWithVariableCompression(int level) {
    this.level = level;
  }

  public static class EndableGZIPOutputStream extends GZIPOutputStream {
    public EndableGZIPOutputStream(final OutputStream os, int level) throws IOException {
      super(os);
      this.def.setLevel(level);
    }

    @Override
    public void finish() throws IOException {
      super.finish();
      def.end();
    }
  }

  public static class CommittedGZIPOutputStream extends CommitHeaderOutputStream {
    private final int level;
    protected CommittedGZIPOutputStream(final OutputStream delegate,
                                        int level) {
      super(delegate, null);
      this.level = level;
    }

    protected GZIPOutputStream gzip;

    public GZIPOutputStream getGzip() {
      return gzip;
    }

    @Override
    public synchronized void commit() {
      if (isHeadersCommitted) return;
      isHeadersCommitted = true;
      try {
        gzip = new EndableGZIPOutputStream(delegate, level);
        delegate = gzip;
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
  }

  @Override
  public void aroundWriteTo(WriterInterceptorContext context)
      throws IOException, WebApplicationException {
    Object encoding = context.getHeaders().getFirst(HttpHeaders.CONTENT_ENCODING);
    if (encoding != null && encoding.toString().equalsIgnoreCase("gzip")) {
      OutputStream old = context.getOutputStream();
      CommittedGZIPOutputStream gzipOutputStream = new CommittedGZIPOutputStream(old, level);
      context.getHeaders().remove("Content-Length");
      context.setOutputStream(gzipOutputStream);
      try {
        context.proceed();
      } finally {
        if (gzipOutputStream.getGzip() != null) gzipOutputStream.getGzip().finish();
        context.setOutputStream(old);
      }
    } else {
      context.proceed();
    }
  }
}
