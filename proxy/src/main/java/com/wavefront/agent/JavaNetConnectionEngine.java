package com.wavefront.agent;

import org.jboss.resteasy.client.jaxrs.ClientHttpEngine;
import org.jboss.resteasy.client.jaxrs.i18n.Messages;
import org.jboss.resteasy.client.jaxrs.internal.ClientInvocation;
import org.jboss.resteasy.client.jaxrs.internal.ClientResponse;
import org.jboss.resteasy.util.CaseInsensitiveMap;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.SSLContext;
import javax.ws.rs.ProcessingException;
import javax.ws.rs.core.MultivaluedMap;

/**
 * {@link ClientHttpEngine} that uses {@link HttpURLConnection} to connect to an Http endpoint.
 *
 * @author Clement Pang (clement@wavefront.com).
 */
public class JavaNetConnectionEngine implements ClientHttpEngine {

  protected SSLContext sslContext;
  protected HostnameVerifier hostnameVerifier;

  public JavaNetConnectionEngine() {
  }

  public ClientResponse invoke(ClientInvocation request) {
    final HttpURLConnection connection;
    int status;
    try {
      connection = this.createConnection(request);
      this.executeRequest(request, connection);
      status = connection.getResponseCode();
    } catch (IOException ex) {
      throw new ProcessingException(Messages.MESSAGES.unableToInvokeRequest(), ex);
    }

    ClientResponse response = new JavaNetConnectionClientResponse(request, connection);
    response.setStatus(status);
    response.setHeaders(this.getHeaders(connection));
    return response;
  }

  protected MultivaluedMap<String, String> getHeaders(HttpURLConnection connection) {
    CaseInsensitiveMap<String> headers = new CaseInsensitiveMap<>();
    final Iterator<Map.Entry<String, List<String>>> headerFieldsIter =
        connection.getHeaderFields().entrySet().iterator();

    while (true) {
      Map.Entry<String, List<String>> header;
      do {
        if (!headerFieldsIter.hasNext()) {
          return headers;
        }

        header = headerFieldsIter.next();
      } while (header.getKey() == null);

      final Iterator<String> valuesIterator = header.getValue().iterator();

      while (valuesIterator.hasNext()) {
        String value = valuesIterator.next();
        headers.add(header.getKey(), value);
      }
    }
  }

  public void close() {
  }

  protected HttpURLConnection createConnection(ClientInvocation request) throws IOException {
    HttpURLConnection connection = (HttpURLConnection) request.getUri().toURL().openConnection();
    connection.setRequestMethod(request.getMethod());
    return connection;
  }

  protected void executeRequest(ClientInvocation request, HttpURLConnection connection) {
    connection.setInstanceFollowRedirects(request.getMethod().equals("GET"));
    if (request.getEntity() != null) {
      if (request.getMethod().equals("GET")) {
        throw new ProcessingException(Messages.MESSAGES.getRequestCannotHaveBody());
      }

      ByteArrayOutputStream baos = new ByteArrayOutputStream();
      request.getDelegatingOutputStream().setDelegate(baos);

      try {
        request.writeRequestBody(request.getEntityStream());
        baos.close();
        this.commitHeaders(request, connection);
        connection.setDoOutput(true);
        OutputStream e = connection.getOutputStream();
        e.write(baos.toByteArray());
        e.flush();
        e.close();
      } catch (IOException ex) {
        throw new RuntimeException(ex);
      }
    } else {
      this.commitHeaders(request, connection);
    }

  }

  protected void commitHeaders(ClientInvocation request, HttpURLConnection connection) {
    final MultivaluedMap<String, String> headers = request.getHeaders().asMap();

    for (Map.Entry<String, List<String>> header : headers.entrySet()) {
      final List<String> values = header.getValue();
      for (String value : values) {
        connection.addRequestProperty(header.getKey(), value);
      }
    }
  }

  public SSLContext getSslContext() {
    return this.sslContext;
  }

  public HostnameVerifier getHostnameVerifier() {
    return this.hostnameVerifier;
  }

  public void setSslContext(SSLContext sslContext) {
    this.sslContext = sslContext;
  }

  public void setHostnameVerifier(HostnameVerifier hostnameVerifier) {
    this.hostnameVerifier = hostnameVerifier;
  }

  private static class JavaNetConnectionClientResponse extends ClientResponse {
    private HttpURLConnection connection;
    private InputStream stream;

    public JavaNetConnectionClientResponse(ClientInvocation request, HttpURLConnection connection) {
      super(request.getClientConfiguration());
      this.connection = connection;
    }

    protected InputStream getInputStream() {
      if (this.stream == null) {
        try {
          this.stream = this.status < 300 ? connection.getInputStream() : connection.getErrorStream();
        } catch (IOException ex) {
          throw new RuntimeException(ex);
        }
      }

      return this.stream;
    }

    protected void setInputStream(InputStream is) {
      this.stream = is;
    }

    public void releaseConnection() throws IOException {
      InputStream is = this.getInputStream();
      if (is != null) {
        is.close();
      }
      connection.disconnect();

      this.stream = null;
      this.connection = null;
      this.properties = null;
      this.configuration = null;
      this.bufferedEntity = null;
    }
  }
}
