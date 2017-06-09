package com.wavefront.agent;

import org.jboss.resteasy.resteasy_jaxrs.i18n.LogMessages;

import java.io.IOException;

import javax.ws.rs.WebApplicationException;
import javax.ws.rs.ext.WriterInterceptor;
import javax.ws.rs.ext.WriterInterceptorContext;

/**
 * This RESTEasy interceptor allows disabling GZIP compression even for methods annotated with @GZIP by removing the
 * Content-Encoding header.
 * RESTEasy always adds "Content-Encoding: gzip" header when it encounters @GZIP annotation, but if the request body
 * is actually sent uncompressed, it violates section 3.1.2.2 of RFC7231.
 *
 * Created by vasily@wavefront.com on 6/9/17.
 */
public class DisableGZIPEncodingInterceptor implements WriterInterceptor {
    public DisableGZIPEncodingInterceptor() {
    }

    public void aroundWriteTo(WriterInterceptorContext context) throws IOException, WebApplicationException {
      LogMessages.LOGGER.debugf("Interceptor : %s,  Method : aroundWriteTo", this.getClass().getName());
      Object encoding = context.getHeaders().getFirst("Content-Encoding");
      if(encoding != null && encoding.toString().equalsIgnoreCase("gzip")) {
        context.getHeaders().remove("Content-Encoding");
      }
      context.proceed();
    }
  }
  