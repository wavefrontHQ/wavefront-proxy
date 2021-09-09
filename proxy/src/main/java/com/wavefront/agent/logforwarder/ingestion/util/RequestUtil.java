package com.wavefront.agent.logforwarder.ingestion.util;

import com.vmware.log.forwarder.exception.CustomHttpException;
import com.vmware.log.forwarder.verticle.VertxResponse;
import com.vmware.xenon.common.Operation;
import com.vmware.xenon.common.Utils;

import org.apache.commons.collections4.MapUtils;
import org.apache.http.HttpStatus;
import org.apache.http.entity.ContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.invoke.MethodHandles;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;

import io.vertx.ext.web.RoutingContext;

/**
 * @author Manoj Ramakrishnan (rmanoj@vmware.com).
 * @since 9/7/21 1:19 PM
 */
public class RequestUtil {

  private static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  public static void setException(CompletableFuture future, Operation op, Throwable e) {
    if (op != null) {
      op.fail(e);
    } else {
      future.completeExceptionally(e);
    }
  }

  public static CustomHttpException getCustomException(Throwable e) {
    CustomHttpException exp = null;
    if (e instanceof CompletionException && e.getCause() instanceof CustomHttpException) {
      exp = (CustomHttpException) e.getCause();
    } else if (e instanceof CustomHttpException) {
      exp = (CustomHttpException) e;
    }
    return exp;
  }

  public static Throwable getExceptionCause(Throwable e) {
    return e instanceof CompletionException ? (e.getCause() != null ? e.getCause() : e) : e;
  }

  public static String getMsg(Throwable e) {
    if (e instanceof IllegalArgumentException) {
      if (e.getCause() != null) {
        return e.getCause().getMessage();
      }
    }
    if (e.getMessage() != null) {
      return e.getMessage();
    }
    return "{}";
  }

  public static void methodNotSupported(CompletableFuture future, RoutingContext context) {
    logger.error(String.format("%s not supported", context.request().method()));
    CustomHttpException exception = new CustomHttpException(HttpStatus.SC_METHOD_NOT_ALLOWED, new
        IllegalStateException("Action not supported: " + context.request().method()), null);
    setException(future, null, exception);
  }

  public static void setResponse(CompletableFuture future, Operation op) {
    if (op != null) {
      op.setBody("{}");
      op.complete();
    } else {
      future.complete(createVertxResponse("{}", ContentType.APPLICATION_JSON.toString(), new HashMap<>()));
    }
  }

  public static void setResponse(CompletableFuture future, Operation op, Object body, String contentType,
                                 Map<String, String> responseHeaders) {
    if (op != null) {
      op.setBody(Utils.toJson(body));
      op.getResponseHeaders().putAll(responseHeaders);
      if (contentType != null) {
        op.setContentType(contentType);
      }
      op.complete();
    } else {
      future.complete(createVertxResponse(body, contentType, responseHeaders));
    }
  }

  public static void setException(CompletableFuture future, Operation op, Throwable e, int statusCode,
                                  String failureMsg, Map<String, String> headers) {
    if (op != null) {
      op.fail(statusCode, e, failureMsg);
      if (MapUtils.isNotEmpty(headers)) {
        for (Map.Entry<String, String> entry : headers.entrySet()) {
          op.addResponseHeader(entry.getKey(), entry.getValue());
        }
      }
    } else {
      future.completeExceptionally(new CustomHttpException(statusCode, e, failureMsg, headers));
    }
  }

  public static void setException(CompletableFuture future, Operation op, Throwable e, int statusCode,
                                  String failureMsg) {
    setException(future, op, e, statusCode, failureMsg, new HashMap<>());
  }

  public static VertxResponse createVertxResponse(Object body, String contentType,
                                                  Map<String, String> responseHeaders) {
    VertxResponse response = new VertxResponse();
    response.body = body;
    response.contentType = contentType;
    response.responseHeaders = responseHeaders;
    return response;
  }

  public static <T> T getBody(RoutingContext routingContext, Operation op, Class<T> cls) {
    return op != null ? op.getBody(cls) : Utils.fromJson(routingContext.getBodyAsJson(), cls);
  }

  public static URI getUri(RoutingContext routingContext, Operation op) {
    return op != null ? op.getUri() : URI.create(routingContext.request().absoluteURI());
  }

  public static String getRequestHeader(RoutingContext routingContext, Operation op, String header) {
    return op != null ? op.getRequestHeader(header) : routingContext.request().getHeader(header);
  }

  public static String getReferer(RoutingContext routingContext, Operation op) {
    return op != null ? op.getReferer().getHost() : routingContext.request().remoteAddress().host();
  }
}

