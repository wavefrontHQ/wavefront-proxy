package com.wavefront.agent.logforwarder.ingestion.client.gateway.verticle;


import com.wavefront.agent.logforwarder.ingestion.client.gateway.metrics.MetricsService;
import com.wavefront.agent.logforwarder.ingestion.client.gateway.utils.Utils;
import com.wavefront.agent.logforwarder.ingestion.restapi.BaseHttpEndpoint;
import com.wavefront.agent.logforwarder.ingestion.util.CustomHttpException;
import com.wavefront.agent.logforwarder.ingestion.util.RequestUtil;

import org.apache.commons.collections.MapUtils;
import org.apache.http.HttpStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.lang.invoke.MethodHandles;
import java.net.HttpURLConnection;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import io.dropwizard.metrics5.Histogram;
import io.dropwizard.metrics5.MetricRegistry;
import io.netty.handler.codec.http.HttpHeaderValues;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.Promise;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.http.HttpHeaders;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;
import io.vertx.ext.web.handler.BodyHandler;
import static io.netty.handler.codec.http.HttpHeaderValues.APPLICATION_JSON;
import static org.apache.http.HttpStatus.SC_BAD_REQUEST;
import static org.apache.http.HttpStatus.SC_INTERNAL_SERVER_ERROR;

import static com.wavefront.agent.logforwarder.ingestion.util.RequestUtil.getMsg;
import static com.wavefront.agent.logforwarder.ingestion.util.RequestUtil.getExceptionCause;

/**
 * @author Manoj Ramakrishnan (rmanoj@vmware.com).
 * @since 9/20/21 10:42 AM
 */
public class IngressVerticle extends AbstractVerticle {

  private static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private static final String START_TIME_MILLIS = "startMs";
  private static final String CONTENT_ENCODING_HEADER = HttpHeaders.CONTENT_ENCODING.toString();
  private static final String GZIP_ENCODING = HttpHeaderValues.GZIP.toString();
  private static final String GZIP_DECOMPRESS_SUCCESS = "vertx.gzip.decompress.success";
  private static final String GZIP_DECOMPRESS_FAILURE = "vertx.gzip.decompress.failure";

  private static final Histogram taskTotalTime = MetricsService.getInstance().getHistogram(
      MetricRegistry.name(IngressVerticle.class.getSimpleName(), "total",
          MetricsService.TIMETAKEN_IN_MICROS).getKey());

  private static final Histogram processTime = MetricsService.getInstance().getHistogram(
      MetricRegistry.name(IngressVerticle.class.getSimpleName(), "process",
          MetricsService.TIMETAKEN_IN_MICROS).getKey());

  private final Router router;
  private Map<String, BaseHttpEndpoint> baseHttpEndpointMap;

  public IngressVerticle(Router router, Map<String, BaseHttpEndpoint> BaseHttpEndpointMap) {
    this.router = router;
    this.baseHttpEndpointMap = BaseHttpEndpointMap;
  }

  @Override
  public void start(Promise<Void> startPromise) {
    this.router.route()
        .handler(BodyHandler.create(false))
        .handler(this::handleIngress)
        .failureHandler(ctx -> {
          CustomHttpException exception = (CustomHttpException) ctx.failure();
          String msg = getMsg(exception);
          ctx.response().putHeader(HttpHeaders.CONTENT_TYPE, APPLICATION_JSON);
          ctx.response().setStatusCode(exception.getStatusCode());
          addProcessedByHeader(ctx);
          ctx.response().end(new JsonObject().put("message", msg).put(
              "statusCode", exception.getStatusCode()).encode());
        });
    startPromise.complete();
  }

  private void handleIngress(RoutingContext routingContext) {
    routingContext.data().put(START_TIME_MILLIS, System.currentTimeMillis());

    // Decompress gzip data and set the headers accordingly
    String encodingValue = routingContext.request().getHeader(CONTENT_ENCODING_HEADER);
    if (GZIP_ENCODING.equals(encodingValue)) {
      int contentLengthBytes = VertxUtils.getBodySize(routingContext);
      if (contentLengthBytes > 0) {
        Buffer decompressedBuffer = VertxUtils.decompressGzip(routingContext.getBody());
        if (decompressedBuffer == null) {
          MetricsService.getInstance().getMeter(GZIP_DECOMPRESS_FAILURE).mark();
          IllegalArgumentException ex = new IllegalArgumentException("Failed to decompress body");
          routingContext.fail(new CustomHttpException(HttpURLConnection.HTTP_BAD_REQUEST, ex,
              ex.getMessage()));
          return;
        }
        routingContext.setBody(decompressedBuffer);
        routingContext.request().headers().set(HttpHeaders.CONTENT_LENGTH,
            String.valueOf(decompressedBuffer.length()));
      }
      routingContext.request().headers().remove(CONTENT_ENCODING_HEADER);
      MetricsService.getInstance().getMeter(GZIP_DECOMPRESS_SUCCESS).mark();
    }

    BaseHttpEndpoint BaseHttpEndpoint = getBaseHttpEndpoint(routingContext.normalisedPath());
    routeRequest(routingContext, BaseHttpEndpoint);
  }

  private BaseHttpEndpoint getBaseHttpEndpoint(String reqPath) {
    for (String path : baseHttpEndpointMap.keySet()) {
      if (reqPath.startsWith(path)) {
        return baseHttpEndpointMap.get(path);
      }
    }
    return null;
  }

  private void routeRequest(RoutingContext routingContext, BaseHttpEndpoint BaseHttpEndpoint) {
    long startTime = System.nanoTime();

    if (BaseHttpEndpoint != null) {
      processVertxFlow(routingContext, BaseHttpEndpoint, startTime);
    } else {
      routingContext.response().setStatusCode(HttpStatus.SC_NOT_FOUND).end();
    }
  }

  private void processVertxFlow(RoutingContext routingContext, BaseHttpEndpoint BaseHttpEndpoint, long startTime) {
    CompletableFuture<?> future = new CompletableFuture();
    long processStartTime = System.nanoTime();
    future.thenAccept(res -> {
      long processEndTime = System.nanoTime();
      processTime.update((processEndTime - processStartTime) / 1000);
      taskTotalTime.update((processEndTime - startTime) / 1000);
      VertxResponse resp = (VertxResponse) res;
      if (resp != null && MapUtils.isNotEmpty(resp.responseHeaders)) {
        resp.responseHeaders.entrySet().forEach(header -> {
          routingContext.response().putHeader(header.getKey(), header.getValue());
        });
      }
      routingContext.response().putHeader(HttpHeaders.CONTENT_TYPE, resp.contentType);
      addProcessedByHeader(routingContext);
      if (resp.bytes != null) {
        routingContext.response().setStatusCode(HttpStatus.SC_OK).end(Buffer.buffer(resp.bytes));
      } else {
        routingContext.response().setStatusCode(HttpStatus.SC_OK).end(Utils.toJson(resp.body));
      }
    }).exceptionally(e1 -> {
      // request failed
      logger.error("Error while handling vertx(future) request with message:" + e1.getMessage(), e1);
      CustomHttpException exp = RequestUtil.getCustomException(e1);
      if (exp != null) {
        if (MapUtils.isNotEmpty(exp.getResponseHeaders())) {
          for (String key : exp.getResponseHeaders().keySet()) {
            routingContext.response().putHeader(key, exp.getResponseHeaders().get(key));
          }
        }
        routingContext.fail(exp.getStatusCode(), exp);
        return null;
      }
      int statusCode = e1.getCause() instanceof IllegalArgumentException ? SC_BAD_REQUEST :
          SC_INTERNAL_SERVER_ERROR;
      Throwable throwable = getExceptionCause(e1);
      addProcessedByHeader(routingContext);
      routingContext.fail(new CustomHttpException(statusCode, throwable, throwable.getMessage()));
      return null;
    });

    switch (routingContext.request().method()) {
      case GET:
        BaseHttpEndpoint.handleGet(future, routingContext);
        break;
      case POST:
        BaseHttpEndpoint.handlePost(future, routingContext);
        break;
      case PATCH:
        BaseHttpEndpoint.handlePatch(future, routingContext);
        break;
      case PUT:
        BaseHttpEndpoint.handlePut(future, routingContext);
        break;
      case DELETE:
        BaseHttpEndpoint.handleDelete(future, routingContext);
        break;
      default:
        throw new RuntimeException("Method not supported");
    }
  }

  private void addProcessedByHeader(RoutingContext routingContext) {
    routingContext.response().putHeader("processed_by", "vertx");
  }
}
