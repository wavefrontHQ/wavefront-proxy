package com.wavefront.agent.channel;

import com.google.common.annotations.VisibleForTesting;
import com.wavefront.agent.ProxyConfig;
import com.wavefront.common.TaggedMetricName;
import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.Gauge;

import org.apache.commons.lang3.ObjectUtils;

import java.net.InetSocketAddress;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.logging.Logger;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.http.DefaultFullHttpResponse;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpHeaderValues;
import io.netty.handler.codec.http.HttpResponse;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpUtil;
import io.netty.handler.codec.http.HttpVersion;
import io.netty.util.CharsetUtil;

/**
 * Centrally manages healthcheck statuses (for controlling load balancers).
 *
 * @author vasily@wavefront.com.
 */
public class HealthCheckManagerImpl implements HealthCheckManager {
  private static final Logger log = Logger.getLogger(HealthCheckManager.class.getCanonicalName());

  private final Map<Integer, Boolean> statusMap;
  private final Set<Integer> enabledPorts;
  private final String path;
  private final String contentType;
  private final int passStatusCode;
  private final String passResponseBody;
  private final int failStatusCode;
  private final String failResponseBody;

  /**
   * @param config Proxy configuration
   */
  public HealthCheckManagerImpl(@Nonnull ProxyConfig config) {
    this(config.getHttpHealthCheckPath(), config.getHttpHealthCheckResponseContentType(),
        config.getHttpHealthCheckPassStatusCode(), config.getHttpHealthCheckPassResponseBody(),
        config.getHttpHealthCheckFailStatusCode(), config.getHttpHealthCheckFailResponseBody());
  }

  /**
   * @param path             Health check's path.
   * @param contentType      Optional content-type of health check's response.
   * @param passStatusCode   HTTP status code for 'pass' health checks.
   * @param passResponseBody Optional response body to return with 'pass' health checks.
   * @param failStatusCode   HTTP status code for 'fail' health checks.
   * @param failResponseBody Optional response body to return with 'fail' health checks.
   */
  @VisibleForTesting
  HealthCheckManagerImpl(@Nullable String path, @Nullable String contentType,
                         int passStatusCode, @Nullable String passResponseBody,
                         int failStatusCode, @Nullable String failResponseBody) {
    this.statusMap = new HashMap<>();
    this.enabledPorts = new HashSet<>();
    this.path = path;
    this.contentType = contentType;
    this.passStatusCode = passStatusCode;
    this.passResponseBody = ObjectUtils.firstNonNull(passResponseBody, "");
    this.failStatusCode = failStatusCode;
    this.failResponseBody = ObjectUtils.firstNonNull(failResponseBody, "");
  }

  @Override
  public HttpResponse getHealthCheckResponse(ChannelHandlerContext ctx,
                                             @Nonnull FullHttpRequest request)
      throws URISyntaxException {
    int port = ((InetSocketAddress) ctx.channel().localAddress()).getPort();
    if (!enabledPorts.contains(port)) return null;
    URI uri = new URI(request.uri());
    if (!(this.path == null || this.path.equals(uri.getPath()))) return null;
    // it is a health check URL, now we need to determine current status and respond accordingly
    final boolean ok = isHealthy(port);
    Metrics.newGauge(new TaggedMetricName("listeners", "healthcheck.status",
        "port", String.valueOf(port)), new Gauge<Integer>() {
      @Override
      public Integer value() {
        return isHealthy(port) ? 1 : 0;
      }
    });
    final FullHttpResponse response = new DefaultFullHttpResponse(HttpVersion.HTTP_1_1,
        HttpResponseStatus.valueOf(ok ? passStatusCode : failStatusCode),
        Unpooled.copiedBuffer(ok ? passResponseBody : failResponseBody, CharsetUtil.UTF_8));
    if (contentType != null) {
      response.headers().set(HttpHeaderNames.CONTENT_TYPE, contentType);
    }
    if (HttpUtil.isKeepAlive(request)) {
      response.headers().set(HttpHeaderNames.CONTENT_LENGTH, response.content().readableBytes());
      response.headers().set(HttpHeaderNames.CONNECTION, HttpHeaderValues.KEEP_ALIVE);
    }
    Metrics.newCounter(new TaggedMetricName("listeners", "healthcheck.httpstatus." +
        (ok ? passStatusCode : failStatusCode) + ".count", "port", String.valueOf(port))).inc();
    return response;
  }

  @Override
  public boolean isHealthy(int port) {
    return statusMap.getOrDefault(port, true);
  }

  @Override
  public void setHealthy(int port) {
    statusMap.put(port, true);
  }

  @Override
  public void setUnhealthy(int port) {
    statusMap.put(port, false);
  }

  @Override
  public void setAllHealthy() {
    enabledPorts.forEach(x -> {
      setHealthy(x);
      log.info("Port " + x + " was marked as healthy");
    });
  }

  @Override
  public void setAllUnhealthy() {
    enabledPorts.forEach(x -> {
      setUnhealthy(x);
      log.info("Port " + x + " was marked as unhealthy");
    });
  }

  @Override
  public void enableHealthcheck(int port) {
    enabledPorts.add(port);
  }
}
