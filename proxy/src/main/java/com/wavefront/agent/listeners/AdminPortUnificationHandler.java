package com.wavefront.agent.listeners;

import com.wavefront.agent.auth.TokenAuthenticator;
import com.wavefront.agent.channel.HealthCheckManager;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.math.NumberUtils;

import java.net.InetSocketAddress;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.annotation.Nullable;

import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpResponseStatus;

import static com.wavefront.agent.channel.ChannelUtils.writeHttpResponse;

/**
 * Admin API for managing proxy-wide healthchecks. Access can be restricted by a client's
 * IP address (must match provided allow list regex).
 * Exposed endpoints:
 *  - GET /status/{port}    check current status for {port}, returns 200 if enabled / 503 if not.
 *  - POST /enable/{port}   mark port {port} as healthy.
 *  - POST /disable/{port}  mark port {port} as unhealthy.
 *  - POST /enable          mark all healthcheck-enabled ports as healthy.
 *  - POST /disable         mark all healthcheck-enabled ports as unhealthy.
 *
 * @author vasily@wavefront.com
 */
@ChannelHandler.Sharable
public class AdminPortUnificationHandler extends AbstractHttpOnlyHandler {
  private static final Logger logger = Logger.getLogger(
      AdminPortUnificationHandler.class.getCanonicalName());

  private static final Pattern PATH = Pattern.compile("/(enable|disable|status)/?(\\d*)/?");

  private final String remoteIpAllowRegex;

  /**
   * Create new instance.
   *
   * @param tokenAuthenticator {@link TokenAuthenticator} for incoming requests.
   * @param healthCheckManager shared health check endpoint handler.
   * @param handle             handle/port number.
   */
  public AdminPortUnificationHandler(@Nullable TokenAuthenticator tokenAuthenticator,
                                     @Nullable HealthCheckManager healthCheckManager,
                                     @Nullable String handle,
                                     @Nullable String remoteIpAllowRegex) {
    super(tokenAuthenticator, healthCheckManager, handle);
    this.remoteIpAllowRegex = remoteIpAllowRegex;
  }

  @Override
  protected void handleHttpMessage(final ChannelHandlerContext ctx,
                                   final FullHttpRequest request) throws URISyntaxException {
    StringBuilder output = new StringBuilder();
    String remoteIp = ((InetSocketAddress) ctx.channel().remoteAddress()).getAddress().
        getHostAddress();
    if (remoteIpAllowRegex != null && !Pattern.compile(remoteIpAllowRegex).
        matcher(remoteIp).matches()) {
      logger.warning("Incoming request from non-allowed remote address " + remoteIp +
          " rejected!");
      writeHttpResponse(ctx, HttpResponseStatus.UNAUTHORIZED, output, request);
      return;
    }
    URI uri = new URI(request.uri());
    HttpResponseStatus status;
    Matcher path = PATH.matcher(uri.getPath());
    if (path.matches()) {
      String strPort = path.group(2);
      Integer port = NumberUtils.isNumber(strPort) ? Integer.parseInt(strPort) : null;
      if (StringUtils.isBlank(strPort) || port != null) {
        switch (path.group(1)) {
          case "status":
            if (request.method().equals(HttpMethod.GET)) {
              if (port == null) {
                output.append("Status check requires a specific port");
                status = HttpResponseStatus.BAD_REQUEST;
              } else {
                // return 200 if status check ok, 503 if not
                status = healthCheck.isHealthy(port) ?
                    HttpResponseStatus.OK :
                    HttpResponseStatus.SERVICE_UNAVAILABLE;
                output.append(status.reasonPhrase());
              }
            } else {
              status = HttpResponseStatus.METHOD_NOT_ALLOWED;
            }
            break;
          case "enable":
            if (request.method().equals(HttpMethod.POST)) {
              if (port == null) {
                logger.info("Request to mark all HTTP ports as healthy from remote: " + remoteIp);
                healthCheck.setAllHealthy();
              } else {
                logger.info("Marking HTTP port " + port + " as healthy, remote: " + remoteIp);
                healthCheck.setHealthy(port);
              }
              status = HttpResponseStatus.OK;
            } else {
              status = HttpResponseStatus.METHOD_NOT_ALLOWED;
            }
            break;
          case "disable":
            if (request.method().equals(HttpMethod.POST)) {
              if (port == null) {
                logger.info("Request to mark all HTTP ports as unhealthy from remote: " + remoteIp);
                healthCheck.setAllUnhealthy();
              } else {
                logger.info("Marking HTTP port " + port + " as unhealthy, remote: " + remoteIp);
                healthCheck.setUnhealthy(port);
              }
              status = HttpResponseStatus.OK;
            } else {
              status = HttpResponseStatus.METHOD_NOT_ALLOWED;
            }
            break;
          default:
            status = HttpResponseStatus.BAD_REQUEST;
        }
      } else {
        status = HttpResponseStatus.BAD_REQUEST;
      }
    } else {
      status = HttpResponseStatus.NOT_FOUND;
    }
    writeHttpResponse(ctx, status, output, request);
  }
}
