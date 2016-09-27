package com.wavefront.agent;

import com.google.common.base.Throwables;
import com.google.common.collect.Lists;

import com.wavefront.agent.preprocessor.PointPreprocessor;
import com.wavefront.ingester.Decoder;

import java.net.InetSocketAddress;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.annotation.Nullable;

import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import sunnylabs.report.ReportPoint;


/**
 * Channel handler for byte array data.
 * @author Mike McLaughlin (mike@wavefront.com)
 */
@ChannelHandler.Sharable
class ChannelByteArrayHandler extends SimpleChannelInboundHandler<byte[]> {
  private static final Logger logger = Logger.getLogger(
      ChannelByteArrayHandler.class.getCanonicalName());

  private final Decoder<byte[]> decoder;
  private final PointHandler pointHandler;

  @Nullable
  private final PointPreprocessor preprocessor;

  /**
   * Constructor.
   */
  ChannelByteArrayHandler(Decoder<byte[]> decoder,
                          final PointHandler pointHandler,
                          @Nullable final PointPreprocessor preprocessor) {
    this.decoder = decoder;
    this.pointHandler = pointHandler;
    this.preprocessor = preprocessor;
  }

  @Override
  protected void channelRead0(ChannelHandlerContext ctx, byte[] msg) throws Exception {
    // ignore empty lines.
    if (msg == null || msg.length == 0) {
      return;
    }

    List<ReportPoint> points = Lists.newArrayListWithExpectedSize(1);
    try {
      decoder.decodeReportPoints(msg, points, "dummy");
      for (final ReportPoint point: points) {
        if (preprocessor != null) {
          // backwards compatibility: apply "pointLine" rules to metric name
          if (!preprocessor.forPointLine().filter(point.getMetric())) {
            pointHandler.handleBlockedPoint(preprocessor.forReportPoint().getLastFilterResult());
            continue;
          }
          preprocessor.forReportPoint().transform(point);
          if (!preprocessor.forReportPoint().filter(point)) {
            pointHandler.handleBlockedPoint(preprocessor.forReportPoint().getLastFilterResult());
            continue;
          }
        }
        pointHandler.reportPoint(point, point.getMetric());
      }
    } catch (final Exception e) {
      final Throwable rootCause = Throwables.getRootCause(e);
      String errMsg = "WF-300 Cannot parse: \"" +
          "\", reason: \"" + e.getMessage() + "\"";
      if (rootCause != null && rootCause.getMessage() != null) {
        errMsg = errMsg + ", root cause: \"" + rootCause.getMessage() + "\"";
      }
      InetSocketAddress remoteAddress = (InetSocketAddress) ctx.channel().remoteAddress();
      if (remoteAddress != null) {
        errMsg += "; remote: " + remoteAddress.getHostString();
      }
      logger.log(Level.WARNING, errMsg, e);
      pointHandler.handleBlockedPoint(errMsg);
    }
  }

  @Override
  public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
    if (cause.getMessage().equals("Connection reset by peer")) {
      // These errors are caused by the client and are safe to ignore
      return;
    }
    final Throwable rootCause = Throwables.getRootCause(cause);
    String message = "WF-301 Error while receiving data, reason: \""
        + cause.getMessage() + "\"";
    if (rootCause != null && rootCause.getMessage() != null) {
      message += ", root cause: \"" + rootCause.getMessage() + "\"";
    }
    InetSocketAddress remoteAddress = (InetSocketAddress) ctx.channel().remoteAddress();
    if (remoteAddress != null) {
      message += "; remote: " + remoteAddress.getHostString();
    }
    pointHandler.handleBlockedPoint(message);
  }
}
