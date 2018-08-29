package com.wavefront.agent.listeners;

import com.google.common.base.Throwables;
import com.google.common.collect.Lists;

import com.wavefront.agent.PointHandler;
import com.wavefront.agent.PointHandlerImpl;
import com.wavefront.agent.preprocessor.ReportableEntityPreprocessor;
import com.wavefront.ingester.Decoder;
import com.wavefront.ingester.GraphiteDecoder;

import java.net.InetSocketAddress;
import java.util.Collections;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.annotation.Nullable;

import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import wavefront.report.ReportPoint;


/**
 * Channel handler for byte array data.
 * @author Mike McLaughlin (mike@wavefront.com)
 */
@ChannelHandler.Sharable
public class ChannelByteArrayHandler extends SimpleChannelInboundHandler<byte[]> {
  private static final Logger logger = Logger.getLogger(ChannelByteArrayHandler.class.getCanonicalName());
  private static final Logger blockedPointsLogger = Logger.getLogger("RawBlockedPoints");

  private final Decoder<byte[]> decoder;
  private final PointHandler pointHandler;

  @Nullable
  private final ReportableEntityPreprocessor preprocessor;
  private final GraphiteDecoder recoder;

  /**
   * Constructor.
   */
  public ChannelByteArrayHandler(Decoder<byte[]> decoder,
                          final PointHandler pointHandler,
                          @Nullable final ReportableEntityPreprocessor preprocessor) {
    this.decoder = decoder;
    this.pointHandler = pointHandler;
    this.preprocessor = preprocessor;
    this.recoder = new GraphiteDecoder(Collections.emptyList());
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
      for (ReportPoint point: points) {
        if (preprocessor != null && preprocessor.forPointLine().hasTransformers()) {
          String pointLine = PointHandlerImpl.pointToString(point);
          pointLine = preprocessor.forPointLine().transform(pointLine);
          List<ReportPoint> parsedPoints = Lists.newArrayListWithExpectedSize(1);
          recoder.decodeReportPoints(pointLine, parsedPoints, "dummy");
          parsedPoints.forEach(this::preprocessAndReportPoint);
        } else {
          preprocessAndReportPoint(point);
        }
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

  private void preprocessAndReportPoint(ReportPoint point) {
    if (preprocessor == null) {
      pointHandler.reportPoint(point, point.getMetric());
      return;
    }
    // backwards compatibility: apply "pointLine" rules to metric name
    if (!preprocessor.forPointLine().filter(point.getMetric())) {
      if (preprocessor.forPointLine().getLastFilterResult() != null) {
        blockedPointsLogger.warning(PointHandlerImpl.pointToString(point));
      } else {
        blockedPointsLogger.info(PointHandlerImpl.pointToString(point));
      }
      pointHandler.handleBlockedPoint(preprocessor.forPointLine().getLastFilterResult());
      return;
    }
    preprocessor.forReportPoint().transform(point);
    if (!preprocessor.forReportPoint().filter(point)) {
      if (preprocessor.forReportPoint().getLastFilterResult() != null) {
        blockedPointsLogger.warning(PointHandlerImpl.pointToString(point));
      } else {
        blockedPointsLogger.info(PointHandlerImpl.pointToString(point));
      }
      pointHandler.handleBlockedPoint(preprocessor.forReportPoint().getLastFilterResult());
      return;
    }
    pointHandler.reportPoint(point, point.getMetric());
  }

  @Override
  public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
    if (cause.getMessage().contains("Connection reset by peer")) {
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
