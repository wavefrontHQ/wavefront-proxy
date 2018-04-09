package com.wavefront.agent;

import com.google.common.base.Throwables;
import com.google.common.collect.Lists;

import com.wavefront.agent.preprocessor.PointPreprocessor;
import com.wavefront.ingester.Decoder;
import com.wavefront.ingester.SourceTagDecoder;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.List;
import java.util.logging.Logger;
import java.util.Random;

import javax.annotation.Nullable;

import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;

import org.apache.commons.lang.math.NumberUtils;
import wavefront.report.ReportPoint;

/**
 * Parses points from a channel using the given decoder and send it off to the AgentAPI interface.
 *
 * @author Clement Pang (clement@wavefront.com).
 */
@ChannelHandler.Sharable
public class ChannelStringHandler extends SimpleChannelInboundHandler<String> {

  private static final Logger blockedPointsLogger = Logger.getLogger("RawBlockedPoints");
  private static final Logger rawDataLogger = Logger.getLogger("RawDataLogger");

  private final Decoder<String> decoder;
  private static final Random RANDOM = new Random();

  /**
   * Transformer to transform each line.
   */
  @Nullable
  private final PointPreprocessor preprocessor;
  private final PointHandler pointHandler;
  private final SourceTagHandler metadataHandler;
  private double logRawDataRate;

  public ChannelStringHandler(Decoder<String> decoder,
                              final PointHandler pointhandler,
                              @Nullable final PointPreprocessor preprocessor,
                              SourceTagHandler metadataHandler) {
    this.decoder = decoder;
    this.pointHandler = pointhandler;
    this.preprocessor = preprocessor;
    this.metadataHandler = metadataHandler;

    // new property rate
    String logRawDataRateProperty = System.getProperty("wavefront.proxy.lograwdata");
    logRawDataRate = logRawDataRateProperty != null && NumberUtils.isNumber(logRawDataRateProperty) ? Double.parseDouble(logRawDataRateProperty) : 0.0d;

    // make sure the rate fits between 0.0d - 1.0d
    if(logRawDataRate < 0.0d) {
      logRawDataRate = 0.0d;
    }
    else if(logRawDataRate > 1.0d) {
      logRawDataRate = 1.0d;
    }
    if(logRawDataRate > 0.0d) {
      rawDataLogger.info("Raw data logging is enabled with " + (logRawDataRate * 100) + "% sampling");
    }
  }

  public ChannelStringHandler(Decoder<String> decoder,
                              final PointHandler pointhandler,
                              @Nullable final PointPreprocessor preprocessor) {
    this(decoder, pointhandler, preprocessor, null);
  }

  @Override
  protected void channelRead0(ChannelHandlerContext ctx, String msg) throws Exception {
    // based on the keyword - consider it a point or a metadata
    if (metadataHandler != null && msg != null) {
      msg = msg.trim();
      if (msg.startsWith(SourceTagDecoder.SOURCE_TAG) ||
          msg.startsWith(SourceTagDecoder.SOURCE_DESCRIPTION)) {
        metadataHandler.processSourceTag(msg);
        return;
      }
    }
    // if msg does not match metadata keywords then treat it as a point
    // use data rate to determine sampling rate
    // logging includes the source host and port
    if(logRawDataRate >= 1.0d || (logRawDataRate > 0.0d && RANDOM.nextDouble() < logRawDataRate)) {
      if(ctx.channel().remoteAddress() != null) {
        String host = ((InetSocketAddress) ctx.channel().remoteAddress()).getHostName();
        int port = ((InetSocketAddress) ctx.channel().remoteAddress()).getPort();
        rawDataLogger.info(String.format("[%s:%d]%s", host, port, msg));
      } else {
        rawDataLogger.info(String.format("[:]%s", msg));
      }
    }
    processPointLine(msg, decoder, pointHandler, preprocessor, ctx);
  }

  /**
   * This probably belongs in a base class.  It's only done like this so it can be easily re-used. This should be
   * refactored when it's clear where it belongs.
   */
  public static void processPointLine(final String message,
                                      Decoder<String> decoder,
                                      final PointHandler pointHandler,
                                      @Nullable final PointPreprocessor preprocessor,
                                      @Nullable final ChannelHandlerContext ctx) {
    // ignore empty lines.
    if (message == null) return;
    String pointLine = message.trim();
    if (pointLine.isEmpty()) return;




    // transform the line if needed
    if (preprocessor != null) {
      pointLine = preprocessor.forPointLine().transform(pointLine);

      // apply white/black lists after formatting
      if (!preprocessor.forPointLine().filter(pointLine)) {
        if (preprocessor.forPointLine().getLastFilterResult() != null) {
          blockedPointsLogger.warning(pointLine);
        } else {
          blockedPointsLogger.info(pointLine);
        }
        pointHandler.handleBlockedPoint(preprocessor.forPointLine().getLastFilterResult());
        return;
      }
    }

    // decode the line into report points
    List<ReportPoint> points = Lists.newArrayListWithExpectedSize(1);
    try {
      decoder.decodeReportPoints(pointLine, points, "dummy");
    } catch (Exception e) {
      final Throwable rootCause = Throwables.getRootCause(e);
      String errMsg = "WF-300 Cannot parse: \"" + pointLine +
          "\", reason: \"" + e.getMessage() + "\"";
      if (rootCause != null && rootCause.getMessage() != null) {
        errMsg = errMsg + ", root cause: \"" + rootCause.getMessage() + "\"";
      }
      if (ctx != null) {
        InetSocketAddress remoteAddress = (InetSocketAddress) ctx.channel().remoteAddress();
        if (remoteAddress != null) {
          errMsg += "; remote: " + remoteAddress.getHostString();
        }
      }
      blockedPointsLogger.warning(pointLine);
      pointHandler.handleBlockedPoint(errMsg);
      return;
    }

    // transform the point after parsing, and apply additional white/black lists if any
    if (preprocessor != null) {
      for (ReportPoint point : points) {
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
      }
    }
    pointHandler.reportPoints(points);
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
