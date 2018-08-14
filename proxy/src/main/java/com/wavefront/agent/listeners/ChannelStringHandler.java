package com.wavefront.agent.listeners;

import com.google.common.base.Throwables;
import com.google.common.collect.Lists;

import com.wavefront.agent.PointHandler;
import com.wavefront.agent.PointHandlerImpl;
import com.wavefront.agent.preprocessor.ReportableEntityPreprocessor;
import com.wavefront.ingester.Decoder;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.List;
import java.util.logging.Level;
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
@Deprecated
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
  private final ReportableEntityPreprocessor preprocessor;
  private final PointHandler pointHandler;
  
  private final boolean logRawDataFlag;
  private double logRawDataRate;

  public ChannelStringHandler(Decoder<String> decoder,
                              final PointHandler pointhandler,
                              @Nullable final ReportableEntityPreprocessor preprocessor) {
    this.decoder = decoder;
    this.pointHandler = pointhandler;
    this.preprocessor = preprocessor;

    // check the property setting for logging raw data
    String logRawDataProperty = System.getProperty("wavefront.proxy.lograwdata");
    logRawDataFlag = logRawDataProperty != null && logRawDataProperty.equalsIgnoreCase("true");

    // make sure the rate fits between 0.0d - 1.0d
    if(logRawDataRate < 0.0d) {
      logRawDataRate = 0.0d;
    }
    else if(logRawDataRate > 1.0d) {
      logRawDataRate = 1.0d;
    }
    if(logRawDataRate > 0.0d && rawDataLogger.isLoggable(Level.FINEST)) {
      rawDataLogger.info("Raw data logging is enabled with " + (logRawDataRate * 100) + "% sampling");
    }
  }


  @Override
  protected void channelRead0(ChannelHandlerContext ctx, String msg) throws Exception {
    // use data rate to determine sampling rate
    // logging includes the source host and port
    if(logRawDataRate >= 1.0d || (logRawDataRate > 0.0d && RANDOM.nextDouble() < logRawDataRate)) {
      if(ctx.channel().remoteAddress() != null) {
        String hostAddress = ((InetSocketAddress) ctx.channel().remoteAddress()).getAddress().getHostAddress();
        int localPort = ((InetSocketAddress) ctx.channel().localAddress()).getPort();
        rawDataLogger.info(String.format("[%s>%d]%s", hostAddress, localPort, msg));
      } else {
        int localPort = ((InetSocketAddress) ctx.channel().localAddress()).getPort();
        rawDataLogger.info(String.format("[>%d]%s", localPort, msg));
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
                                      @Nullable final ReportableEntityPreprocessor preprocessor,
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
      if (rootCause != null && rootCause.getMessage() != null && rootCause != e) {
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
