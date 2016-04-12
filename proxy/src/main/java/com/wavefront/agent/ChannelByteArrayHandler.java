package com.wavefront.agent;

import com.google.common.base.Function;
import com.google.common.base.Throwables;
import com.google.common.collect.Lists;

import com.wavefront.agent.api.ForceQueueEnabledAgentAPI;
import com.wavefront.common.MetricWhiteBlackList;
import com.wavefront.ingester.Decoder;

import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import org.apache.commons.lang.StringUtils;
import sunnylabs.report.ReportPoint;

import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.annotation.Nullable;


/**
 * Channel handler for byte array data.
 */
@ChannelHandler.Sharable
public class ChannelByteArrayHandler extends SimpleChannelInboundHandler<byte[]> {
  private static final Logger logger = Logger.getLogger(
      ChannelByteArrayHandler.class.getCanonicalName());

  private final Decoder<byte[]> decoder;
  private final String prefix;
  private final PointHandler pointHandler;

  private MetricWhiteBlackList whiteBlackList;

  /**
   * Constructor.
   */
  public ChannelByteArrayHandler(Decoder<byte[]> decoder,
                                 final ForceQueueEnabledAgentAPI agentApi,
                                 final UUID daemonId,
                                 final int port,
                                 final String prefix,
                                 final String logLevel,
                                 final String validationLevel,
                                 final long millisecondsPerBatch,
                                 final int blockedPointsPerBatch,
                                 @Nullable final String pointLineWhiteListRegex,
                                 @Nullable final String pointLineBlackListRegex) {
    this.decoder = decoder;
    this.pointHandler = new PointHandler(agentApi, daemonId, port, logLevel,
                                         validationLevel, millisecondsPerBatch,
                                         blockedPointsPerBatch);

    this.prefix = prefix;
    this.whiteBlackList = new MetricWhiteBlackList(
      pointLineWhiteListRegex, pointLineBlackListRegex, String.valueOf(port));
  }

  @Override
  protected void channelRead0(ChannelHandlerContext ctx, byte[] msg) throws Exception {
    // ignore empty lines.
    if (msg == null || msg.length == 0) {
      return;
    }

    List<ReportPoint> points = Lists.newArrayListWithExpectedSize(1);
    try {
      decoder.decodeReportPoints(ctx, msg, points, "dummy");
      for (final ReportPoint point: points) {
        if (!this.whiteBlackList.passes(point.getMetric())) {
          pointHandler.handleBlockedPoint(point.getMetric());
          continue;
        }
        if (prefix != null) {
          point.setMetric(prefix + "." + point.getMetric());
        }
        pointHandler.reportPoint(point, point.getMetric());
      }
    } catch (final Exception any) {
      logger.log(Level.WARNING, "Failed to read:\n" + new String(msg), any);
      final Throwable rootCause = Throwables.getRootCause(any);
      if (rootCause == null || rootCause.getMessage() == null) {
        final String message = "WF-300 Cannot parse: \"" + new String(msg)
            + "\", reason: \"" + any.getMessage() + "\"";
        pointHandler.handleBlockedPoint(message);
      } else {
        final String message = "WF-300 Cannot parse: \"" + new String(msg)
            + "\", reason: \"" + any.getMessage()
            + "\", root cause: \"" + rootCause.getMessage() + "\"";
        pointHandler.handleBlockedPoint(message);
      }
    }
  }

  @Override
  public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
    final Throwable rootCause = Throwables.getRootCause(cause);
    if (rootCause == null || rootCause.getMessage() == null) {
      final String message = "WF-301 Channel Handler Failed, reason: \""
          + cause.getMessage() + "\"";
      pointHandler.handleBlockedPoint(message);
    } else {
      final String message = "WF-301 Channel Handler Failed, reason: \""
          + cause.getMessage() + "\", root cause: \"" + rootCause.getMessage() + "\"";
      pointHandler.handleBlockedPoint(message);
    }
  }
}
