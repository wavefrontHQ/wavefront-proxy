package com.wavefront.agent;

import com.google.common.base.Function;
import com.google.common.base.Throwables;
import com.google.common.collect.Lists;

import com.wavefront.agent.api.ForceQueueEnabledAgentAPI;
import com.wavefront.ingester.Decoder;
import com.wavefront.common.MetricWhiteBlackList;

import org.apache.commons.lang.StringUtils;

import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import java.util.logging.Logger;

import javax.annotation.Nullable;

import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import sunnylabs.report.ReportPoint;

/**
 * Parses points from a channel using the given decoder and send it off to the AgentAPI interface.
 *
 * @author Clement Pang (clement@wavefront.com).
 */
@ChannelHandler.Sharable
public class ChannelStringHandler extends SimpleChannelInboundHandler<String> {

  private static final Logger logger = Logger.getLogger(ChannelStringHandler.class.getCanonicalName());

  private final Decoder<String> decoder;
  private final String prefix;
  /**
   * Transformer to transform each line.
   */
  @Nullable
  private final Function<String, String> transformer;
  private final PointHandler pointHandler;

  private MetricWhiteBlackList whiteBlackList;

  public ChannelStringHandler(Decoder<String> decoder,
                              final ForceQueueEnabledAgentAPI agentAPI,
                              final UUID daemonId,
                              final int port,
                              final String prefix,
                              final String logLevel,
                              final String validationLevel,
                              final long millisecondsPerBatch,
                              final int blockedPointsPerBatch,
                              @Nullable final Function<String, String> transformer,
                              @Nullable final String pointLineWhiteListRegex,
                              @Nullable final String pointLineBlackListRegex) {
    this.decoder = decoder;
    this.pointHandler = new PointHandler(agentAPI, daemonId, port, logLevel, validationLevel, millisecondsPerBatch, blockedPointsPerBatch);

    this.prefix = prefix;
    this.transformer = transformer;
    this.whiteBlackList = new MetricWhiteBlackList(pointLineWhiteListRegex,
                                                   pointLineBlackListRegex,
                                                   String.valueOf(port));
  }

  public static final String PUSH_DATA_DELIMETER = "\n";

  public static List<String> unjoinPushData(String pushData) {
    return Arrays.asList(StringUtils.split(pushData, PUSH_DATA_DELIMETER));
  }

  public static String joinPushData(List<String> pushData) {
    return StringUtils.join(pushData, PUSH_DATA_DELIMETER);
  }

  @Override
  protected void channelRead0(ChannelHandlerContext ctx, String msg) throws Exception {
    // ignore empty lines.
    if (msg == null || msg.trim().length() == 0) return;
    if (transformer != null) {
      msg = transformer.apply(msg);
    }
    String pointLine = msg.trim();

    // apply white/black lists after formatting, but before prefixing
    if (!this.whiteBlackList.passes(pointLine)) {
      pointHandler.handleBlockedPoint(pointLine);
      return;
    }
    List<ReportPoint> points = Lists.newArrayListWithExpectedSize(1);
    try {
      decoder.decodeReportPoints(ctx, pointLine, points, "dummy");
    } catch (Exception e) {
      final Throwable rootCause = Throwables.getRootCause(e);
      if (rootCause == null || rootCause.getMessage() == null) {
        final String message = "WF-300 Cannot parse: \"" + pointLine +
            "\", reason: \"" + e.getMessage() + "\"";
        pointHandler.handleBlockedPoint(message);
      } else {
        final String message = "WF-300 Cannot parse: \"" + pointLine +
            "\", reason: \"" + e.getMessage() +
            "\", root cause: \"" + rootCause.getMessage() + "\"";
        pointHandler.handleBlockedPoint(message);
      }
    }
    if (!points.isEmpty()) {
      ReportPoint point = points.get(0);
      if (prefix != null) {
        point.setMetric(prefix + "." + point.getMetric());
      }
      pointHandler.reportPoint(point, pointLine);
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
