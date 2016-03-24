package com.wavefront.agent;

import com.google.common.base.Function;
import com.google.common.base.Throwables;
import com.google.common.collect.Lists;

import com.wavefront.agent.api.ForceQueueEnabledAgentAPI;
import com.wavefront.ingester.Decoder;
import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.Counter;
import com.yammer.metrics.core.MetricName;

import org.apache.commons.lang.StringUtils;

import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import java.util.logging.Logger;
import java.util.regex.Pattern;

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

  private final Decoder decoder;
  private final String prefix;
  /**
   * Transformer to transform each line.
   */
  @Nullable
  private final Function<String, String> transformer;
  private final PointHandler pointHandler;

  @Nullable
  private final Pattern pointLineWhiteList;
  @Nullable
  private final Pattern pointLineBlackList;

  private final Counter regexRejects;

  private int blockedPointsPerBatch;

  public ChannelStringHandler(Decoder decoder, final ForceQueueEnabledAgentAPI agentAPI,
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
    this.pointHandler = new PointHandler(agentAPI, daemonId, port, logLevel, validationLevel,
        millisecondsPerBatch, blockedPointsPerBatch);

    this.prefix = prefix;
    this.blockedPointsPerBatch = blockedPointsPerBatch;
    this.transformer = transformer;

    this.pointLineWhiteList = StringUtils.isBlank(pointLineWhiteListRegex) ?
        null : Pattern.compile(pointLineWhiteListRegex);

    this.pointLineBlackList = StringUtils.isBlank(pointLineBlackListRegex) ?
        null : Pattern.compile(pointLineBlackListRegex);

    this.regexRejects = Metrics.newCounter(
        new MetricName("validationRegex." + String.valueOf(port), "", "points-rejected"));
  }

  public static final String PUSH_DATA_DELIMETER = "\n";

  public static List<String> unjoinPushData(String pushData) {
    return Arrays.asList(StringUtils.split(pushData, PUSH_DATA_DELIMETER));
  }

  public static String joinPushData(List<String> pushData) {
    return StringUtils.join(pushData, PUSH_DATA_DELIMETER);
  }

  protected boolean passesWhiteAndBlackLists(String pointLine) {
    if (pointLineWhiteList != null && !pointLineWhiteList.matcher(pointLine).matches() ||
        pointLineBlackList != null && pointLineBlackList.matcher(pointLine).matches()) {
      regexRejects.inc();
      return false;
    }
    return true;
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
    if (!passesWhiteAndBlackLists(pointLine)) {
      handleBlockedPoint(pointLine);
      return;
    }
    if (prefix != null) {
      pointLine = prefix + "." + msg;
    }
    List<ReportPoint> points = Lists.newArrayListWithExpectedSize(1);
    try {
      decoder.decodeReportPoints(pointLine, points, "dummy");
    } catch (Exception e) {
      final Throwable rootCause = Throwables.getRootCause(e);
      if (rootCause == null || rootCause.getMessage() == null) {
        final String message = "WF-300 Cannot parse: \"" + pointLine +
            "\", reason: \"" + e.getMessage() + "\"";
        handleBlockedPoint(message);
      } else {
        final String message = "WF-300 Cannot parse: \"" + pointLine +
            "\", reason: \"" + e.getMessage() +
            "\", root cause: \"" + rootCause.getMessage() + "\"";
        handleBlockedPoint(message);
      }
    }
    if (!points.isEmpty()) {
      ReportPoint point = points.get(0);
      pointHandler.reportPoint(point, pointLine);
    }
  }

  private void handleBlockedPoint(String pointLine) {
    final PostPushDataTimedTask randomPostTask = pointHandler.getRandomPostTask();
    if (randomPostTask.getBlockedSampleSize() < this.blockedPointsPerBatch) {
      randomPostTask.addBlockedSample(pointLine);
    }
    randomPostTask.incrementBlockedPoints();
  }

  @Override
  public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
    // ignore.
  }
}
