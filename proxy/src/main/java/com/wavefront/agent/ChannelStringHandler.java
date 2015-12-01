package com.wavefront.agent;

import com.google.common.base.Function;

import com.wavefront.agent.api.ForceQueueEnabledAgentAPI;
import com.wavefront.ingester.Decoder;
import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.Counter;
import com.yammer.metrics.core.MetricName;

import org.apache.commons.lang.StringUtils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
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

  private final Decoder decoder;
  private final List<ReportPoint> validatedPoints = new ArrayList<>();
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
                              final int pointsPerBatch,
                              final int blockedPointsPerBatch,
                              @Nullable final Function<String, String> transformer,
                              @Nullable final String pointLineWhiteListRegex,
                              @Nullable final String pointLineBlackListRegex) {
    this.decoder = decoder;
    this.pointHandler = new PointHandler(agentAPI, daemonId, port, logLevel, validationLevel,
        millisecondsPerBatch, pointsPerBatch, blockedPointsPerBatch);

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
    String pointLine = msg;

    // apply white/black lists after formatting, but before prefixing
    if (!passesWhiteAndBlackLists(pointLine)) {
      handleBlockedPoint(pointLine);
      return;
    }
    if (prefix != null) {
      pointLine = prefix + "." + msg;
    }
    if (validatedPoints.size() != 0) {
      validatedPoints.clear();
    }
    try {
      decoder.decodeReportPoints(pointLine, validatedPoints, "dummy");
    } catch (Exception e) {
      handleBlockedPoint(pointLine);
    }
    if (!validatedPoints.isEmpty()) {
      ReportPoint point = validatedPoints.get(0);
      pointHandler.reportPoint(point, pointLine);
    }
  }

  private void handleBlockedPoint(String pointLine) {
    if (pointHandler.sendDataTask.getBlockedSampleSize() < this.blockedPointsPerBatch) {
      pointHandler.sendDataTask.addBlockedSample(pointLine);
    }
    pointHandler.sendDataTask.incrementBlockedPoints();
  }

  @Override
  public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
    // ignore.
  }
}
