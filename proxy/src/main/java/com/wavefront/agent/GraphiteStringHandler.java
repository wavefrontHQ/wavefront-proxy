package com.wavefront.agent;

import com.wavefront.agent.api.ForceQueueEnabledAgentAPI;
import com.wavefront.agent.formatter.Formatter;
import com.wavefront.ingester.graphite.GraphiteDecoder;
import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.Counter;
import com.yammer.metrics.core.MetricName;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import org.apache.commons.lang.StringUtils;
import sunnylabs.report.ReportPoint;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import java.util.regex.Pattern;

/**
 * Adds all graphite strings to a working list, and batches them up on a set schedule (100ms) to
 * be sent (through the daemon's logic) up to the collector on the server side.
 */
@ChannelHandler.Sharable
public class GraphiteStringHandler extends SimpleChannelInboundHandler<String> {

  // formatted
  private GraphiteDecoder decoder = new GraphiteDecoder("unknown");

  private List<ReportPoint> validatedPoints = new ArrayList<>();

  private String prefix = null;
  private int blockedPointsPerBatch;
  private Formatter formatter;

  private final PointHandler pointHandler;

  private final Pattern pointLineWhiteList;
  private final Pattern pointLineBlackList;

  private final Counter regexRejects;

  public GraphiteStringHandler(final ForceQueueEnabledAgentAPI agentAPI,
                               final UUID daemonId,
                               final int port,
                               final String prefix,
                               final String logLevel,
                               final String validationLevel,
                               final long millisecondsPerBatch,
                               final int pointsPerBatch,
                               final int blockedPointsPerBatch,
                               final Formatter formatter,
                               @Nullable final String pointLineWhiteListRegex,
                               @Nullable final String pointLineBlackListRegex) {
    this.pointHandler = new PointHandler(agentAPI, daemonId, port, logLevel, validationLevel,
        millisecondsPerBatch, pointsPerBatch, blockedPointsPerBatch);

    this.prefix = prefix;
    this.blockedPointsPerBatch = blockedPointsPerBatch;
    this.formatter = formatter;

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
    if (formatter != null) {
      msg = formatter.format(msg);
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
      point.setTimestamp(Clock.now());
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
