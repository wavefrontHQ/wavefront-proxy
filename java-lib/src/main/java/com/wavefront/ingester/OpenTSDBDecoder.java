package com.wavefront.ingester;

import com.google.common.base.Preconditions;

import java.util.List;

import io.netty.util.CharsetUtil;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import sunnylabs.report.ReportPoint;

/**
 * OpenTSDB decoder that takes in a point of the type:
 *
 * PUT [metric] [timestamp] [value] [annotations]
 *
 * @author Clement Pang (clement@wavefront.com).
 */
public class OpenTSDBDecoder implements Decoder<String> {

  private final String hostName;
  private static final IngesterFormatter FORMAT = IngesterFormatter.newBuilder().whiteSpace()
      .appendCaseInsensitiveLiteral("put").whiteSpace()
      .appendMetricName().whiteSpace()
      .appendTimestamp().whiteSpace()
      .appendValue().whiteSpace()
      .appendAnnotationsConsumer().whiteSpace().build();
  private List<String> customSourceTags;

  public OpenTSDBDecoder(List<String> customSourceTags) {
    this.hostName = "unknown";
    Preconditions.checkNotNull(customSourceTags);
    this.customSourceTags = customSourceTags;
  }

  public OpenTSDBDecoder(String hostName, List<String> customSourceTags) {
    Preconditions.checkNotNull(hostName);
    this.hostName = hostName;
    Preconditions.checkNotNull(customSourceTags);
    this.customSourceTags = customSourceTags;
  }

  @Override
  public void decodeReportPoints(ChannelHandlerContext ctx, String msg, List<ReportPoint> out, String customerId) {
    if (msg.startsWith("version")) {
      ByteBuf buf = Unpooled.copiedBuffer("Wavefront proxy OpenTSDB text protocol implementation\n", CharsetUtil.UTF_8);
      ctx.writeAndFlush(buf);
      return;
    }
    ReportPoint point = FORMAT.drive(msg, hostName, customerId, customSourceTags);
    if (out != null) {
      out.add(point);
    }
  }

  @Override
  public void decodeReportPoints(ChannelHandlerContext ctx, String msg, List<ReportPoint> out) {
    decodeReportPoints(ctx, msg, out, "dummy");
  }
}
