package com.wavefront.agent.channel;

import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.http.HttpMessage;
import io.netty.handler.codec.http.HttpObjectAggregator;
import io.netty.handler.codec.http.HttpRequest;

/**
 * A {@link HttpObjectAggregator} that correctly tracks HTTP 413 returned
 * for incoming payloads that are too large.
 *
 * @author vasily@wavefront.com
 */
public class StatusTrackingHttpObjectAggregator extends HttpObjectAggregator {

  public StatusTrackingHttpObjectAggregator(int maxContentLength) {
    super(maxContentLength);
  }

  @Override
  protected void handleOversizedMessage(ChannelHandlerContext ctx, HttpMessage oversized)
      throws Exception {
    if (oversized instanceof HttpRequest) {
      ChannelUtils.getHttpStatusCounter(ctx, 413).inc();
    }
    super.handleOversizedMessage(ctx, oversized);
  }
}
