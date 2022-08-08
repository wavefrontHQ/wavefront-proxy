package com.wavefront.agent.channel;

import static org.easymock.EasyMock.createMock;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.replay;
import static org.junit.Assert.assertEquals;

import com.google.common.collect.ImmutableList;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import org.junit.Test;

/** @author vasily@wavefront.com */
public class SharedGraphiteHostAnnotatorTest {

  @Test
  public void testHostAnnotator() throws Exception {
    ChannelHandlerContext ctx = createMock(ChannelHandlerContext.class);
    Channel channel = createMock(Channel.class);
    InetSocketAddress remote = new InetSocketAddress(InetAddress.getLocalHost(), 2878);
    expect(ctx.channel()).andReturn(channel).anyTimes();
    expect(channel.remoteAddress()).andReturn(remote).anyTimes();
    replay(channel, ctx);
    SharedGraphiteHostAnnotator annotator =
        new SharedGraphiteHostAnnotator(ImmutableList.of("tag1", "tag2", "tag3"), x -> "default");

    String point;
    point = "request.count 1 source=test.wavefront.com";
    assertEquals(point, annotator.apply(ctx, point));
    point = "\"request.count\" 1 \"source\"=\"test.wavefront.com\"";
    assertEquals(point, annotator.apply(ctx, point));
    point = "request.count 1 host=test.wavefront.com";
    assertEquals(point, annotator.apply(ctx, point));
    point = "request.count 1 \"host\"=test.wavefront.com";
    assertEquals(point, annotator.apply(ctx, point));
    point = "request.count 1 tag1=test.wavefront.com";
    assertEquals(point, annotator.apply(ctx, point));
    point = "request.count 1 tag2=\"test.wavefront.com\"";
    assertEquals(point, annotator.apply(ctx, point));
    point = "request.count 1 tag3=test.wavefront.com";
    assertEquals(point, annotator.apply(ctx, point));
    point = "request.count 1 tag4=test.wavefront.com";
    assertEquals(
        "request.count 1 tag4=test.wavefront.com source=\"default\"", annotator.apply(ctx, point));
    String log = "{\"tag4\":\"test.wavefront.com\"}";
    assertEquals(
        "{\"source\":\"default\", \"tag4\":\"test.wavefront.com\"}",
        annotator.apply(ctx, log, true));
  }
}
