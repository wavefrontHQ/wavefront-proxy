package com.wavefront.agent.channel;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Streams;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.net.InetAddress;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;

import static com.wavefront.agent.channel.ChannelUtils.getRemoteAddress;

/**
 * Given a raw Graphite/Wavefront line, look for any host tag, and add it if implicit.
 *
 * Differences from GraphiteHostAnnotator:
 * - sharable
 * - lazy load - does not proactively perform rDNS lookups unless needed
 * - can be applied to HTTP payloads
 *
 * @author vasily@wavefront.com
 */
@ChannelHandler.Sharable
public class SharedGraphiteHostAnnotator {
  private static final List<String> DEFAULT_SOURCE_TAGS = ImmutableList.of("source",
      "host", "\"source\"", "\"host\"");

  private final Function<InetAddress, String> hostnameResolver;
  private final List<String> sourceTags;

  public SharedGraphiteHostAnnotator(@Nullable List<String> customSourceTags,
                                     @Nonnull Function<InetAddress, String> hostnameResolver) {
    if (customSourceTags == null) {
      customSourceTags = ImmutableList.of();
    }
    this.hostnameResolver = hostnameResolver;
    this.sourceTags = Streams.concat(DEFAULT_SOURCE_TAGS.stream(), customSourceTags.stream()).
        map(customTag -> customTag + "=").collect(Collectors.toList());
  }

  public String apply(ChannelHandlerContext ctx, String msg) {
    for (int i = 0; i < sourceTags.size(); i++) {
      String tag = sourceTags.get(i);
      int strIndex = msg.indexOf(tag);
      // if a source tags is found and is followed by a non-whitespace tag value, add without change
      if (strIndex > -1 && msg.length() - strIndex - tag.length() > 0 &&
          msg.charAt(strIndex + tag.length()) > ' ') {
        return msg;
      }
    }
    return msg + " source=\"" + hostnameResolver.apply(getRemoteAddress(ctx)) + "\"";
  }
}
