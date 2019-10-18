package com.wavefront.agent.channel;

import com.google.common.collect.Lists;

import java.net.InetAddress;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;

import static com.wavefront.agent.channel.CachingHostnameLookupResolver.getRemoteAddress;

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

  private final Function<InetAddress, String> hostnameResolver;
  private final List<String> sourceTags;

  public SharedGraphiteHostAnnotator(@Nullable final List<String> customSourceTags,
                                     @Nonnull Function<InetAddress, String> hostnameResolver) {
    this.hostnameResolver = hostnameResolver;
    this.sourceTags = Lists.newArrayListWithExpectedSize(customSourceTags == null ? 4 : customSourceTags.size() + 4);
    this.sourceTags.add("source=");
    this.sourceTags.add("source\"=");
    this.sourceTags.add("host=");
    this.sourceTags.add("host\"=");
    if (customSourceTags != null) {
      this.sourceTags.addAll(customSourceTags.stream().map(customTag -> customTag + "=").collect(Collectors.toList()));
    }
  }

  public String apply(ChannelHandlerContext ctx, String msg) {
    for (String tag : sourceTags) {
      int strIndex = msg.indexOf(tag);
      // if a source tags is found and is followed by a non-whitespace tag value, add without change
      if (strIndex > -1 && msg.length() - strIndex - tag.length() > 0 && msg.charAt(strIndex + tag.length()) > ' ') {
        return msg;
      }
    }
    return msg + " source=\"" + hostnameResolver.apply(getRemoteAddress(ctx)) + "\"";
  }
}
