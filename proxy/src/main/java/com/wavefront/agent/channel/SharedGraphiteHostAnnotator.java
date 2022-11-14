package com.wavefront.agent.channel;

import static com.wavefront.agent.channel.ChannelUtils.getRemoteAddress;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Streams;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import java.net.InetAddress;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * Given a raw Graphite/Wavefront line, look for any host tag, and add it if implicit.
 *
 * <p>Differences from GraphiteHostAnnotator: - sharable - lazy load - does not proactively perform
 * rDNS lookups unless needed - can be applied to HTTP payloads
 */
@ChannelHandler.Sharable
public class SharedGraphiteHostAnnotator {
  private static final List<String> DEFAULT_SOURCE_TAGS =
      ImmutableList.of("source", "host", "\"source\"", "\"host\"");

  private final Function<InetAddress, String> hostnameResolver;
  private final List<String> sourceTags;
  private final List<String> sourceTagsJson;

  public SharedGraphiteHostAnnotator(
      @Nullable List<String> customSourceTags,
      @Nonnull Function<InetAddress, String> hostnameResolver) {
    if (customSourceTags == null) {
      customSourceTags = ImmutableList.of();
    }
    this.hostnameResolver = hostnameResolver;
    this.sourceTags =
        Streams.concat(DEFAULT_SOURCE_TAGS.stream(), customSourceTags.stream())
            .map(customTag -> customTag + "=")
            .collect(Collectors.toList());
    this.sourceTagsJson =
        Streams.concat(
                DEFAULT_SOURCE_TAGS.subList(2, 4).stream(),
                customSourceTags.stream().map(customTag -> "\"" + customTag + "\""))
            .collect(Collectors.toList());
  }

  public String apply(ChannelHandlerContext ctx, String msg) {
    return apply(ctx, msg, false);
  }

  public String apply(ChannelHandlerContext ctx, String msg, boolean addAsJsonProperty) {
    List<String> defaultSourceTags = addAsJsonProperty ? sourceTagsJson : sourceTags;
    for (int i = 0; i < defaultSourceTags.size(); i++) {
      String tag = defaultSourceTags.get(i);
      int strIndex = msg.indexOf(tag);
      // if a source tags is found and is followed by a non-whitespace tag value, add without
      // change
      if (strIndex > -1
          && msg.length() - strIndex - tag.length() > 0
          && msg.charAt(strIndex + tag.length()) > ' ') {
        return msg;
      }
    }

    String sourceValue = "\"" + hostnameResolver.apply(getRemoteAddress(ctx)) + "\"";

    if (addAsJsonProperty) {
      return msg.replaceFirst("\\{", "{\"source\":" + sourceValue + ", ");
    } else {
      return msg + " source=" + sourceValue;
    }
  }
}
