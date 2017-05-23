package com.wavefront.ingester;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToMessageDecoder;

/**
 * Given a raw graphite line, look for any host tag, and add it if implicit. Does not perform full
 * decoding, though.
 */
public class GraphiteHostAnnotator extends MessageToMessageDecoder<String> {

  private final String hostName;
  private final List<String> sourceTags = new ArrayList<>();

  public GraphiteHostAnnotator(String hostName, final List<String> customSourceTags) {
    this.hostName = hostName;
    this.sourceTags.add("source=");
    this.sourceTags.add("host=");
    this.sourceTags.addAll(customSourceTags.stream().map(customTag -> customTag + "=").collect(Collectors.toList()));
  }

  // Decode from a possibly host-annotated graphite string to a definitely host-annotated graphite string.
  @Override
  protected void decode(ChannelHandlerContext ctx, String msg, List<Object> out) throws Exception {
    for (String tag : sourceTags) {
      int strIndex = msg.indexOf(tag);
      // if a source tags is found and is followed by a non-whitespace tag value, add without change
      if (strIndex > -1 && msg.length() - strIndex - tag.length() > 0 && msg.charAt(strIndex + tag.length()) > ' ') {
        out.add(msg);
        return;
      }
    }
    out.add(msg + " source=\"" + hostName + "\"");
  }
}
