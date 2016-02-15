package com.wavefront.ingester;

import java.util.LinkedHashSet;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToMessageDecoder;

/**
 * Given a raw graphite line, look for any host tag, and add it if implicit. Does not perform full
 * decoding, though.
 */
public class GraphiteHostAnnotator extends MessageToMessageDecoder<String> {

  private final Pattern sourceExistencePattern;
  private final String hostName;

  public GraphiteHostAnnotator(String hostName, LinkedHashSet<String> customSourceTags) {
    this.hostName = hostName;
    StringBuffer pattern = new StringBuffer(".+(");
    for (String tag : customSourceTags) {
      pattern.append(tag);
      pattern.append("|");
    }
    pattern.append("source|host)=[^\\s].+");
    sourceExistencePattern = Pattern.compile(pattern.toString(), Pattern.CASE_INSENSITIVE);
  }

  // Decode from a possibly host-annotated graphite string to a definitely host-annotated graphite string.
  @Override
  protected void decode(ChannelHandlerContext ctx, String msg, List<Object> out) throws Exception {
    Matcher m = sourceExistencePattern.matcher(msg);
    if (m.matches()) {
      // Has a source; add without change
      out.add(msg);
    } else {
      out.add(msg + " source=" + hostName);
    }
  }
}
