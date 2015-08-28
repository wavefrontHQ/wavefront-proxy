package com.wavefront.ingester.graphite;

import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToMessageDecoder;

import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Given a raw graphite line, look for any host tag, and add it if implicit. Do not perform full decoding, though.
 */
public class GraphiteHostAnnotator extends MessageToMessageDecoder<String> {

  // Host names may have spaces, but the first character must exist and be non-whitespace
  private static final Pattern HOST_EXISTENCE_PATTERN = Pattern.compile("(.+)(host=[^\\s].+)",
      Pattern.CASE_INSENSITIVE);
  private static final Pattern SOURCE_EXISTENCE_PATTERN = Pattern.compile("(.+)(source=[^\\s].+)",
      Pattern.CASE_INSENSITIVE);
  private final String hostName;

  public GraphiteHostAnnotator(String hostName) {
    this.hostName = hostName;
  }

  // Decode from a possibly host-annotated graphite string to a definitely host-annotated graphite string.
  @Override
  protected void decode(ChannelHandlerContext ctx, String msg, List<Object> out) throws Exception {
    Matcher m = HOST_EXISTENCE_PATTERN.matcher(msg);
    if (m.matches()) {
      // Has a host; add without change
      out.add(msg);
    } else {
      m = SOURCE_EXISTENCE_PATTERN.matcher(msg);
      if (m.matches()) {
        out.add(msg);
      } else {
        // No host? Add what we got from the socket
        out.add(msg + " source=" + hostName);
      }
    }
  }
}
