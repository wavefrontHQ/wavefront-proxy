package com.wavefront.agent.channel;

import com.wavefront.agent.listeners.PortUnificationHandler;

import org.apache.commons.lang3.StringUtils;

import java.net.InetSocketAddress;
import java.nio.charset.Charset;
import java.util.List;
import java.util.logging.Logger;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.LineBasedFrameDecoder;

/**
 * Line-delimited decoder that has the ability of detecting when clients have disconnected while leaving some
 * data in the buffer.
 *
 * @author vasily@wavefront.com
 */
public class IncompleteLineDetectingLineBasedFrameDecoder extends LineBasedFrameDecoder {

  protected static final Logger logger = Logger.getLogger(
      IncompleteLineDetectingLineBasedFrameDecoder.class.getName());

  IncompleteLineDetectingLineBasedFrameDecoder(int maxLength) {
    super(maxLength, true, false);
  }

  @Override
  protected void decodeLast(ChannelHandlerContext ctx, ByteBuf in, List<Object> out) throws Exception {
    super.decodeLast(ctx, in, out);
    if (in.readableBytes() > 0) {
      String discardedData = in.readBytes(in.readableBytes()).toString(Charset.forName("UTF-8"));
      if (StringUtils.isNotBlank(discardedData)) {
        logger.warning("Client " + PortUnificationHandler.getRemoteName(ctx) +
            " disconnected, leaving unterminated string. Input (" + in.readableBytes() +
            " bytes) discarded: \"" + discardedData + "\"");
      }
    }
  }
}
