package com.wavefront.agent.channel;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.LineBasedFrameDecoder;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.function.Consumer;
import javax.annotation.Nonnull;
import org.apache.commons.lang3.StringUtils;

/**
 * Line-delimited decoder that has the ability of detecting when clients have disconnected while
 * leaving some data in the buffer.
 */
public class IncompleteLineDetectingLineBasedFrameDecoder extends LineBasedFrameDecoder {
  private final Consumer<String> warningMessageConsumer;

  IncompleteLineDetectingLineBasedFrameDecoder(
      @Nonnull Consumer<String> warningMessageConsumer, int maxLength) {
    super(maxLength, true, false);
    this.warningMessageConsumer = warningMessageConsumer;
  }

  @Override
  protected void decodeLast(ChannelHandlerContext ctx, ByteBuf in, List<Object> out)
      throws Exception {
    super.decodeLast(ctx, in, out);
    int readableBytes = in.readableBytes();
    if (readableBytes > 0) {
      String discardedData = in.readBytes(readableBytes).toString(StandardCharsets.UTF_8);
      if (StringUtils.isNotBlank(discardedData)) {
        warningMessageConsumer.accept(
            "Client "
                + ChannelUtils.getRemoteName(ctx)
                + " disconnected, leaving unterminated string. Input ("
                + readableBytes
                + " bytes) discarded: \""
                + discardedData
                + "\"");
      }
    }
  }
}
