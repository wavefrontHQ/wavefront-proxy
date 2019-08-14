package com.wavefront.ingester;

import com.google.common.base.Charsets;
import com.google.common.base.Function;

import java.util.ArrayList;
import java.util.List;

import javax.annotation.Nullable;

import io.netty.channel.Channel;
import io.netty.channel.ChannelHandler;
import io.netty.handler.codec.LineBasedFrameDecoder;
import io.netty.handler.codec.string.StringDecoder;

/**
 * Default Ingester thread that sets up decoders and a command handler to listen for metrics that
 * are string formatted lines on a port.
 */
@Deprecated
public class StringLineIngester extends TcpIngester {

  private static final int MAXIMUM_FRAME_LENGTH_DEFAULT = 4096;

  public StringLineIngester(List<Function<Channel, ChannelHandler>> decoders,
                            ChannelHandler commandHandler, int port, int maxLength) {
    super(createDecoderList(decoders, maxLength), commandHandler, port);
  }

  public StringLineIngester(List<Function<Channel, ChannelHandler>> decoders,
                            ChannelHandler commandHandler, int port) {
    this(decoders, commandHandler, port, MAXIMUM_FRAME_LENGTH_DEFAULT);
  }

  public StringLineIngester(ChannelHandler commandHandler, int port, int maxLength) {
    super(createDecoderList(null, maxLength), commandHandler, port);
  }

  public StringLineIngester(ChannelHandler commandHandler, int port) {
    this(commandHandler, port, MAXIMUM_FRAME_LENGTH_DEFAULT);
  }

  /**
   * Returns a copy of the given list plus inserts the 2 decoders needed for this specific ingester
   * (LineBasedFrameDecoder and StringDecoder)
   *
   * @param decoders  the starting list
   * @param maxLength maximum frame length for decoding the input stream
   * @return copy of the provided list with additional decodiers prepended
   */
  private static List<Function<Channel, ChannelHandler>> createDecoderList(@Nullable final List<Function<Channel,
      ChannelHandler>> decoders, int maxLength) {
    final List<Function<Channel, ChannelHandler>> copy;
    if (decoders == null) {
      copy = new ArrayList<>();
    } else {
      copy = new ArrayList<>(decoders);
    }
    copy.add(0, new Function<Channel, ChannelHandler>() {
      @Override
      public ChannelHandler apply(Channel input) {
        return new LineBasedFrameDecoder(maxLength, true, false);
      }
    });
    copy.add(1, new Function<Channel, ChannelHandler>() {
      @Override
      public ChannelHandler apply(Channel input) {
        return new StringDecoder(Charsets.UTF_8);
      }
    });

    return copy;
  }
}
