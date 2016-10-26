package com.wavefront.ingester;

import com.google.common.base.Charsets;
import com.google.common.base.Function;

import org.apache.commons.lang.StringUtils;

import java.util.ArrayList;
import java.util.Arrays;
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
public class StringLineIngester extends TcpIngester {

  private static final String PUSH_DATA_DELIMETER = "\n";

  public StringLineIngester(List<Function<Channel, ChannelHandler>> decoders,
                            ChannelHandler commandHandler, int port) {
    super(createDecoderList(decoders), commandHandler, port);
  }

  public StringLineIngester(ChannelHandler commandHandler, int port) {
    super(createDecoderList(null), commandHandler, port);
  }

  /**
   * Returns a copy of the given list plus inserts the 2 decoders needed for this specific ingester
   * (LineBasedFrameDecoder and StringDecoder)
   *
   * @param decoders the starting list
   * @return copy of the provided list with additional decodiers prepended
   */
  private static List<Function<Channel, ChannelHandler>> createDecoderList(@Nullable final List<Function<Channel, ChannelHandler>> decoders) {
    final List<Function<Channel, ChannelHandler>> copy;
    if (decoders == null) {
      copy = new ArrayList<>();
    } else {
      copy = new ArrayList<>(decoders);
    }
    copy.add(0, new Function<Channel, ChannelHandler>() {
      @Override
      public ChannelHandler apply(Channel input) {
        return new LineBasedFrameDecoder(4096, true, false);
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

  public static List<String> unjoinPushData(String pushData) {
    return Arrays.asList(StringUtils.split(pushData, PUSH_DATA_DELIMETER));
  }

  public static String joinPushData(List<String> pushData) {
    return StringUtils.join(pushData, PUSH_DATA_DELIMETER);
  }

  public static List<Integer> indexPushData(String pushData) {
    List<Integer> index = new ArrayList<>();
    index.add(0);
    int lastIndex = pushData.indexOf(PUSH_DATA_DELIMETER);
    final int delimiterLength = PUSH_DATA_DELIMETER.length();
    while (lastIndex != -1) {
      index.add(lastIndex);
      index.add(lastIndex + delimiterLength);
      lastIndex = pushData.indexOf(PUSH_DATA_DELIMETER, lastIndex + delimiterLength);
    }
    index.add(pushData.length());
    return index;
  }
}
