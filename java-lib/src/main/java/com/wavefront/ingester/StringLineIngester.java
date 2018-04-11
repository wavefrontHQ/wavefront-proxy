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

  /**
   * Calculates the number of points in the pushData payload
   * @param pushData a delimited string with the points payload
   * @return number of points
   */
  public static int pushDataSize(String pushData) {
    int length = StringUtils.countMatches(pushData, PUSH_DATA_DELIMETER);
    return length > 0
        ? length + 1
        : (pushData.length() > 0 ? 1 : 0);

  }
}
