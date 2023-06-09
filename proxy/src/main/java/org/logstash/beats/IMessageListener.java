package org.logstash.beats;

import io.netty.channel.ChannelHandlerContext;

/**
 * This class is implemented in ruby in `lib/logstash/inputs/beats/message_listener`, this class is
 * used to link the events triggered from the different connection to the actual work inside the
 * plugin.
 */
public interface IMessageListener {
  /**
   * This is triggered on every new message parsed by the beats handler and should be executed in
   * the ruby world.
   */
  void onNewMessage(ChannelHandlerContext ctx, Message message);

  /**
   * Triggered when a new client connect to the input, this is used to link a connection to a codec
   * in the ruby world.
   */
  void onNewConnection(ChannelHandlerContext ctx);

  /**
   * Triggered when a connection is close on the remote end and we need to flush buffered events to
   * the queue.
   */
  void onConnectionClose(ChannelHandlerContext ctx);

  /**
   * Called went something bad occur in the pipeline, allow to clear buffered codec went somethign
   * goes wrong.
   */
  void onException(ChannelHandlerContext ctx, Throwable cause);

  /** Called when a error occur in the channel initialize, usually ssl handshake error. */
  void onChannelInitializeException(ChannelHandlerContext ctx, Throwable cause);
}
