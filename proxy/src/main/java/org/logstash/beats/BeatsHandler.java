package org.logstash.beats;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.wavefront.common.Utils;
import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.Counter;
import com.yammer.metrics.core.MetricName;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import java.net.InetSocketAddress;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.net.ssl.SSLHandshakeException;

@ChannelHandler.Sharable
public class BeatsHandler extends SimpleChannelInboundHandler<Batch> {
  private static final Logger logger = Logger.getLogger(BeatsHandler.class.getCanonicalName());
  private final IMessageListener messageListener;
  private final Supplier<Counter> duplicateBatchesIgnored =
      Utils.lazySupplier(
          () ->
              Metrics.newCounter(
                  new MetricName("logsharvesting", "", "filebeat-duplicate-batches")));
  private final Cache<String, BatchIdentity> batchDedupeCache =
      Caffeine.newBuilder().expireAfterAccess(1, TimeUnit.HOURS).build();

  public BeatsHandler(IMessageListener listener) {
    messageListener = listener;
  }

  @Override
  public void channelActive(final ChannelHandlerContext ctx) throws Exception {
    if (logger.isLoggable(Level.FINEST)) {
      logger.finest(format(ctx, "Channel Active"));
    }
    super.channelActive(ctx);
    messageListener.onNewConnection(ctx);
  }

  @Override
  public void channelInactive(ChannelHandlerContext ctx) throws Exception {
    super.channelInactive(ctx);
    if (logger.isLoggable(Level.FINEST)) {
      logger.finest(format(ctx, "Channel Inactive"));
    }
    messageListener.onConnectionClose(ctx);
  }

  @Override
  public void channelRead0(ChannelHandlerContext ctx, Batch batch) throws Exception {
    if (logger.isLoggable(Level.FINE)) {
      logger.fine(format(ctx, "Received a new payload"));
    }
    try {
      boolean isFirstMessage = true;
      String key;
      BatchIdentity value;
      for (Message message : batch) {
        if (isFirstMessage) {
          // check whether we've processed that batch already
          isFirstMessage = false;
          key = BatchIdentity.keyFrom(message);
          value = BatchIdentity.valueFrom(message);
          if (key != null && value != null) {
            BatchIdentity cached = batchDedupeCache.getIfPresent(key);
            if (value.equals(cached)) {
              duplicateBatchesIgnored.get().inc();
              if (logger.isLoggable(Level.FINE)) {
                logger.fine(format(ctx, "Duplicate filebeat batch received, ignoring"));
              }
              // ack the entire batch and stop processing the rest of it
              writeAck(
                  ctx, message.getBatch().getProtocol(), message.getBatch().getHighestSequence());
              break;
            } else {
              batchDedupeCache.put(key, value);
            }
          }
        }
        if (logger.isLoggable(Level.FINE)) {
          logger.fine(
              format(
                  ctx,
                  "Sending a new message for the listener, sequence: " + message.getSequence()));
        }
        messageListener.onNewMessage(ctx, message);

        if (needAck(message)) {
          ack(ctx, message);
        }
      }
    } finally {
      // this channel is done processing this payload, instruct the connection handler to stop
      // sending TCP keep alive
      ctx.channel().attr(ConnectionHandler.CHANNEL_SEND_KEEP_ALIVE).get().set(false);
      if (logger.isLoggable(Level.FINE)) {
        logger.log(Level.FINE, () -> String.format(
            "%s: batches pending: %s",
            ctx.channel().id().asShortText(),
            ctx.channel().attr(ConnectionHandler.CHANNEL_SEND_KEEP_ALIVE).get().get()));
      }
      batch.release();
      ctx.flush();
    }
  }

  /*
   * Do not propagate the SSL handshake exception down to the ruby layer handle it locally instead and close the connection
   * if the channel is still active. Calling `onException` will flush the content of the codec's buffer, this call
   * may block the thread in the event loop until completion, this should only affect LS 5 because it still supports
   * the multiline codec, v6 drop support for buffering codec in the beats input.
   *
   * For v5, I cannot drop the content of the buffer because this will create data loss because multiline content can
   * overlap Filebeat transmission; we were recommending multiline at the source in v5 and in v6 we enforce it.
   */
  @Override
  public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
    try {
      if (!(cause instanceof SSLHandshakeException)) {
        messageListener.onException(ctx, cause);
      }
      String causeMessage =
          cause.getMessage() == null ? cause.getClass().toString() : cause.getMessage();

      if (logger.isLoggable(Level.FINE)) {
        logger.log(Level.FINE, format(ctx, "Handling exception: " + causeMessage), cause);
      }
      logger.info(format(ctx, "Handling exception: " + causeMessage));
    } finally {
      super.exceptionCaught(ctx, cause);
      ctx.flush();
      ctx.close();
    }
  }

  private boolean needAck(Message message) {
    return message.getSequence() == message.getBatch().getHighestSequence();
  }

  private void ack(ChannelHandlerContext ctx, Message message) {
    if (logger.isLoggable(Level.FINEST)) {
      logger.finest(format(ctx, "Acking message number " + message.getSequence()));
    }
    writeAck(ctx, message.getBatch().getProtocol(), message.getSequence());
    writeAck(ctx, message.getBatch().getProtocol(), 0); // send blank ack
  }

  private void writeAck(ChannelHandlerContext ctx, byte protocol, int sequence) {
    ctx.writeAndFlush(new Ack(protocol, sequence))
        .addListener(
            (ChannelFutureListener)
                channelFuture -> {
                  if (channelFuture.isSuccess() && logger.isLoggable(Level.FINEST) && sequence > 0) {
                    logger.finest(format(ctx, "Ack complete for message number " + sequence));
                  }
                });
  }

  /*
   * There is no easy way in Netty to support MDC directly,
   * we will use similar logic than Netty's LoggingHandler
   */
  private String format(ChannelHandlerContext ctx, String message) {
    InetSocketAddress local = (InetSocketAddress) ctx.channel().localAddress();
    InetSocketAddress remote = (InetSocketAddress) ctx.channel().remoteAddress();

    String localhost;
    if (local != null) {
      localhost = local.getAddress().getHostAddress() + ":" + local.getPort();
    } else {
      localhost = "undefined";
    }

    String remotehost;
    if (remote != null) {
      remotehost = remote.getAddress().getHostAddress() + ":" + remote.getPort();
    } else {
      remotehost = "undefined";
    }

    return "[local: " + localhost + ", remote: " + remotehost + "] " + message;
  }
}
