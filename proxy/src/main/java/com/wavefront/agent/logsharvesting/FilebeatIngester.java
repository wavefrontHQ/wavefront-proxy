package com.wavefront.agent.logsharvesting;

import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.Counter;
import com.yammer.metrics.core.Histogram;
import com.yammer.metrics.core.MetricName;
import io.netty.channel.ChannelHandlerContext;
import java.util.function.Supplier;
import org.logstash.beats.IMessageListener;
import org.logstash.beats.Message;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FilebeatIngester implements IMessageListener {
  protected static final Logger logger =
      LoggerFactory.getLogger(LogsIngester.class.getCanonicalName());
  private final LogsIngester logsIngester;
  private final Counter received;
  private final Counter malformed;
  private final Histogram drift;
  private final Supplier<Long> currentMillis;

  public FilebeatIngester(LogsIngester logsIngester, Supplier<Long> currentMillis) {
    this.logsIngester = logsIngester;
    this.received = Metrics.newCounter(new MetricName("logsharvesting", "", "filebeat-received"));
    this.malformed = Metrics.newCounter(new MetricName("logsharvesting", "", "filebeat-malformed"));
    this.drift = Metrics.newHistogram(new MetricName("logsharvesting", "", "filebeat-drift"));
    this.currentMillis = currentMillis;
  }

  @Override
  public void onNewMessage(ChannelHandlerContext ctx, Message message) {
    received.inc();
    FilebeatMessage filebeatMessage;
    try {
      filebeatMessage = new FilebeatMessage(message);
    } catch (MalformedMessageException exn) {
      logger.error("Malformed message received from filebeat, dropping (" + exn.getMessage() + ")");
      malformed.inc();
      return;
    }

    if (filebeatMessage.getTimestampMillis() != null) {
      drift.update(currentMillis.get() - filebeatMessage.getTimestampMillis());
    }

    logsIngester.ingestLog(filebeatMessage);
  }

  @Override
  public void onNewConnection(ChannelHandlerContext ctx) {
    logger.info("New filebeat connection.");
  }

  @Override
  public void onConnectionClose(ChannelHandlerContext ctx) {
    logger.info("Filebeat connection closed.");
  }

  @Override
  public void onException(ChannelHandlerContext ctx, Throwable cause) {
    logger.error("Caught error processing beats data.", cause);
  }

  @Override
  public void onChannelInitializeException(ChannelHandlerContext ctx, Throwable cause) {
    logger.error("Caught initializing beats data processor.", cause);
  }
}
