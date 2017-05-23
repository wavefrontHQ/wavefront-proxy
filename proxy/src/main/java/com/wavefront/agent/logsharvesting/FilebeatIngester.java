package com.wavefront.agent.logsharvesting;

import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.Counter;
import com.yammer.metrics.core.Histogram;
import com.yammer.metrics.core.MetricName;

import org.logstash.beats.IMessageListener;
import org.logstash.beats.Message;

import java.util.function.Supplier;
import java.util.logging.Level;
import java.util.logging.Logger;

import io.netty.channel.ChannelHandlerContext;

/**
 * @author Mori Bellamy (mori@wavefront.com)
 */
public class FilebeatIngester implements IMessageListener {
  protected static final Logger logger = Logger.getLogger(LogsIngester.class.getCanonicalName());
  private LogsIngester logsIngester;
  private Counter received, malformed;
  private Histogram drift;
  private Supplier<Long> currentMillis;

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
      logger.severe("Malformed message received from filebeat, dropping.");
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
    logger.log(Level.SEVERE, "Caught error processing beats data.", cause);
  }

  @Override
  public void onChannelInitializeException(ChannelHandlerContext ctx, Throwable cause) {
    logger.log(Level.SEVERE, "Caught initializing beats data processor.", cause);
  }
}
