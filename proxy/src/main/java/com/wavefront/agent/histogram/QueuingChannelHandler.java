package com.wavefront.agent.histogram;

import com.google.common.base.Preconditions;

import com.squareup.tape.ObjectQueue;
import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.Counter;
import com.yammer.metrics.core.MetricName;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Logger;

import javax.validation.constraints.NotNull;

import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;

/**
 * Inbound handler streaming a netty channel out to a square tape.
 *
 * @author Tim Schmidt (tim@wavefront.com).
 */
@ChannelHandler.Sharable
public class QueuingChannelHandler<T> extends SimpleChannelInboundHandler<Object> {
  protected static final Logger logger = Logger.getLogger(QueuingChannelHandler.class.getCanonicalName());
  private final ObjectQueue<List<T>> tape;
  private List<T> buffer;
  private final int maxCapacity;
  private final AtomicBoolean histogramDisabled;
  // Have a counter that goes from 0 to 25 and then resets to 0
  // this is just to avoid excessive logging ...
  private int channelReadCounter = 0;
  private final Counter discardedHistogramPointsCounter = Metrics.newCounter(new MetricName(
      "histogram.ingester.disabled", "", "discarded_points"));

  public QueuingChannelHandler(@NotNull ObjectQueue<List<T>> tape, int maxCapacity, AtomicBoolean histogramDisabled) {
    Preconditions.checkNotNull(tape);
    Preconditions.checkArgument(maxCapacity > 0);
    this.tape = tape;
    this.buffer = new ArrayList<>();
    this.maxCapacity = maxCapacity;
    this.histogramDisabled = histogramDisabled;
  }

  private void ship() {
    List<T> bufferCopy = null;
    synchronized (this) {
      int blockSize;
      if (!buffer.isEmpty()) {
        blockSize = Math.min(buffer.size(), maxCapacity);
        bufferCopy = buffer.subList(0, blockSize);
        buffer = new ArrayList<>(buffer.subList(blockSize, buffer.size()));
      }
    }
    if (bufferCopy != null && bufferCopy.size() > 0) {
      tape.add(bufferCopy);
    }
  }

  private void innerAdd(T t) {
    if (!histogramDisabled.get()) {
      // if histogram feature is disabled on the server increment counter and log it every 25 times ...
      discardedHistogramPointsCounter.inc();
      if (channelReadCounter == 0) {
        logger.info("Ingested point discarded because histogram feature is disabled on the server");
      }
      channelReadCounter = (channelReadCounter + 1) % 25;
    } else {
      // histograms are not disabled on the server, so add the input to the buffer
      synchronized (this) {
        buffer.add(t);
      }
    }
  }

  @Override
  protected void channelRead0(ChannelHandlerContext channelHandlerContext, Object t) throws Exception {
    if (t != null) {
      innerAdd((T) t);
    }
  }

  public Runnable getBufferFlushTask() {
    return QueuingChannelHandler.this::ship;
  }
}
