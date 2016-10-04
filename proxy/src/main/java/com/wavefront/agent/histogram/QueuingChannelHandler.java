package com.wavefront.agent.histogram;

import com.google.common.base.Preconditions;

import com.squareup.tape.ObjectQueue;

import java.util.ArrayList;
import java.util.List;

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
  private final ObjectQueue<List<T>> tape;
  private List<T> buffer;
  private final int maxCapacity;


  public QueuingChannelHandler(@NotNull ObjectQueue<List<T>> tape, int maxCapacity) {
    Preconditions.checkNotNull(tape);
    Preconditions.checkArgument(maxCapacity > 0);
    this.tape = tape;
    this.buffer = new ArrayList<>(maxCapacity);
    this.maxCapacity = maxCapacity;
  }

  private void ship() {
    synchronized (this) {
      if (!buffer.isEmpty()) {
        tape.add(buffer);
        buffer = new ArrayList<>(maxCapacity);
      }
    }
  }

  private void innerAdd(T t) {
    synchronized (this) {
      buffer.add(t);
      if (buffer.size() >= maxCapacity) {
        ship();
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
