package com.wavefront.agent.buffer;

import com.google.common.util.concurrent.RecyclableRateLimiter;
import com.google.common.util.concurrent.RecyclableRateLimiterImpl;
import com.google.common.util.concurrent.RecyclableRateLimiterWithMetrics;
import com.wavefront.common.NamedThreadFactory;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class RatedBridge implements Runnable {

  private final Buffer src;
  private final Buffer dst;
  private final QueueInfo key;
  private final RecyclableRateLimiter rate;

  public static void createNewBridge(Buffer src, Buffer dst, QueueInfo key, double rateLimit) {
    RatedBridge bridge = new RatedBridge(src, dst, key, rateLimit);
    ScheduledExecutorService exec =
        Executors.newScheduledThreadPool(
            3, new NamedThreadFactory("RatedBridge." + key.getQueue()));
    exec.scheduleAtFixedRate(bridge, 0, 1, TimeUnit.SECONDS);
    exec.scheduleAtFixedRate(bridge, 0, 1, TimeUnit.SECONDS);
    exec.scheduleAtFixedRate(bridge, 0, 1, TimeUnit.SECONDS);
  }

  public RatedBridge(Buffer src, Buffer dst, QueueInfo key, double rateLimit) {
    this.src = src;
    this.dst = dst;
    this.key = key;
    this.rate =
        new RecyclableRateLimiterWithMetrics(
            RecyclableRateLimiterImpl.create(rateLimit, 1), "RatedBridge-" + key.getQueue());
  }

  @Override
  public void run() {
    src.onMsgBatch(
        key,
        1000,
        rate,
        batch -> {
          //          dst.sendMsg(key, batch);
        });
  }
}
