package com.wavefront.agent.buffer;

import com.google.common.util.concurrent.RecyclableRateLimiter;
import com.google.common.util.concurrent.RecyclableRateLimiterImpl;
import com.google.common.util.concurrent.RecyclableRateLimiterWithMetrics;

public class RatedBridge implements Runnable {

  private final Buffer src;
  private final Buffer dst;
  private final QueueInfo key;
  private final RecyclableRateLimiter rate;

  public static void createNewBridge(Buffer src, Buffer dst, QueueInfo key) {
    RatedBridge bridge = new RatedBridge(src, dst, key);
    for (int i = 0; i < 3; i++) {
      new Thread(bridge, "RatedBridge." + i + "." + key.getQueue()).start();
    }
  }

  public RatedBridge(Buffer src, Buffer dst, QueueInfo key) {
    this.src = src;
    this.dst = dst;
    this.key = key;
    this.rate =
        new RecyclableRateLimiterWithMetrics(
            RecyclableRateLimiterImpl.create(100, 1), "RatedBridge." + key.getQueue());
  }

  @Override
  public void run() {
    while (true) {
      src.onMsgBatch(
          key,
          1000,
          rate,
          batch -> {
            dst.sendMsg(key, batch);
          });
    }
  }
}
