package com.wavefront.agent.handlers;

import com.wavefront.agent.buffer.BuffersManager;
import com.wavefront.agent.data.EntityProperties;
import java.util.List;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

abstract class AbstractSenderTask implements SenderTask, Runnable {
  private static final Logger logger =
      Logger.getLogger(AbstractSenderTask.class.getCanonicalName());
  private HandlerKey handlerKey;
  private EntityProperties properties;
  private ScheduledExecutorService scheduler;
  private boolean isRunning;

  AbstractSenderTask(
      HandlerKey handlerKey,
      int threadId,
      EntityProperties properties,
      ScheduledExecutorService scheduler) {
    this.handlerKey = handlerKey;
    this.properties = properties;
    this.scheduler = scheduler;
  }

  @Override
  public void run() {
    // TODO: review getDataPerBatch and getRateLimiter
    BuffersManager.onMsgBatch(
        handlerKey, properties.getDataPerBatch(), properties.getRateLimiter(), this::processBatch);
    if (isRunning) {
      scheduler.schedule(this, 1000, TimeUnit.MILLISECONDS);
    }
  }

  @Override
  public void start() {
    if (!isRunning) {
      isRunning = true;
      this.scheduler.schedule(this, properties.getPushFlushInterval(), TimeUnit.MILLISECONDS);
    }
  }

  @Override
  public void stop() {
    isRunning = false;
    scheduler.shutdown();
  }

  private void processBatch(List<String> batch) throws Exception {
    int result = processSingleBatch(batch);
    if (result != 0) {
      // TODO: review Exception
      throw new Exception("Error rending point to the server, error code:" + result);
    }
  }
}
