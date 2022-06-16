package com.wavefront.agent.handlers;

import com.wavefront.agent.buffer.BuffersManager;
import com.wavefront.agent.data.EntityProperties;
import com.wavefront.agent.data.TaskResult;
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

  abstract TaskResult processSingleBatch(List<String> batch);

  @Override
  public void run() {
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
    TaskResult result = processSingleBatch(batch);
    switch (result) {
      case DELIVERED:
        break;
      case PERSISTED:
      case PERSISTED_RETRY:
      case RETRY_LATER:
      default:
        throw new Exception("error"); // TODO: review Exception
    }
  }
}
