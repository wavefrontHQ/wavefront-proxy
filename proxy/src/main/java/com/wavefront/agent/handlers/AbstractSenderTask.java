package com.wavefront.agent.handlers;

import com.wavefront.agent.data.EntityProperties;
import java.util.concurrent.ScheduledExecutorService;
import java.util.logging.Logger;

abstract class AbstractSenderTask implements SenderTask, Runnable {
  private static final Logger logger =
      Logger.getLogger(AbstractSenderTask.class.getCanonicalName());

  AbstractSenderTask(
      HandlerKey handlerKey,
      int threadId,
      EntityProperties properties,
      ScheduledExecutorService scheduler) {}

  @Override
  public void run() {}

  @Override
  public void start() {}

  @Override
  public void stop() {}
}
