package com.wavefront.agent.handlers;

import com.wavefront.agent.data.EntityProperties;
import com.wavefront.agent.data.QueueingReason;
import com.wavefront.agent.data.SourceTagSubmissionTask;
import com.wavefront.agent.data.TaskResult;
import com.wavefront.agent.queueing.TaskQueue;
import com.wavefront.api.SourceTagAPI;
import com.wavefront.dto.SourceTag;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * This class is responsible for accumulating the source tag changes and post it in a batch. This
 * class is similar to PostPushDataTimedTask.
 *
 * @author Suranjan Pramanik (suranjan@wavefront.com)
 * @author vasily@wavefront.com
 */
class SourceTagSenderTask extends AbstractSenderTask<SourceTag> {
  private static final Logger logger =
      Logger.getLogger(SourceTagSenderTask.class.getCanonicalName());

  private final SourceTagAPI proxyAPI;
  private final TaskQueue<SourceTagSubmissionTask> backlog;

  /**
   * Create new instance
   *
   * @param proxyAPI    handles interaction with Wavefront servers as well as queueing.
   * @param handlerKey  metrics pipeline handler key.
   * @param threadId    thread number.
   * @param properties  container for mutable proxy settings.
   * @param scheduler   executor service for this task
   * @param backlog     backing queue
   */
  SourceTagSenderTask(HandlerKey handlerKey, SourceTagAPI proxyAPI,
                      int threadId, EntityProperties properties,
                      ScheduledExecutorService scheduler,
                      TaskQueue<SourceTagSubmissionTask> backlog) {
    super(handlerKey, threadId, properties, scheduler);
    this.proxyAPI = proxyAPI;
    this.backlog = backlog;
  }

  @Override
  TaskResult processSingleBatch(List<SourceTag> batch) {
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public void run() {
    long nextRunMillis = properties.getPushFlushInterval();
    isSending = true;
    try {
      List<SourceTag> current = createBatch();
      if (current.size() == 0) return;
      Iterator<SourceTag> iterator = current.iterator();
      while (iterator.hasNext()) {
        if (rateLimiter == null || rateLimiter.tryAcquire()) {
          SourceTag tag = iterator.next();
          SourceTagSubmissionTask task = new SourceTagSubmissionTask(proxyAPI, properties,
              backlog, handlerKey.getHandle(), tag, null);
          TaskResult result = task.execute();
          this.attemptedCounter.inc();
          switch (result) {
            case DELIVERED:
              continue;
            case PERSISTED:
            case PERSISTED_RETRY:
              if (rateLimiter != null) rateLimiter.recyclePermits(1);
              continue;
            case RETRY_LATER:
              final List<SourceTag> remainingItems = new ArrayList<>();
              remainingItems.add(tag);
              iterator.forEachRemaining(remainingItems::add);
              undoBatch(remainingItems);
              if (rateLimiter != null) rateLimiter.recyclePermits(1);
              return;
            default:
          }
        } else {
          final List<SourceTag> remainingItems = new ArrayList<>();
          iterator.forEachRemaining(remainingItems::add);
          undoBatch(remainingItems);
          // if proxy rate limit exceeded, try again in 1/4..1/2 of flush interval
          // to introduce some degree of fairness.
          nextRunMillis = (int) (1 + Math.random()) * nextRunMillis / 4;
          final long willRetryIn = nextRunMillis;
          throttledLogger.log(Level.INFO, () -> "[" + handlerKey.getHandle() + " thread " +
              threadId + "]: WF-4 Proxy rate limiter " + "active (pending " +
              handlerKey.getEntityType() + ": " + datum.size() + "), will retry in " +
              willRetryIn + "ms");
          return;
        }
      }
    } catch (Throwable t) {
      logger.log(Level.SEVERE, "Unexpected error in flush loop", t);
    } finally {
      isSending = false;
      scheduler.schedule(this, nextRunMillis, TimeUnit.MILLISECONDS);
    }
  }

  @Override
  void flushSingleBatch(List<SourceTag> batch, @Nullable QueueingReason reason) {
    for (SourceTag tag : batch) {
      SourceTagSubmissionTask task = new SourceTagSubmissionTask(proxyAPI, properties, backlog,
          handlerKey.getHandle(), tag, null);
      task.enqueue(reason);
    }
  }
}
