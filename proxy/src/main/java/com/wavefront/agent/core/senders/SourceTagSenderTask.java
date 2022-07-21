package com.wavefront.agent.core.senders;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.wavefront.agent.core.buffers.Buffer;
import com.wavefront.agent.core.buffers.BuffersManager;
import com.wavefront.agent.core.queues.QueueInfo;
import com.wavefront.agent.core.queues.QueueStats;
import com.wavefront.agent.data.EntityProperties;
import com.wavefront.agent.data.SourceTagSubmissionTask;
import com.wavefront.api.SourceTagAPI;
import com.wavefront.dto.SourceTag;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.logging.Logger;

/**
 * This class is responsible for accumulating the source tag changes and post it in a batch. This
 * class is similar to PostPushDataTimedTask.
 */
public class SourceTagSenderTask extends AbstractSenderTask {
  private static final Logger log = Logger.getLogger(SourceTagSenderTask.class.getCanonicalName());

  private final QueueInfo queue;
  private final SourceTagAPI proxyAPI;
  private EntityProperties properties;
  private QueueStats queueStats;

  /**
   * Create new instance
   *
   * @param queue metrics pipeline handler key.
   * @param proxyAPI handles interaction with Wavefront servers as well as queueing.
   * @param properties container for mutable proxy settings.
   * @param buffer
   * @param queueStats
   */
  SourceTagSenderTask(
      QueueInfo queue,
      int idx,
      SourceTagAPI proxyAPI,
      EntityProperties properties,
      Buffer buffer,
      QueueStats queueStats) {
    super(queue, idx, properties, buffer);
    this.queue = queue;
    this.proxyAPI = proxyAPI;
    this.properties = properties;
    this.queueStats = queueStats;
  }

  @Override
  public int processSingleBatch(List<String> batch) {

    ObjectMapper objectMapper = new ObjectMapper();

    Iterator<String> iterator = batch.iterator();
    while (iterator.hasNext()) {
      String sourceTagStr = iterator.next();
      try {
        SourceTag tag = objectMapper.readValue(sourceTagStr, SourceTag.class);
        SourceTagSubmissionTask task =
            new SourceTagSubmissionTask(proxyAPI, properties, queue, tag, null, queueStats);
        int res = task.execute();
        if (res != 0) {
          // if there is a communication problem, we send back the point to the buffer
          final List<String> remainingItems = new ArrayList<>();
          remainingItems.add(sourceTagStr);
          BuffersManager.sendMsg(queue, sourceTagStr);
          iterator.forEachRemaining(s -> BuffersManager.sendMsg(queue, s));
        }
      } catch (JsonProcessingException e) {
        log.severe("Error parsing a SourceTag point. " + e);
      }
    }
    return 0;
  }

  //   TODO: review
  //  @Override
  //  public void run() {
  //    //    long nextRunMillis = properties.getPushFlushInterval();
  //    //    isSending = true;
  //    //    try {
  //    //      List<SourceTag> current = createBatch();
  //    //      if (current.size() == 0) return;
  //    //      Iterator<SourceTag> iterator = current.iterator();
  //    //      while (iterator.hasNext()) {
  //    //        if (rateLimiter == null || rateLimiter.tryAcquire()) {
  //    //          SourceTag tag = iterator.next();
  //    //          SourceTagSubmissionTask task =
  //    //              new SourceTagSubmissionTask(
  //    //                  proxyAPI, properties, backlog, handlerKey.getHandle(), tag, null);
  //    //          TaskResult result = task.execute();
  //    //          this.attemptedCounter.inc();
  //    //          switch (result) {
  //    //            case DELIVERED:
  //    //              continue;
  //    //            case PERSISTED:
  //    //            case PERSISTED_RETRY:
  //    //              if (rateLimiter != null) rateLimiter.recyclePermits(1);
  //    //              continue;
  //    //            case RETRY_LATER:
  //    //              final List<SourceTag> remainingItems = new ArrayList<>();
  //    //              remainingItems.add(tag);
  //    //              iterator.forEachRemaining(remainingItems::add);
  //    //              undoBatch(remainingItems);
  //    //              if (rateLimiter != null) rateLimiter.recyclePermits(1);
  //    //              return;
  //    //            default:
  //    //          }
  //    //        } else {
  //    //          final List<SourceTag> remainingItems = new ArrayList<>();
  //    //          iterator.forEachRemaining(remainingItems::add);
  //    //          undoBatch(remainingItems);
  //    //          // if proxy rate limit exceeded, try again in 1/4..1/2 of flush interval
  //    //          // to introduce some degree of fairness.
  //    //          nextRunMillis = (int) (1 + Math.random()) * nextRunMillis / 4;
  //    //          final long willRetryIn = nextRunMillis;
  //    //          throttledLogger.log(
  //    //              Level.INFO,
  //    //              () ->
  //    //                  "["
  //    //                      + handlerKey.getHandle()
  //    //                      + " thread "
  //    //                      + threadId
  //    //                      + "]: WF-4 Proxy rate limiter "
  //    //                      + "active (pending "
  //    //                      + handlerKey.getEntityType()
  //    //                      + ": "
  //    //                      + "datum.size()"
  //    //                      + "), will retry in "
  //    //                      + willRetryIn
  //    //                      + "ms");
  //    //          return;
  //    //        }
  //    //      }
  //    //    } catch (Throwable t) {
  //    //      logger.log(Level.SEVERE, "Unexpected error in flush loop", t);
  //    //    } finally {
  //    //      isSending = false;
  //    //      scheduler.schedule(this, nextRunMillis, TimeUnit.MILLISECONDS);
  //    //    }
  //  }

  //  @Override
  //  void flushSingleBatch(List<SourceTag> batch, @Nullable QueueingReason reason) {
  //    for (SourceTag tag : batch) {
  //      SourceTagSubmissionTask task =
  //          new SourceTagSubmissionTask(
  //              proxyAPI, properties, backlog, handlerKey.getHandle(), tag, null);
  //      task.enqueue(reason);
  //    }
  //  }

}
