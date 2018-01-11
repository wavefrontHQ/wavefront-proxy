package com.wavefront.agent;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.base.Throwables;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.AtomicDouble;
import com.google.common.util.concurrent.RateLimiter;
import com.google.common.util.concurrent.RecyclableRateLimiter;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import com.fasterxml.jackson.databind.JsonNode;
import com.github.benmanes.caffeine.cache.CacheLoader;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;
import com.squareup.tape.FileException;
import com.squareup.tape.FileObjectQueue;
import com.squareup.tape.ObjectQueue;
import com.wavefront.agent.api.ForceQueueEnabledAgentAPI;
import com.wavefront.api.WavefrontAPI;
import com.wavefront.api.agent.AgentConfiguration;
import com.wavefront.api.agent.ShellOutputDTO;
import com.wavefront.ingester.StringLineIngester;
import com.wavefront.metrics.ExpectedAgentMetric;
import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.Counter;
import com.yammer.metrics.core.Gauge;
import com.yammer.metrics.core.Histogram;
import com.yammer.metrics.core.Meter;
import com.yammer.metrics.core.MetricName;
import com.yammer.metrics.core.MetricsRegistry;

import net.jpountz.lz4.LZ4BlockInputStream;
import net.jpountz.lz4.LZ4BlockOutputStream;

import org.apache.commons.lang.ArrayUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.exception.ExceptionUtils;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.ws.rs.core.Response;

import static com.google.common.collect.ImmutableList.of;

/**
 * A wrapper for any WavefrontAPI that queues up any result posting if the backend is not available.
 * Current data will always be submitted right away (thus prioritizing live data) while background
 * threads will submit backlogged data.
 *
 * @author Clement Pang (clement@wavefront.com)
 */
public class QueuedAgentService implements ForceQueueEnabledAgentAPI {

  private static final Logger logger = Logger.getLogger(QueuedAgentService.class.getCanonicalName());
  private static final String SERVER_ERROR = "Server error";

  private final Gson resubmissionTaskMarshaller;
  private final WavefrontAPI wrapped;
  private final List<ResubmissionTaskQueue> taskQueues;
  private static AtomicInteger splitBatchSize = new AtomicInteger(50000);
  private static AtomicDouble retryBackoffBaseSeconds = new AtomicDouble(2.0);
  private boolean lastKnownQueueSizeIsPositive = true;
  private boolean lastKnownSourceTagQueueSizeIsPositive = true;
  private final ExecutorService executorService;

  /**
   *  A loading cache for tracking queue sizes (refreshed once a minute). Calculating the number of objects across
   *  all queues can be a non-trivial operation, hence the once-a-minute refresh.
   */
  private final LoadingCache<ResubmissionTaskQueue, AtomicInteger> queueSizes;
  private final List<ResubmissionTaskQueue> sourceTagTaskQueues;

  private MetricsRegistry metricsRegistry = new MetricsRegistry();
  private Meter resultPostingMeter = metricsRegistry.newMeter(QueuedAgentService.class, "post-result", "results",
      TimeUnit.MINUTES);
  private Counter permitsGranted = Metrics.newCounter(new MetricName("limiter", "", "permits-granted"));
  private Counter permitsDenied = Metrics.newCounter(new MetricName("limiter", "", "permits-denied"));
  private Counter permitsRetried = Metrics.newCounter(new MetricName("limiter", "", "permits-retried"));
  private final AtomicLong queuePointsCount = new AtomicLong();
  private Gauge queuedPointsCountGauge = null;
  /**
   * Biases result sizes to the last 5 minutes heavily. This histogram does not see all result
   * sizes. The executor only ever processes one posting at any given time and drops the rest.
   * {@link #resultPostingMeter} records the actual rate (i.e. sees all posting calls).
   */
  private Histogram resultPostingSizes = metricsRegistry.newHistogram(QueuedAgentService.class, "result-size", true);
  /**
   * A single threaded bounded work queue to update result posting sizes.
   */
  private ExecutorService resultPostingSizerExecutorService = new ThreadPoolExecutor(1, 1, 60L, TimeUnit.SECONDS,
      new ArrayBlockingQueue<Runnable>(1), new NamedThreadFactory("result-posting-sizer"));

  /**
   * Only size postings once every 5 seconds.
   */
  private final RateLimiter resultSizingRateLimier = RateLimiter.create(0.2);
  /**
   * @return bytes per minute for requests submissions. Null if no data is available yet.
   */
  @Nullable
  public Long getBytesPerMinute() {
    if (resultPostingMeter.fifteenMinuteRate() == 0 || resultPostingSizes.mean() == 0 || resultPostingSizes.count() <
        50) {
      return null;
    }
    return (long) (resultPostingSizes.mean() * resultPostingMeter.fifteenMinuteRate());
  }

  @Deprecated
  public QueuedAgentService(WavefrontAPI service, String bufferFile, final int retryThreads,
                            final ScheduledExecutorService executorService, boolean purge,
                            final UUID agentId, final boolean splitPushWhenRateLimited,
                            final String logLevel) throws IOException {
    this(service, bufferFile, retryThreads, executorService, purge,
        agentId, splitPushWhenRateLimited, (RecyclableRateLimiter) null);
  }


  public QueuedAgentService(WavefrontAPI service, String bufferFile, final int retryThreads,
                            final ScheduledExecutorService executorService, boolean purge,
                            final UUID agentId, final boolean splitPushWhenRateLimited,
                            final RecyclableRateLimiter pushRateLimiter)
      throws IOException {
    if (retryThreads <= 0) {
      logger.severe("You have no retry threads set up. Any points that get rejected will be lost.\n Change this by " +
          "setting retryThreads to a value > 0");
    }
    if (pushRateLimiter != null) {
      logger.info("Pushing to Wavefront with average PPS: " + String.valueOf(pushRateLimiter.getRate()));
    } else {
      logger.info("Pushing to Wavefront without user defined rate limit.");
    }
    resubmissionTaskMarshaller = new GsonBuilder().
        registerTypeHierarchyAdapter(ResubmissionTask.class, new ResubmissionTaskDeserializer()).create();
    this.wrapped = service;
    this.taskQueues = Lists.newArrayListWithExpectedSize(retryThreads);
    this.sourceTagTaskQueues = Lists.newArrayListWithExpectedSize(retryThreads);
    String bufferFileSourceTag = bufferFile + "SourceTag";
    this.executorService = executorService;

    queueSizes = Caffeine.newBuilder()
        .refreshAfterWrite(15, TimeUnit.SECONDS)
        .build(new CacheLoader<ResubmissionTaskQueue, AtomicInteger>() {
                 @Override
                 public AtomicInteger load(@Nonnull ResubmissionTaskQueue key) throws Exception {
                   return new AtomicInteger(key.size());
                 }

                 // reuse old object if possible
                 @Override
                 public AtomicInteger reload(@Nonnull ResubmissionTaskQueue key,
                                             @Nonnull AtomicInteger oldValue) {
                   oldValue.set(key.size());
                   return oldValue;
                 }
               });

    for (int i = 0; i < retryThreads; i++) {
      final int threadId = i;
      File buffer = new File(bufferFile + "." + i);
      File bufferSourceTag = new File(bufferFileSourceTag + "." + i);
      if (purge) {
        if (buffer.delete()) {
          logger.warning("Retry buffer has been purged: " + buffer.getAbsolutePath());
        }
        if (bufferSourceTag.delete()) {
          logger.warning("SourceTag retry buffer has been purged: " + bufferSourceTag
              .getAbsolutePath());
        }
      }

      ObjectQueue<ResubmissionTask> queue = createTaskQueue(agentId, buffer);

      // Having two proxy processes write to the same buffer file simultaneously causes buffer file corruption.
      // To prevent concurrent access from another process, we try to obtain exclusive access to each buffer file
      // trylock() is platform-specific so there is no iron-clad guarantee, but it works well in most cases
      try {
        FileChannel channel = new RandomAccessFile(buffer, "rw").getChannel();
        Preconditions.checkNotNull(channel.tryLock()); // fail if tryLock() returns null (lock couldn't be acquired)
      } catch (Exception e) {
        logger.severe("WF-005: Error requesting exclusive access to the buffer file " + bufferFile + "." + i +
            " - please make sure that no other processes access this file and restart the proxy");
        System.exit(-1);
      }

      final ResubmissionTaskQueue taskQueue = new ResubmissionTaskQueue(queue,
          task -> {
            task.service = wrapped;
            task.currentAgentId = agentId;
          }
      );

      Runnable taskRunnable = createRunnable(executorService, splitPushWhenRateLimited, threadId, taskQueue, pushRateLimiter);

      executorService.schedule(taskRunnable, (long) (Math.random() * retryThreads), TimeUnit.SECONDS);
      taskQueues.add(taskQueue);

      ObjectQueue<ResubmissionTask> sourceTagTaskQueue = createTaskQueue(agentId,
          bufferSourceTag);

      // Having two proxy processes write to the same buffer file simultaneously causes buffer
      // file corruption. To prevent concurrent access from another process, we try to obtain
      // exclusive access to each buffer file trylock() is platform-specific so there is no
      // iron-clad guarantee, but it works well in most cases
      try {
        FileChannel channel = new RandomAccessFile(bufferSourceTag, "rw").getChannel();
        Preconditions.checkNotNull(channel.tryLock()); // fail if tryLock() returns null (lock
        // couldn't be acquired)
      } catch (Exception e) {
        logger.severe("WF-005: Error requesting exclusive access to the buffer file " +
            bufferFileSourceTag + "." + i +
            " - please make sure that no other processes access this file and restart the proxy");
        System.exit(-1);
      }
      final ResubmissionTaskQueue sourceTagQueue = new ResubmissionTaskQueue(sourceTagTaskQueue,
            task -> {
              task.service = wrapped;
              task.currentAgentId = agentId;
            }
          );
      // create a new rate-limiter for the source tag retry queue, because the API calls are
      // rate-limited on the server-side as well. We don't want the retry logic to keep hitting
      // that rate-limit.
      Runnable sourceTagTaskRunnable = createRunnable(executorService, splitPushWhenRateLimited,
          threadId, sourceTagQueue, RecyclableRateLimiter.create(1, 1));
      executorService.schedule(sourceTagTaskRunnable, (long) (Math.random() * retryThreads),
          TimeUnit.SECONDS);
      sourceTagTaskQueues.add(sourceTagQueue);
    }

    if (retryThreads > 0) {
      executorService.scheduleAtFixedRate(() -> {
        try {
          Supplier<Stream<Integer>> sizes = () -> taskQueues.stream()
              .map(k -> Math.max(0, queueSizes.get(k).intValue()));
          if (sizes.get().anyMatch(i -> i > 0)) {
            lastKnownQueueSizeIsPositive = true;
            logger.info("current retry queue sizes: [" +
                sizes.get().map(Object::toString).collect(Collectors.joining("/")) + "]");
          } else if (lastKnownQueueSizeIsPositive) {
            lastKnownQueueSizeIsPositive = false;
            queuePointsCount.set(0);
            if (queuedPointsCountGauge == null) {
              // since we don't persist the number of points in the queue between proxy restarts yet, and Tape library
              // only lets us know the number of tasks in the queue, start reporting ~agent.buffer.points-count
              // metric only after it's confirmed that the retry queue is empty, as going through the entire queue
              // to calculate the number of points can be a very costly operation.
              queuedPointsCountGauge = Metrics.newGauge(new MetricName("buffer", "", "points-count"),
                  new Gauge<Long>() {
                    @Override
                    public Long value() {
                      return queuePointsCount.get();
                    }
                  }
              );
            }
            logger.info("retry queue has been cleared");
          }
          // do the same thing for sourceTagQueues
          List<Integer> sourceTagQueueSizes = Lists.newArrayList(Lists.transform
              (sourceTagTaskQueues,  new Function<ObjectQueue<ResubmissionTask>, Integer>() {
                @Override
                public Integer apply(ObjectQueue<ResubmissionTask> input) {
                  return input.size();
                }
              }));
          if (Iterables.tryFind(sourceTagQueueSizes, new Predicate<Integer>() {
            @Override
            public boolean apply(Integer input) {
              return input > 0;
            }
          }).isPresent()) {
            lastKnownSourceTagQueueSizeIsPositive = true;
            logger.warning("current source tag retry queue sizes: [" + Joiner.on("/").join
                (sourceTagQueueSizes) + "]");
          } else if (lastKnownSourceTagQueueSizeIsPositive) {
            lastKnownSourceTagQueueSizeIsPositive = false;
            logger.warning("source tag retry queue has been cleared");
          }
        } catch (Exception ex) {
          logger.warning("Exception " + ex);
        }
      }, 0, 5, TimeUnit.SECONDS);
    }

    Metrics.newGauge(ExpectedAgentMetric.BUFFER_BYTES_PER_MINUTE.metricName, new Gauge<Long>() {
      @Override
      public Long value() {
        return getBytesPerMinute();
      }
    });

    Metrics.newGauge(ExpectedAgentMetric.CURRENT_QUEUE_SIZE.metricName, new Gauge<Long>() {
      @Override
      public Long value() {
        return getQueuedTasksCount();
      }
    });
  }

  private Runnable createRunnable(final ScheduledExecutorService executorService, final boolean
      splitPushWhenRateLimited, final int threadId, final
      ResubmissionTaskQueue taskQueue, final RecyclableRateLimiter pushRateLimiter) {
    return new Runnable() {
      private int backoffExponent = 1;

      @Override
      public void run() {
        int successes = 0;
        int failures = 0;
        boolean rateLimiting = false;
        try {
          logger.fine("[RETRY THREAD " + threadId + "] TASK STARTING");
          while (taskQueue.size() > 0 && taskQueue.size() > failures) {
            if (Thread.currentThread().isInterrupted()) return;
            taskQueue.getLockObject().lock();
            try {
              ResubmissionTask task = taskQueue.peek();
              int taskSize = task == null ? 0 : task.size();
              if (pushRateLimiter != null && pushRateLimiter.getAvailablePermits() < pushRateLimiter.getRate()) {
                // if there's less than 1 second worth of accumulated credits, don't process the backlog queue
                rateLimiting = true;
                permitsDenied.inc(taskSize);
                break;
              }

              if (pushRateLimiter != null && taskSize > 0) {
                pushRateLimiter.acquire(taskSize);
                permitsGranted.inc(taskSize);
              }

              boolean removeTask = true;
              try {
                if (task != null) {
                  task.execute(null);
                  successes++;
                }
              } catch (Exception ex) {
                if (pushRateLimiter != null) {
                  pushRateLimiter.recyclePermits(taskSize);
                  permitsRetried.inc(taskSize);
                }
                failures++;
                //noinspection ThrowableResultOfMethodCallIgnored
                if (Throwables.getRootCause(ex) instanceof QueuedPushTooLargeException) {
                  // this should split this task, remove it from the queue, and not try more tasks
                  logger.warning("[RETRY THREAD " + threadId + "] Wavefront server rejected push with " +
                      "HTTP 413: request too large - splitting data into smaller chunks to retry. ");
                  List<? extends ResubmissionTask> splitTasks = task.splitTask();
                  for (ResubmissionTask smallerTask : splitTasks) {
                    taskQueue.add(smallerTask);
                    queueSizes.get(taskQueue).incrementAndGet();
                    queuePointsCount.addAndGet(smallerTask.size());
                  }
                  break;
                } else //noinspection ThrowableResultOfMethodCallIgnored
                  if (Throwables.getRootCause(ex) instanceof RejectedExecutionException) {
                    // this should either split and remove the original task or keep it at front
                    // it also should not try any more tasks
                    logger.warning("[RETRY THREAD " + threadId + "] Wavefront server rejected the submission " +
                        "(global rate limit exceeded) - will attempt later.");
                    if (splitPushWhenRateLimited) {
                      List<? extends ResubmissionTask> splitTasks = task.splitTask();
                      for (ResubmissionTask smallerTask : splitTasks) {
                        taskQueue.add(smallerTask);
                        queueSizes.get(taskQueue).incrementAndGet();
                        queuePointsCount.addAndGet(smallerTask.size());
                      }
                    } else {
                      removeTask = false;
                    }
                    break;
                  } else {
                    logger.log(Level.WARNING, "[RETRY THREAD " + threadId + "] cannot submit data to Wavefront servers. Will " +
                        "re-attempt later", Throwables.getRootCause(ex));
                  }
                // this can potentially cause a duplicate task to be injected (but since submission is mostly
                // idempotent it's not really a big deal)
                task.service = null;
                task.currentAgentId = null;
                taskQueue.add(task);
                queueSizes.get(taskQueue).incrementAndGet();
                queuePointsCount.addAndGet(taskSize);
                if (failures > 10) {
                  logger.warning("[RETRY THREAD " + threadId + "] saw too many submission errors. Will " +
                      "re-attempt later");
                  break;
                }
              } finally {
                if (removeTask) {
                  taskQueue.remove();
                  queueSizes.get(taskQueue).decrementAndGet();
                  queuePointsCount.addAndGet(-taskSize);
                }
              }
            } finally {
              taskQueue.getLockObject().unlock();
            }
          }
        } catch (Throwable ex) {
          logger.log(Level.WARNING, "[RETRY THREAD " + threadId + "] unexpected exception", ex);
        } finally {
          if (rateLimiting) {
            logger.fine("[RETRY THREAD " + threadId + "] Successful Batches: " + successes +
                ", Failed Batches: " + failures);
            logger.fine("[RETRY THREAD " + threadId + "] Rate limit reached, will re-attempt later");
            // if proxy rate limit exceeded, try again in 250..500ms (to introduce some degree of fairness)
            executorService.schedule(this, 250 + (int) (Math.random() * 250), TimeUnit.MILLISECONDS);
          } else {
            if (successes == 0 && failures != 0) {
              backoffExponent = Math.min(4, backoffExponent + 1); // caps at 2*base^4
            } else {
              backoffExponent = 1;
            }
            long next = (long) ((Math.random() + 1.0) *
                Math.pow(retryBackoffBaseSeconds.get(), backoffExponent));
            logger.fine("[RETRY THREAD " + threadId + "] Successful Batches: " + successes +
                ", Failed Batches: " + failures);
            logger.fine("[RETRY THREAD " + threadId + "] RESCHEDULING in " + next);
            executorService.schedule(this, next, TimeUnit.SECONDS);
          }
        }
      }
    };
  }

  private ObjectQueue<ResubmissionTask> createTaskQueue(final UUID agentId, File buffer) throws
      IOException {
    return new FileObjectQueue<>(buffer,
        new FileObjectQueue.Converter<ResubmissionTask>() {
          @Override
          public ResubmissionTask from(byte[] bytes) throws IOException {
            try {
              ObjectInputStream ois = new ObjectInputStream(new LZ4BlockInputStream(new ByteArrayInputStream(bytes)));
              return (ResubmissionTask) ois.readObject();
            } catch (Throwable t) {
              logger.warning("Failed to read a single retry submission from buffer, ignoring: " + t);
              return null;
            }
          }

          @Override
          public void toStream(ResubmissionTask o, OutputStream bytes) throws IOException {
            LZ4BlockOutputStream lz4BlockOutputStream = new LZ4BlockOutputStream(bytes);
            ObjectOutputStream oos = new ObjectOutputStream(lz4BlockOutputStream);
            oos.writeObject(o);
            oos.close();
            lz4BlockOutputStream.close();
          }
        });
  }

  public void shutdown() {
    executorService.shutdown();
  }

  public static void setRetryBackoffBaseSeconds(AtomicDouble newSecs) {
    retryBackoffBaseSeconds = newSecs;
  }

  public static void setSplitBatchSize(AtomicInteger newSize) {
    splitBatchSize = newSize;
  }

  public long getQueuedTasksCount() {
    long toReturn = 0;
    for (ResubmissionTaskQueue taskQueue : taskQueues) {
      toReturn += taskQueue.size();
    }
    return toReturn;
  }

  public long getQueuedSourceTagTasksCount() {
    long toReturn = 0;
    for (ObjectQueue<ResubmissionTask> taskQueue : sourceTagTaskQueues) {
      toReturn += taskQueue.size();
    }
    return toReturn;
  }

  private ResubmissionTaskQueue getSmallestQueue() {
    Optional<ResubmissionTaskQueue> smallestQueue = taskQueues.stream()
        .min(Comparator.comparingInt(q -> queueSizes.get(q).intValue()));
    return smallestQueue.orElse(null);
  }

  private ObjectQueue<ResubmissionTask> getSmallestSourceTagQueue() {
    int size = Integer.MAX_VALUE;
    ObjectQueue<ResubmissionTask> toReturn = null;
    for (ObjectQueue<ResubmissionTask> queue : sourceTagTaskQueues) {
      if (queue.size() == 0) return queue;
      else if (queue.size() < size){
        toReturn = queue;
        size = queue.size();
      }
    }
    return toReturn;
  }

  private Runnable getPostingSizerTask(final ResubmissionTask task) {
    return () -> {
      try {
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        LZ4BlockOutputStream lz4OutputStream = new LZ4BlockOutputStream(outputStream);
        ObjectOutputStream oos = new ObjectOutputStream(lz4OutputStream);
        oos.writeObject(task);
        oos.close();
        lz4OutputStream.close();
        resultPostingSizes.update(outputStream.size());
      } catch (Throwable t) {
        // ignored. this is a stats task.
      }
    };
  }

  private void scheduleTaskForSizing(ResubmissionTask task) {
    try {
      if (resultSizingRateLimier.tryAcquire()) {
        resultPostingSizerExecutorService.submit(getPostingSizerTask(task));
      }
    } catch (RejectedExecutionException ex) {
      // ignored.
    } catch (RuntimeException ex) {
      logger.warning("cannot size a submission task for stats tracking: " + ex);
    }
  }

  @Override
  public AgentConfiguration getConfig(UUID agentId, String hostname, Long currentMillis,
                                      Long bytesLeftForbuffer, Long bytesPerMinuteForBuffer, Long currentQueueSize,
                                      String token, String version) {
    return wrapped.getConfig(agentId, hostname, currentMillis, bytesLeftForbuffer, bytesPerMinuteForBuffer,
        currentQueueSize, token, version);
  }

  @Override
  public AgentConfiguration checkin(UUID agentId, String hostname, String token, String version, Long currentMillis,
                                    Boolean localAgent, JsonNode agentMetrics, Boolean pushAgent, Boolean ephemeral) {
    return wrapped.checkin(agentId, hostname, token, version, currentMillis, localAgent, agentMetrics, pushAgent, ephemeral);
  }

  @Override
  public Response postWorkUnitResult(final UUID agentId, final UUID workUnitId, final UUID targetId,
                                     final ShellOutputDTO shellOutputDTO) {
    return this.postWorkUnitResult(agentId, workUnitId, targetId, shellOutputDTO, false);
  }

  @Override
  public Response postWorkUnitResult(UUID agentId, UUID workUnitId, UUID targetId, ShellOutputDTO shellOutputDTO,
                                     boolean forceToQueue) {
    PostWorkUnitResultTask task = new PostWorkUnitResultTask(agentId, workUnitId, targetId, shellOutputDTO);

    if (forceToQueue) {
      addTaskToSmallestQueue(task);
      return Response.status(Response.Status.NOT_ACCEPTABLE).build();
    } else {

      try {
        resultPostingMeter.mark();
        parsePostingResponse(wrapped.postWorkUnitResult(agentId, workUnitId, targetId, shellOutputDTO));
        scheduleTaskForSizing(task);
      } catch (RuntimeException ex) {
        handleTaskRetry(ex, task);
        return Response.status(Response.Status.NOT_ACCEPTABLE).build();
      }
      return Response.ok().build();
    }
  }

  @Override
  public Response postPushData(final UUID agentId, final UUID workUnitId, final Long currentMillis,
                               final String format, final String pushData) {
    return this.postPushData(agentId, workUnitId, currentMillis, format, pushData, false);
  }

  @Override
  public Response postPushData(UUID agentId, UUID workUnitId, Long currentMillis, String format, String pushData,
                               boolean forceToQueue) {
    PostPushDataResultTask task = new PostPushDataResultTask(agentId, workUnitId, currentMillis, format, pushData);

    if (forceToQueue) {
      // bypass the charade of posting to the wrapped agentAPI. Just go straight to the retry queue
      addTaskToSmallestQueue(task);
      return Response.status(Response.Status.NOT_ACCEPTABLE).build();
    } else {
      try {
        resultPostingMeter.mark();
        parsePostingResponse(wrapped.postPushData(agentId, workUnitId, currentMillis, format, pushData));

        scheduleTaskForSizing(task);
      } catch (RuntimeException ex) {
        List<PostPushDataResultTask> splitTasks = handleTaskRetry(ex, task);
        for (PostPushDataResultTask splitTask : splitTasks) {
          // we need to ensure that we use the latest agent id.
          postPushData(agentId, splitTask.getWorkUnitId(), splitTask.getCurrentMillis(),
              splitTask.getFormat(), splitTask.getPushData());
        }
        return Response.status(Response.Status.NOT_ACCEPTABLE).build();
      }
      return Response.ok().build();
    }
  }

  /**
   * @return list of tasks to immediately retry
   */
  private <T extends ResubmissionTask<T>> List<T> handleTaskRetry(RuntimeException failureException, T taskToRetry) {
    if (failureException instanceof QueuedPushTooLargeException) {
      List<T> resubmissionTasks = taskToRetry.splitTask();
      // there are split tasks, so go ahead and return them
      // otherwise, nothing got split, so this should just get queued up
      if (resubmissionTasks.size() > 1) {
        return resubmissionTasks;
      }
    }
    logger.warning("Cannot post push data result to Wavefront servers. " +
        "Will enqueue and retry later: " + Throwables.getRootCause(failureException));
    addTaskToSmallestQueue(taskToRetry);
    return Collections.emptyList();
  }

  private void handleSourceTagTaskRetry(RuntimeException failureException,
                                        PostSourceTagResultTask taskToRetry) {
    logger.warning("Cannot post push data result to Wavefront servers. Will enqueue and retry " +
        "later: " + failureException);
    addSourceTagTaskToSmallestQueue(taskToRetry);
  }

  private void addSourceTagTaskToSmallestQueue(ResubmissionTask taskToRetry) {
    ObjectQueue<ResubmissionTask> queue = getSmallestSourceTagQueue();
    if (queue != null) {
      synchronized (queue) {
        try {
          queue.add(taskToRetry);
        } catch (FileException ex) {
          logger.log(Level.WARNING, "CRITICAL (Losing sourceTags!): WF-1: Submission queue is " +
              "full.", ex);
        }
      }
    } else {
      logger.warning("CRITICAL (Losing sourceTags!): WF-2: No retry queues found.");
    }
  }

  private void addTaskToSmallestQueue(ResubmissionTask taskToRetry) {
    ResubmissionTaskQueue queue = getSmallestQueue();
    if (queue != null) {
      queue.getLockObject().lock();
      try {
        queue.add(taskToRetry);
        queueSizes.get(queue).incrementAndGet();
        queuePointsCount.addAndGet(taskToRetry.size());
      } catch (FileException e) {
        logger.log(Level.SEVERE, "CRITICAL (Losing points!): WF-1: Submission queue is full.", e);
      } finally {
        queue.getLockObject().unlock();
      }
    } else {
      logger.severe("CRITICAL (Losing points!): WF-2: No retry queues found.");
    }
  }

  private static void parsePostingResponse(Response response) {
    if (response == null) throw new RuntimeException("No response from server");
    try {
      if (response.getStatus() != Response.Status.OK.getStatusCode()) {
        if (response.getStatus() == Response.Status.NOT_ACCEPTABLE.getStatusCode()) {
          throw new RejectedExecutionException("Response not accepted by server: " + response.getStatus());
        } else if (response.getStatus() == Response.Status.REQUEST_ENTITY_TOO_LARGE.getStatusCode()) {
          throw new QueuedPushTooLargeException("Request too large: " + response.getStatus());
        } else if (response.getStatus() == 407 || response.getStatus() == 408) {
          boolean isWavefrontResponse = false;
          // check if the HTTP 407/408 response was actually received from Wavefront - if it's a JSON object
          // containing "code" key, with value equal to the HTTP response code, it's most likely from us.
          try {
            Map<String, Object> resp = new HashMap<>();
            resp = (Map<String, Object>) new Gson().fromJson(response.readEntity(String.class), resp.getClass());
            if (resp.containsKey("code") && resp.get("code") instanceof Number &&
                ((Number) resp.get("code")).intValue() == response.getStatus()) {
              isWavefrontResponse = true;
            }
          } catch (Exception ex) {
            // ignore
          }
          if (isWavefrontResponse) {
            throw new RuntimeException("Response not accepted by server: " + response.getStatus() +
                " unclaimed proxy - please verify that your token is valid and has Agent Management permission!");
          } else {
            throw new RuntimeException("HTTP " + response.getStatus() + ": Please verify your " +
                "network/HTTP proxy settings!");
          }
        } else {
          throw new RuntimeException(SERVER_ERROR + ": " + response.getStatus());
        }
      }
    } finally {
      response.close();
    }
  }

  @Override
  public void agentError(UUID agentId, String details) {
    wrapped.agentError(agentId, details);
  }

  @Override
  public void agentConfigProcessed(UUID agentId) {
    wrapped.agentConfigProcessed(agentId);
  }

  @Override
  public void hostConnectionFailed(UUID agentId, UUID hostId, String details) {
    wrapped.hostConnectionFailed(agentId, hostId, details);
  }

  @Override
  public void hostConnectionEstablished(UUID agentId, UUID hostId) {
    wrapped.hostConnectionEstablished(agentId, hostId);
  }

  @Override
  public void hostAuthenticated(UUID agentId, UUID hostId) {
    wrapped.hostAuthenticated(agentId, hostId);
  }

  @Override
  public Response removeTag(String id, String token, String tagValue) {
    return removeTag(id, token, tagValue, false);
  }

  @Override
  public Response removeDescription(String id, String token) {
    return removeDescription(id, token, false);
  }

  @Override
  public Response setTags(String id, String token, List<String> tagValuesToSet) {
     return setTags(id, token, tagValuesToSet, false);
  }

  @Override
  public Response setDescription(String id, String token, String description) {
    return setDescription(id, token, description, false);
  }

  @Override
  public Response setTags(String id, String token, List<String> tagValuesToSet, boolean forceToQueue) {
    PostSourceTagResultTask task = new PostSourceTagResultTask(id, tagValuesToSet,
        PostSourceTagResultTask.ActionType.save, PostSourceTagResultTask.MessageType.tag, token);

    if (forceToQueue) {
      // bypass the charade of posting to the wrapped agentAPI. Just go straight to the retry queue
      addSourceTagTaskToSmallestQueue(task);
      return Response.status(Response.Status.NOT_ACCEPTABLE).build();
    } else {
      // invoke server side API
      try {
        Response response = wrapped.setTags(id, token, tagValuesToSet);
        logger.info("Received response status = " + response.getStatus());
        parsePostingResponse(response);
      } catch (RuntimeException ex) {
        // If it is a server error then no need of retrying
        if (!ex.getMessage().startsWith(SERVER_ERROR))
          handleSourceTagTaskRetry(ex, task);
        logger.warning("Unable to process the source tag request" + ExceptionUtils
            .getFullStackTrace(ex));
        return Response.status(Response.Status.NOT_ACCEPTABLE).build();
      }
      return Response.ok().build();
    }
  }

  @Override
  public Response removeDescription(String id, String token, boolean forceToQueue) {
    PostSourceTagResultTask task = new PostSourceTagResultTask(id, StringUtils.EMPTY,
        PostSourceTagResultTask.ActionType.delete, PostSourceTagResultTask.MessageType.desc, token);

    if (forceToQueue) {
      // bypass the charade of posting to the wrapped agentAPI. Just go straight to the retry queue
      addSourceTagTaskToSmallestQueue(task);
      return Response.status(Response.Status.NOT_ACCEPTABLE).build();
    } else {
      // invoke server side API
      try {
        parsePostingResponse(wrapped.removeDescription(id, token));
      } catch (RuntimeException ex) {
        // If it is a server error then no need of retrying
        if (!ex.getMessage().startsWith(SERVER_ERROR))
        handleSourceTagTaskRetry(ex, task);
        logger.warning("Unable to process the source tag request" + ExceptionUtils
            .getFullStackTrace(ex));
        return Response.status(Response.Status.NOT_ACCEPTABLE).build();
      }
      return Response.ok().build();
    }
  }

  @Override
  public Response setDescription(String id, String token, String desc, boolean forceToQueue) {
    PostSourceTagResultTask task = new PostSourceTagResultTask(id, desc,
        PostSourceTagResultTask.ActionType.save, PostSourceTagResultTask.MessageType.desc, token);

    if (forceToQueue) {
      // bypass the charade of posting to the wrapped agentAPI. Just go straight to the retry queue
      addSourceTagTaskToSmallestQueue(task);
      return Response.status(Response.Status.NOT_ACCEPTABLE).build();
    } else {
      // invoke server side API
      try {
        parsePostingResponse(wrapped.setDescription(id, token, desc));
      } catch (RuntimeException ex) {
        // If it is a server error then no need of retrying
        if (!ex.getMessage().startsWith(SERVER_ERROR))
          handleSourceTagTaskRetry(ex, task);
        logger.warning("Unable to process the source tag request" + ExceptionUtils
            .getFullStackTrace(ex));
        return Response.status(Response.Status.NOT_ACCEPTABLE).build();
      }
      return Response.ok().build();
    }
  }

  @Override
  public Response removeTag(String id, String token, String tagValue, boolean forceToQueue) {

    PostSourceTagResultTask task = new PostSourceTagResultTask(id, tagValue,
        PostSourceTagResultTask.ActionType.delete, PostSourceTagResultTask.MessageType.tag, token);

    if (forceToQueue) {
      // bypass the charade of posting to the wrapped agentAPI. Just go straight to the retry queue
      addSourceTagTaskToSmallestQueue(task);
      return Response.status(Response.Status.NOT_ACCEPTABLE).build();
    } else {
      // invoke server side API
      try {
        parsePostingResponse(wrapped.removeTag(id, token, tagValue));
      } catch (RuntimeException ex) {
        // If it is a server error then no need of retrying
        if (!ex.getMessage().startsWith(SERVER_ERROR))
          handleSourceTagTaskRetry(ex, task);
        logger.warning("Unable to process the source tag request" + ExceptionUtils
            .getFullStackTrace(ex));
        return Response.status(Response.Status.NOT_ACCEPTABLE).build();
      }
      return Response.ok().build();
    }
  }

  public static class PostWorkUnitResultTask extends ResubmissionTask {

    @VisibleForTesting
    final UUID agentId;
    @VisibleForTesting
    final UUID workUnitId;
    @VisibleForTesting
    final UUID hostId;
    @VisibleForTesting
    final ShellOutputDTO shellOutputDTO;

    public PostWorkUnitResultTask(UUID agentId, UUID workUnitId, UUID hostId, ShellOutputDTO shellOutputDTO) {
      this.agentId = agentId;
      this.workUnitId = workUnitId;
      this.hostId = hostId;
      this.shellOutputDTO = shellOutputDTO;
    }

    @Override
    public void execute(Object callback) {
      parsePostingResponse(service.postWorkUnitResult(currentAgentId, workUnitId, hostId, shellOutputDTO));
    }

    @Override
    public List<PostWorkUnitResultTask> splitTask() {
      // doesn't make sense to split this, so just return a new task
      return of(new PostWorkUnitResultTask(agentId, workUnitId, hostId, shellOutputDTO));
    }

    @Override
    public int size() {
      return 1;
    }
  }

  public static class PostSourceTagResultTask extends ResubmissionTask<PostSourceTagResultTask> {
    private final String id;
    private final String[] tagValues;
    private final String description;
    private final int taskSize;
    private final String token;

    public enum ActionType {save, delete}
    public enum MessageType {tag, desc}
    private final ActionType actionType;
    private final MessageType messageType;

    public PostSourceTagResultTask(String id, String tagValue, ActionType actionType, MessageType msgType,
                                   String token) {
      this.id = id;
      if (msgType == MessageType.desc) {
        description = tagValue;
        tagValues = ArrayUtils.EMPTY_STRING_ARRAY;
      }
      else {
        tagValues = new String[]{tagValue};
        description = StringUtils.EMPTY;
      }
      this.actionType = actionType;
      this.messageType = msgType;
      this.taskSize = 1;
      this.token = token;
    }

    public PostSourceTagResultTask(String id, List<String> tagValuesToSet, ActionType actionType, MessageType msgType,
                                   String token) {
      this.id = id;
      this.tagValues = tagValuesToSet.toArray(new String[tagValuesToSet.size()]);
      description = StringUtils.EMPTY;
      this.actionType = actionType;
      this.messageType = msgType;
      this.taskSize = 1;
      this.token = token;
    }

    @Override
    public List<PostSourceTagResultTask> splitTask() {
      // currently this is a no-op
      List<PostSourceTagResultTask> splitTasks = Lists.newArrayList();
      splitTasks.add(new PostSourceTagResultTask(id, tagValues[0], this.actionType,
          this.messageType, this.token));
      return splitTasks;
    }

    @Override
    public int size() {
      return taskSize;
    }

    @Override
    public void execute(Object callback) {
      Response response;
      try {
        switch (messageType) {
          case tag:
            if (actionType == ActionType.delete)
              response = service.removeTag(id, token, tagValues[0]);
            else
              response = service.setTags(id, token, Arrays.asList(tagValues));
            break;
          case desc:
            if (actionType == ActionType.delete)
              response = service.removeDescription(id, token);
            else
              response = service.setDescription(id, token, description);
            break;
          default:
            logger.warning("Invalid message type.");
            response = Response.serverError().build();
        }
      } catch (Exception ex) {
        throw new RuntimeException(SERVER_ERROR + ": " + Throwables.getRootCause(ex));
      }
      parsePostingResponse(response);
    }
  }

  public static class PostPushDataResultTask extends ResubmissionTask<PostPushDataResultTask> {

    private final UUID agentId;
    private final UUID workUnitId;
    private final Long currentMillis;
    private final String format;
    private final String pushData;
    private final int taskSize;

    private transient Histogram timeSpentInQueue;

    public PostPushDataResultTask(UUID agentId, UUID workUnitId, Long currentMillis, String format, String pushData) {
      this.agentId = agentId;
      this.workUnitId = workUnitId;
      this.currentMillis = currentMillis;
      this.format = format;
      this.pushData = pushData;
      this.taskSize = StringLineIngester.pushDataSize(pushData);
    }

    @Override
    public void execute(Object callback) {
      parsePostingResponse(service.postPushData(currentAgentId, workUnitId, currentMillis, format, pushData));
      if (timeSpentInQueue == null) {
        timeSpentInQueue = Metrics.newHistogram(new MetricName("buffer", "", "queue-time"));
      }
      // timestamps on PostPushDataResultTask are local system clock, not drift-corrected clock
      timeSpentInQueue.update(System.currentTimeMillis() - currentMillis);
    }

    @Override
    public List<PostPushDataResultTask> splitTask() {
      // pull the pushdata back apart to split and put back together
      List<PostPushDataResultTask> splitTasks = Lists.newArrayList();

      List<Integer> dataIndex = StringLineIngester.indexPushData(pushData);

      int numDatum = dataIndex.size() / 2;
      if (numDatum > 1) {
        // in this case, at least split the strings in 2 batches.  batch size must be less
        // than splitBatchSize
        int stride = Math.min(splitBatchSize.get(), (int) Math.ceil((float) numDatum / 2.0));
        int endingIndex = 0;
        for (int startingIndex = 0; endingIndex < numDatum - 1; startingIndex += stride) {
          endingIndex = Math.min(numDatum, startingIndex + stride) - 1;
          splitTasks.add(new PostPushDataResultTask(agentId, workUnitId, currentMillis, format,
              pushData.substring(dataIndex.get(startingIndex * 2), dataIndex.get(endingIndex * 2 + 1))));
        }
      } else {
        // 1 or 0
        splitTasks.add(new PostPushDataResultTask(agentId, workUnitId, currentMillis, format, pushData));
      }

      return splitTasks;
    }

    @Override
    public int size() {
      return taskSize;
    }

    @VisibleForTesting
    public UUID getAgentId() {
      return agentId;
    }

    @VisibleForTesting
    public UUID getWorkUnitId() {
      return workUnitId;
    }

    @VisibleForTesting
    public Long getCurrentMillis() {
      return currentMillis;
    }

    @VisibleForTesting
    public String getFormat() {
      return format;
    }

    @VisibleForTesting
    public String getPushData() {
      return pushData;
    }
  }
}
