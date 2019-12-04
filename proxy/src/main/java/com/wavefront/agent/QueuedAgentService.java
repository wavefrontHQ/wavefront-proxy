package com.wavefront.agent;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.AtomicDouble;
import com.google.common.util.concurrent.RateLimiter;
import com.google.common.util.concurrent.RecyclableRateLimiter;
import com.google.gson.Gson;

import com.fasterxml.jackson.databind.JsonNode;
import com.github.benmanes.caffeine.cache.CacheLoader;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;
import com.squareup.tape.FileException;
import com.squareup.tape.FileObjectQueue;
import com.squareup.tape.ObjectQueue;
import com.squareup.tape.TaskQueue;
import com.wavefront.agent.handlers.LineDelimitedUtils;
import com.wavefront.agent.api.ForceQueueEnabledProxyAPI;
import com.wavefront.agent.api.WavefrontV2API;
import com.wavefront.api.agent.AgentConfiguration;
import com.wavefront.common.Clock;
import com.wavefront.common.NamedThreadFactory;
import com.wavefront.dto.Event;
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
import java.util.concurrent.atomic.AtomicBoolean;
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

import static com.wavefront.agent.AbstractAgent.NO_RATE_LIMIT;

/**
 * A wrapper for any WavefrontAPI that queues up any result posting if the backend is not available.
 * Current data will always be submitted right away (thus prioritizing live data) while background
 * threads will submit backlogged data.
 *
 * @author Clement Pang (clement@wavefront.com)
 */
public class QueuedAgentService implements ForceQueueEnabledProxyAPI {

  private static final Logger logger = Logger.getLogger(QueuedAgentService.class.getCanonicalName());
  private static final String SERVER_ERROR = "Server error";

  private WavefrontV2API wrapped;
  private final List<ResubmissionTaskQueue> taskQueues;
  private final List<ResubmissionTaskQueue> sourceTagTaskQueues;
  private final List<ResubmissionTaskQueue> eventTaskQueues;
  private final List<Runnable> taskRunnables;
  private final List<Runnable> sourceTagTaskRunnables;
  private final List<Runnable> eventTaskRunnables;
  private static AtomicInteger minSplitBatchSize = new AtomicInteger(100);
  private static AtomicDouble retryBackoffBaseSeconds = new AtomicDouble(2.0);
  private boolean lastKnownQueueSizeIsPositive = true;
  private boolean lastKnownSourceTagQueueSizeIsPositive = false;
  private boolean lastKnownEventQueueSizeIsPositive = false;
  private AtomicBoolean isRunning = new AtomicBoolean(false);
  private final ScheduledExecutorService executorService;
  private final String token;
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

  public QueuedAgentService(WavefrontV2API service, String bufferFile, final int retryThreads,
                            final ScheduledExecutorService executorService, boolean purge,
                            final UUID agentId, final boolean splitPushWhenRateLimited,
                            @Nullable final RecyclableRateLimiter pushRateLimiter,
                            @Nullable final String token) throws IOException {
    if (retryThreads <= 0) {
      logger.severe("You have no retry threads set up. Any points that get rejected will be lost.\n Change this by " +
          "setting retryThreads to a value > 0");
    }
    if (pushRateLimiter != null && pushRateLimiter.getRate() < NO_RATE_LIMIT) {
      logger.info("Point rate limited at the proxy at : " + String.valueOf(pushRateLimiter.getRate()));
    } else {
      logger.info("No rate limit configured.");
    }
    this.wrapped = service;
    this.taskQueues = QueuedAgentService.createResubmissionTasks(service, retryThreads, bufferFile, purge, agentId,
        token);
    this.sourceTagTaskQueues = QueuedAgentService.createResubmissionTasks(service, retryThreads,
        bufferFile + "SourceTag", purge, agentId, token);
    this.eventTaskQueues = QueuedAgentService.createResubmissionTasks(service, retryThreads,
        bufferFile + ".events", purge, agentId, token);
    this.taskRunnables = Lists.newArrayListWithExpectedSize(taskQueues.size());
    this.sourceTagTaskRunnables = Lists.newArrayListWithExpectedSize(sourceTagTaskQueues.size());
    this.eventTaskRunnables = Lists.newArrayListWithExpectedSize(eventTaskQueues.size());
    this.executorService = executorService;
    this.token = token;

    int threadId = 0;
    for (ResubmissionTaskQueue taskQueue : taskQueues) {
      taskRunnables.add(createRunnable(executorService, splitPushWhenRateLimited, threadId++,
          taskQueue, pushRateLimiter));
    }
    threadId = 0;
    for (ResubmissionTaskQueue taskQueue : sourceTagTaskQueues) {
      sourceTagTaskRunnables.add(createRunnable(executorService, splitPushWhenRateLimited,
          threadId++, taskQueue, pushRateLimiter));
    }
    threadId = 0;
    for (ResubmissionTaskQueue taskQueue : eventTaskQueues) {
      eventTaskRunnables.add(createRunnable(executorService, splitPushWhenRateLimited,
          threadId++, taskQueue, pushRateLimiter));
    }

    if (taskQueues.size() > 0) {
      executorService.scheduleAtFixedRate(() -> {
        try {
          Supplier<Stream<Integer>> sizes = () -> taskQueues.stream().map(TaskQueue::size);
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
          Supplier<Stream<Integer>> sourceTagQueueSizes = () -> sourceTagTaskQueues.stream().map(TaskQueue::size);
          if (sourceTagQueueSizes.get().anyMatch(i -> i > 0)) {
            lastKnownSourceTagQueueSizeIsPositive = true;
            logger.warning("current source tag retry queue sizes: [" +
                sourceTagQueueSizes.get().map(Object::toString).collect(Collectors.joining("/")) + "]");
          } else if (lastKnownSourceTagQueueSizeIsPositive) {
            lastKnownSourceTagQueueSizeIsPositive = false;
            logger.warning("source tag retry queue has been cleared");
          }

          // do the same thing for event queues
          Supplier<Stream<Integer>> eventQueueSizes = () -> eventTaskQueues.stream().
              map(TaskQueue::size);
          if (eventQueueSizes.get().anyMatch(i -> i > 0)) {
            lastKnownEventQueueSizeIsPositive = true;
            logger.warning("current event retry queue sizes: [" +
                eventQueueSizes.get().map(Object::toString).collect(Collectors.joining("/")) + "]");
          } else if (lastKnownEventQueueSizeIsPositive) {
            lastKnownEventQueueSizeIsPositive = false;
            logger.warning("event retry queue has been cleared");
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

  public void start() {
    if (!isRunning.getAndSet(true)) {
      taskRunnables.forEach(taskRunnable -> executorService.schedule(taskRunnable,
          (long) (Math.random() * taskRunnables.size()), TimeUnit.SECONDS));
      sourceTagTaskRunnables.forEach(taskRunnable -> executorService.schedule(taskRunnable,
          (long) (Math.random() * taskRunnables.size()), TimeUnit.SECONDS));
      eventTaskRunnables.forEach(taskRunnable -> executorService.schedule(taskRunnable,
          (long) (Math.random() * taskRunnables.size()), TimeUnit.SECONDS));
    }
  }

  private Runnable createRunnable(final ScheduledExecutorService executorService,
                                  final boolean splitPushWhenRateLimited,
                                  final int threadId,
                                  final ResubmissionTaskQueue taskQueue,
                                  final RecyclableRateLimiter pushRateLimiter) {
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
            ResubmissionTask task = taskQueue.peek();
            int taskSize = task == null ? 0 : task.size();
            if (pushRateLimiter != null && !pushRateLimiter.immediatelyAvailable(
                Math.max((int) pushRateLimiter.getRate(), taskSize))) {
              // if there's less than 1 second or 1 task size worth of accumulated credits
              // (whichever is greater), don't process the backlog queue
              rateLimiting = true;
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
              queuePointsCount.addAndGet(taskSize);
              if (failures > 10) {
                logger.warning("[RETRY THREAD " + threadId + "] saw too many submission errors. Will " +
                    "re-attempt later");
                break;
              }
            } finally {
              if (removeTask) {
                taskQueue.remove();
                queuePointsCount.addAndGet(-taskSize);
              }
            }
          }
        } catch (Throwable ex) {
          logger.log(Level.WARNING, "[RETRY THREAD " + threadId + "] unexpected exception", ex);
        } finally {
          logger.fine("[RETRY THREAD " + threadId + "] Successful Batches: " + successes +
              ", Failed Batches: " + failures);
          if (rateLimiting) {
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
            logger.fine("[RETRY THREAD " + threadId + "] RESCHEDULING in " + next);
            executorService.schedule(this, next, TimeUnit.SECONDS);
          }
        }
      }
    };
  }

  public static ObjectQueue<ResubmissionTask> createTaskQueue(final UUID agentId, File buffer) throws
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

  public static List<ResubmissionTaskQueue> createResubmissionTasks(WavefrontV2API wrapped, int retryThreads,
                                                                    String bufferFile, boolean purge, UUID agentId,
                                                                    String token) throws IOException {
    // Having two proxy processes write to the same buffer file simultaneously causes buffer file corruption.
    // To prevent concurrent access from another process, we try to obtain exclusive access to a .lck file
    // trylock() is platform-specific so there is no iron-clad guarantee, but it works well in most cases
    try {
      File lockFile = new File(bufferFile + ".lck");
      if (lockFile.exists()) {
        Preconditions.checkArgument(true, lockFile.delete());
      }
      FileChannel channel = new RandomAccessFile(lockFile, "rw").getChannel();
      Preconditions.checkNotNull(channel.tryLock()); // fail if tryLock() returns null (lock couldn't be acquired)
    } catch (Exception e) {
      logger.severe("WF-005: Error requesting exclusive access to the buffer lock file " + bufferFile + ".lck" +
          " - please make sure that no other processes access this file and restart the proxy");
      System.exit(-1);
    }

    List<ResubmissionTaskQueue> output = Lists.newArrayListWithExpectedSize(retryThreads);
    for (int i = 0; i < retryThreads; i++) {
      File buffer = new File(bufferFile + "." + i);
      if (purge) {
        if (buffer.delete()) {
          logger.warning("Retry buffer has been purged: " + buffer.getAbsolutePath());
        }
      }

      ObjectQueue<ResubmissionTask> queue = createTaskQueue(agentId, buffer);
      final ResubmissionTaskQueue taskQueue = new ResubmissionTaskQueue(queue,
          task -> {
            task.service = wrapped;
            task.currentAgentId = agentId;
            task.token = token;
          }
      );
      output.add(taskQueue);
    }
    return output;
  }

  public void setWrappedApi(WavefrontV2API api) {
    this.wrapped = api;
  }

  public boolean isRunning() {
    return this.isRunning.get();
  }

  public void shutdown() {
    executorService.shutdown();
  }

  public static void setRetryBackoffBaseSeconds(AtomicDouble newSecs) {
    retryBackoffBaseSeconds = newSecs;
  }

  @Deprecated
  public static void setSplitBatchSize(AtomicInteger newSize) {
  }

  @VisibleForTesting
  static void setMinSplitBatchSize(int newSize) {
    minSplitBatchSize.set(newSize);
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
  public AgentConfiguration proxyCheckin(UUID agentId, String token, String hostname, String version,
                                         Long currentMillis, JsonNode agentMetrics, Boolean ephemeral) {
    return wrapped.proxyCheckin(agentId, token, hostname, version, currentMillis, agentMetrics, ephemeral);
  }

  @Override
  public Response proxyReport(final UUID agentId, final String format, final String pushData) {
    return this.proxyReport(agentId, format, pushData, false);
  }

  @Override
  public Response proxyReport(UUID agentId, String format, String pushData, boolean forceToQueue) {
    PostPushDataResultTask task = new PostPushDataResultTask(agentId, Clock.now(), format, pushData);

    if (forceToQueue) {
      // bypass the charade of posting to the wrapped agentAPI. Just go straight to the retry queue
      addTaskToSmallestQueue(task);
      return Response.status(Response.Status.NOT_ACCEPTABLE).build();
    } else {
      try {
        resultPostingMeter.mark();
        parsePostingResponse(wrapped.proxyReport(agentId, format, pushData));

        scheduleTaskForSizing(task);
      } catch (RuntimeException ex) {
        List<PostPushDataResultTask> splitTasks = handleTaskRetry(ex, task);
        for (PostPushDataResultTask splitTask : splitTasks) {
          // we need to ensure that we use the latest agent id.
          proxyReport(agentId, splitTask.getFormat(), splitTask.getPushData());
        }
        return Response.status(Response.Status.NOT_ACCEPTABLE).build();
      }
      return Response.ok().build();
    }
  }

  @Override
  public void proxyConfigProcessed(final UUID proxyId) {
    wrapped.proxyConfigProcessed(proxyId);
  }

  @Override
  public void proxyError(final UUID proxyId, String details) {
    wrapped.proxyError(proxyId, details);
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

  private void handleEventTaskRetry(RuntimeException failureException,
                                    PostEventResultTask taskToRetry) {
    if (failureException instanceof QueuedPushTooLargeException) {
      taskToRetry.splitTask().forEach(this::addEventTaskToSmallestQueue);
    } else {
      addTaskToSmallestQueue(taskToRetry);
    }
  }

  private void addSourceTagTaskToSmallestQueue(PostSourceTagResultTask taskToRetry) {
    // we need to make sure the we preserve the order of operations for each source
    ResubmissionTaskQueue queue = sourceTagTaskQueues.get(Math.abs(taskToRetry.id.hashCode()) % sourceTagTaskQueues.size());
    if (queue != null) {
      try {
        queue.add(taskToRetry);
      } catch (FileException ex) {
        logger.log(Level.SEVERE, "CRITICAL (Losing sourceTags!): WF-1: Submission queue is " +
            "full.", ex);
      }
    } else {
      logger.severe("CRITICAL (Losing sourceTags!): WF-2: No retry queues found.");
    }
  }

  private void addTaskToSmallestQueue(ResubmissionTask taskToRetry) {
    ResubmissionTaskQueue queue = taskQueues.stream().min(Comparator.comparingInt(TaskQueue::size)).
        orElse(null);
    if (queue != null) {
      try {
        queue.add(taskToRetry);
        queuePointsCount.addAndGet(taskToRetry.size());
      } catch (FileException e) {
        logger.log(Level.SEVERE, "CRITICAL (Losing points!): WF-1: Submission queue is full.", e);
      }
    } else {
      logger.severe("CRITICAL (Losing points!): WF-2: No retry queues found.");
    }
  }

  private void addEventTaskToSmallestQueue(ResubmissionTask taskToRetry) {
    ResubmissionTaskQueue queue = eventTaskQueues.stream().
        min(Comparator.comparingInt(TaskQueue::size)).orElse(null);
    if (queue != null) {
      try {
        queue.add(taskToRetry);
      } catch (FileException e) {
        logger.log(Level.SEVERE, "CRITICAL (Losing events!): WF-1: Submission queue is full.", e);
      }
    } else {
      logger.severe("CRITICAL (Losing events!): WF-2: No retry queues found.");
    }
  }

  private static void parsePostingResponse(Response response) {
    if (response == null) throw new RuntimeException("No response from server");
    try {
      if (response.getStatus() < 200 || response.getStatus() >= 300) {
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
  public Response appendTag(String id, String token, String tagValue) {
    return appendTag(id, tagValue, false);
  }

  @Override
  public Response removeTag(String id, String token, String tagValue) {
    return removeTag(id, tagValue, false);
  }

  @Override
  public Response removeDescription(String id, String token) {
    return removeDescription(id, false);
  }

  @Override
  public Response setTags(String id, String token, List<String> tagValuesToSet) {
     return setTags(id, tagValuesToSet, false);
  }

  @Override
  public Response setDescription(String id, String token, String description) {
    return setDescription(id, description, false);
  }

  @Override
  public Response setTags(String id, List<String> tagValuesToSet, boolean forceToQueue) {
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
  public Response removeDescription(String id, boolean forceToQueue) {
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
  public Response reportEvents(List<Event> eventBatch, boolean forceToQueue) {
    PostEventResultTask task = new PostEventResultTask(eventBatch);

    if (forceToQueue) {
      addEventTaskToSmallestQueue(task);
      return Response.status(Response.Status.NOT_ACCEPTABLE).build();
    } else {
      try {
        parsePostingResponse(wrapped.reportEvents(eventBatch));
      } catch (RuntimeException ex) {
        logger.warning("Unable to create events: " + ExceptionUtils.getFullStackTrace(ex));
        addEventTaskToSmallestQueue(task);
        return Response.status(Response.Status.NOT_ACCEPTABLE).build();
      }
      return Response.ok().build();
    }
  }

  @Override
  public Response setDescription(String id, String desc, boolean forceToQueue) {
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
  public Response appendTag(String id, String tagValue, boolean forceToQueue) {
    PostSourceTagResultTask task = new PostSourceTagResultTask(id, tagValue, PostSourceTagResultTask.ActionType.add,
        PostSourceTagResultTask.MessageType.tag, token);

    if (forceToQueue) {
      // bypass the charade of posting to the wrapped agentAPI. Just go straight to the retry queue
      addSourceTagTaskToSmallestQueue(task);
      return Response.status(Response.Status.NOT_ACCEPTABLE).build();
    } else {
      // invoke server side API
      try {
        parsePostingResponse(wrapped.appendTag(id, token, tagValue));
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
  public Response removeTag(String id, String tagValue, boolean forceToQueue) {
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

  @Override
  public Response reportEvents(List<Event> events) {
    return reportEvents(events, false);
  }

  public static class PostEventResultTask extends ResubmissionTask<PostEventResultTask> {
    private final List<Event> events;

    public PostEventResultTask(List<Event> events) {
      this.events = events;
    }

    @Override
    public List<PostEventResultTask> splitTask() {
      // currently this is a no-op
      return ImmutableList.of(this);
    }

    @Override
    public int size() {
      return 1;
    }

    @Override
    public void execute(Object callback) {
      Response response;
      try {
        response = service.reportEvents(events);
      } catch (Exception ex) {
        throw new RuntimeException(SERVER_ERROR + ": " + Throwables.getRootCause(ex));
      }
      parsePostingResponse(response);
    }
  }

  public static class PostSourceTagResultTask extends ResubmissionTask<PostSourceTagResultTask> {
    private final String id;
    private final String[] tagValues;
    private final String description;
    private final int taskSize;

    public enum ActionType {save, add, delete}
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
            switch (actionType) {
              case add:
                response = service.appendTag(id, token, tagValues[0]);
                break;
              case delete:
                response = service.removeTag(id, token, tagValues[0]);
                break;
              case save:
                response = service.setTags(id, token, Arrays.asList(tagValues));
                break;
              default:
                logger.warning("Invalid action type.");
                response = Response.serverError().build();
            }
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
    private static final long serialVersionUID = 1973695079812309903L; // to ensure backwards compatibility

    private final UUID agentId;
    private final Long currentMillis;
    private final String format;
    private final String pushData;
    private final int taskSize;

    private transient Histogram timeSpentInQueue;

    public PostPushDataResultTask(UUID agentId, Long currentMillis, String format, String pushData) {
      this.agentId = agentId;
      this.currentMillis = currentMillis;
      this.format = format;
      this.pushData = pushData;
      this.taskSize = LineDelimitedUtils.pushDataSize(pushData);
    }

    @Override
    public void execute(Object callback) {
      // timestamps on PostPushDataResultTask are local system clock, not drift-corrected clock
      if (timeSpentInQueue == null) {
        timeSpentInQueue = Metrics.newHistogram(new MetricName("buffer", "", "queue-time"));
      }
      timeSpentInQueue.update(System.currentTimeMillis() - currentMillis);
      parsePostingResponse(service.proxyReport(currentAgentId, format, pushData));
    }

    @Override
    public List<PostPushDataResultTask> splitTask() {
      // pull the pushdata back apart to split and put back together
      List<PostPushDataResultTask> splitTasks = Lists.newArrayListWithExpectedSize(2);

      if (taskSize > minSplitBatchSize.get()) {
        // in this case, split the payload in 2 batches approximately in the middle.
        int splitPoint = pushData.indexOf(LineDelimitedUtils.PUSH_DATA_DELIMETER,
            pushData.length() / 2);
        if (splitPoint > 0) {
          splitTasks.add(new PostPushDataResultTask(agentId, currentMillis, format,
              pushData.substring(0, splitPoint)));
          splitTasks.add(new PostPushDataResultTask(agentId, currentMillis, format,
              pushData.substring(splitPoint + 1)));
          return splitTasks;
        }
      }
      // 1 or 0
      splitTasks.add(new PostPushDataResultTask(agentId, currentMillis, format,
          pushData));
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
