package com.wavefront.agent;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.base.Predicate;
import com.google.common.base.Throwables;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.RateLimiter;
import com.google.common.util.concurrent.RecyclableRateLimiter;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import com.fasterxml.jackson.databind.JsonNode;
import com.squareup.tape.FileException;
import com.squareup.tape.FileObjectQueue;
import com.wavefront.agent.api.ForceQueueEnabledAgentAPI;
import com.wavefront.api.AgentAPI;
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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.io.Reader;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.zip.GZIPInputStream;

import javax.annotation.Nullable;
import javax.ws.rs.core.Response;

import static com.google.common.collect.ImmutableList.of;

/**
 * A wrapper for any AgentAPI that queues up any result posting if the backend is not available.
 * Current data will always be submitted right away (thus prioritizing live data) while background
 * threads will submit backlogged data.
 *
 * @author Clement Pang (clement@wavefront.com)
 */
public class QueuedAgentService implements ForceQueueEnabledAgentAPI {

  private static final int MAX_SPLIT_BATCH_SIZE = 50000; // same value as default pushFlushMaxPoints
  private static final double MAX_RETRY_BACKOFF_BASE_SECONDS = 60.0;
  private static final Logger logger = Logger.getLogger(QueuedAgentService.class.getCanonicalName());

  private final Gson resubmissionTaskMarshaller;
  private final AgentAPI wrapped;
  private final List<ResubmissionTaskQueue> taskQueues;
  private static int splitBatchSize = MAX_SPLIT_BATCH_SIZE;
  private static double retryBackoffBaseSeconds = 2.0;
  private boolean lastKnownQueueSizeIsPositive = true;
  private final ExecutorService executorService;
  private MetricsRegistry metricsRegistry = new MetricsRegistry();
  private Meter resultPostingMeter = metricsRegistry.newMeter(QueuedAgentService.class, "post-result", "results",
      TimeUnit.MINUTES);
  private Counter permitsGranted = Metrics.newCounter(new MetricName("limiter", "", "permits-granted"));
  private Counter permitsDenied = Metrics.newCounter(new MetricName("limiter", "", "permits-denied"));
  private Counter permitsRetried = Metrics.newCounter(new MetricName("limiter", "", "permits-retried"));
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
      new ArrayBlockingQueue<Runnable>(1));

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
  public QueuedAgentService(AgentAPI service, String bufferFile, final int retryThreads,
                            final ScheduledExecutorService executorService, boolean purge,
                            final UUID agentId, final boolean splitPushWhenRateLimited,
                            final String logLevel) throws IOException {
    this(service, bufferFile, retryThreads, executorService, purge,
        agentId, splitPushWhenRateLimited, (RecyclableRateLimiter) null);
  }


  public QueuedAgentService(AgentAPI service, String bufferFile, final int retryThreads,
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
    this.executorService = executorService;
    for (int i = 0; i < retryThreads; i++) {
      final int threadId = i;
      File buffer = new File(bufferFile + "." + i);
      if (purge) {
        if (buffer.delete()) {
          logger.warning("Retry buffer has been purged: " + buffer.getAbsolutePath());
        }
      }
      final ResubmissionTaskQueue taskQueue = new ResubmissionTaskQueue(
          new FileObjectQueue<>(buffer, new FileObjectQueue.Converter<ResubmissionTask>() {
            @Override
            public ResubmissionTask from(byte[] bytes) throws IOException {
              try {
                if (bytes.length > 2 && bytes[0] == (byte) 0x1f && bytes[1] == (byte) 0x8b) {
                  // gzip signature detected (backwards compatibility mode)
                  Reader reader = new InputStreamReader(new GZIPInputStream(new ByteArrayInputStream(bytes)));
                  return resubmissionTaskMarshaller.fromJson(reader, ResubmissionTask.class);
                }
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
          }),
          task -> {
            task.service = wrapped;
            task.currentAgentId = agentId;
          }
      );

      Runnable taskRunnable = new Runnable() {
        private int backoffExponent = 1;

        @Override
        public void run() {
          int successes = 0;
          int failures = 0;
          boolean rateLimiting = false;
          try {
            logger.fine("[RETRY THREAD " + threadId + "] TASK STARTING");
            while (taskQueue.size() > 0 && taskQueue.size() > failures) {
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
                    logger.warning("[RETRY THREAD " + threadId + "] Wavefront server rejected push (413 response). " +
                        "Split data and attempt later: " + ex);
                    List<? extends ResubmissionTask> splitTasks = task.splitTask();
                    for (ResubmissionTask smallerTask : splitTasks) {
                      taskQueue.add(smallerTask);
                    }
                    break;
                  } else //noinspection ThrowableResultOfMethodCallIgnored
                    if (Throwables.getRootCause(ex) instanceof RejectedExecutionException) {
                      // this should either split and remove the original task or keep it at front
                      // it also should not try any more tasks
                      logger.warning("[RETRY THREAD " + threadId + "] Wavefront server rejected the submission. Will " +
                          "attempt later: " + ex);
                      if (splitPushWhenRateLimited) {
                        List<? extends ResubmissionTask> splitTasks = task.splitTask();
                        for (ResubmissionTask smallerTask : splitTasks) {
                          taskQueue.add(smallerTask);
                        }
                      } else {
                        removeTask = false;
                      }
                      break;
                    } else {
                      logger.log(Level.WARNING, "[RETRY THREAD " + threadId + "] cannot submit data to Wavefront servers. Will " +
                          "re-attempt later", ex);
                    }
                  // this can potentially cause a duplicate task to be injected (but since submission is mostly
                  // idempotent it's not really a big deal)
                  task.service = null;
                  task.currentAgentId = null;
                  taskQueue.add(task);
                  if (failures > 10) {
                    logger.warning("[RETRY THREAD " + threadId + "] saw too many submission errors. Will " +
                        "re-attempt later");
                    break;
                  }
                } finally {
                  if (removeTask) taskQueue.remove();
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
                  Math.pow(retryBackoffBaseSeconds, backoffExponent));
              logger.fine("[RETRY THREAD " + threadId + "] Successful Batches: " + successes +
                  ", Failed Batches: " + failures);
              logger.fine("[RETRY THREAD " + threadId + "] RESCHEDULING in " + next);
              executorService.schedule(this, next, TimeUnit.SECONDS);
            }
          }
        }
      };

      executorService.schedule(taskRunnable, (long) (Math.random() * retryThreads), TimeUnit.SECONDS);
      taskQueues.add(taskQueue);
    }

    if (retryThreads > 0) {
      executorService.scheduleAtFixedRate(new Runnable() {
        @Override
        public void run() {
          List<Integer> queueSizes = Lists.newArrayList(Lists.transform(taskQueues,
              new Function<ResubmissionTaskQueue, Integer>() {
                @Override
                public Integer apply(ResubmissionTaskQueue input) {
                  return input.size();
                }
              }
          ));
          if (Iterables.tryFind(queueSizes, new Predicate<Integer>() {
            @Override
            public boolean apply(Integer input) {
              return input > 0;
            }
          }).isPresent()) {
            lastKnownQueueSizeIsPositive = true;
            logger.info("current retry queue sizes: [" + Joiner.on("/").join(queueSizes) + "]");
          } else if (lastKnownQueueSizeIsPositive) {
            lastKnownQueueSizeIsPositive = false;
            logger.info("retry queue has been cleared");
          }
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

  public void shutdown() {
    executorService.shutdown();
  }

  public static void setRetryBackoffBaseSeconds(double newSecs) {
    retryBackoffBaseSeconds = Math.min(newSecs, MAX_RETRY_BACKOFF_BASE_SECONDS);
    retryBackoffBaseSeconds = Math.max(retryBackoffBaseSeconds, 1.0);
  }

  public static void setSplitBatchSize(int newSize) {
    splitBatchSize = Math.min(newSize, MAX_SPLIT_BATCH_SIZE);
    splitBatchSize = Math.max(splitBatchSize, 1);
  }

  public long getQueuedTasksCount() {
    long toReturn = 0;
    for (ResubmissionTaskQueue taskQueue : taskQueues) {
      toReturn += taskQueue.size();
    }
    return toReturn;
  }

  private ResubmissionTaskQueue getSmallestQueue() {
    int size = Integer.MAX_VALUE;
    ResubmissionTaskQueue toReturn = null;
    for (ResubmissionTaskQueue queue : taskQueues) {
      if (queue.size() == 0) {
        return queue;
      } else if (queue.size() < size) {
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
        lz4OutputStream.finish();
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
        logger.warning("Cannot post work unit result to Wavefront servers. Will enqueue and retry later: " + ex);
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
        "Will enqueue and retry later: " + failureException);
    addTaskToSmallestQueue(taskToRetry);
    return Collections.emptyList();
  }

  private void addTaskToSmallestQueue(ResubmissionTask taskToRetry) {
    ResubmissionTaskQueue queue = getSmallestQueue();
    if (queue != null) {
      queue.getLockObject().lock();
      try {
        queue.add(taskToRetry);
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
            throw new RejectedExecutionException("Response not accepted by server: " + response.getStatus() +
                " unclaimed agent - please verify that your token is valid and has Agent Management permission!");
          } else {
            throw new RuntimeException("HTTP " + response.getStatus() + ": Please verify your " +
                "network/HTTP proxy settings!");
          }
        } else {
          throw new RuntimeException("Server error: " + response.getStatus());
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

  public static class PostPushDataResultTask extends ResubmissionTask<PostPushDataResultTask> {

    private final UUID agentId;
    private final UUID workUnitId;
    private final Long currentMillis;
    private final String format;
    private final String pushData;

    public PostPushDataResultTask(UUID agentId, UUID workUnitId, Long currentMillis, String format, String pushData) {
      this.agentId = agentId;
      this.workUnitId = workUnitId;
      this.currentMillis = currentMillis;
      this.format = format;
      this.pushData = pushData;
    }

    @Override
    public void execute(Object callback) {
      Response response;
      try {
        response = service.postPushData(currentAgentId, workUnitId, currentMillis, format, pushData);
      } catch (Exception ex) {
        throw new RuntimeException("Server error: " + Throwables.getRootCause(ex));
      }
      parsePostingResponse(response);
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
        int stride = Math.min(splitBatchSize, (int) Math.ceil((float) numDatum / 2.0));
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
      return StringLineIngester.pushDataSize(pushData);
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
