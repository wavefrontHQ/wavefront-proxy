package com.wavefront.agent;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.base.Predicate;
import com.google.common.base.Throwables;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.squareup.tape.FileObjectQueue;
import com.squareup.tape.TaskInjector;
import com.squareup.tape.TaskQueue;
import com.wavefront.agent.api.ForceQueueEnabledAgentAPI;
import com.wavefront.api.AgentAPI;
import com.wavefront.api.agent.AgentConfiguration;
import com.wavefront.api.agent.ShellOutputDTO;
import com.wavefront.metrics.ExpectedAgentMetric;
import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.Gauge;
import com.yammer.metrics.core.Histogram;
import com.yammer.metrics.core.Meter;
import com.yammer.metrics.core.MetricsRegistry;

import javax.annotation.Nullable;
import javax.ws.rs.core.Response;
import java.io.*;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.*;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

import static com.google.common.collect.ImmutableList.of;

/**
 * A wrapper for any AgentAPI that queues up any result posting if the backend is not available. Current data
 * will always be submitted right away (thus prioritizing live data) while background threads will submit
 * backlogged data.
 *
 * @author Clement Pang (clement@wavefront.com)
 */
public class QueuedAgentService implements ForceQueueEnabledAgentAPI {

  private static final Logger logger = Logger.getLogger(QueuedAgentService.class.getCanonicalName());

  private final Gson resubmissionTaskMarshaller;
  private final AgentAPI wrapped;
  private final List<TaskQueue<ResubmissionTask>> taskQueues;
  private boolean lastKnownQueueSizeIsPositive = true;
  private MetricsRegistry metricsRegistry = new MetricsRegistry();
  private Meter resultPostingMeter = metricsRegistry.newMeter(QueuedAgentService.class, "post-result", "results",
      TimeUnit.MINUTES);
  /**
   * Biases result sizes to the last 5 minutes heavily. This histogram does not see all result sizes. The executor
   * only ever processes one posting at any given time and drops the rest. {@link #resultPostingMeter} records the
   * actual rate (i.e. sees all posting calls).
   */
  private Histogram resultPostingSizes = metricsRegistry.newHistogram(QueuedAgentService.class, "result-size", true);
  /**
   * A single threaded bounded work queue to update result posting sizes.
   */
  private ExecutorService resultPostingSizerExecutorService = new ThreadPoolExecutor(1, 1, 60L, TimeUnit.SECONDS,
      new ArrayBlockingQueue<Runnable>(1));

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

  public QueuedAgentService(AgentAPI service, String bufferFile, int retryThreads,
                            ScheduledExecutorService executorService, boolean purge) throws IOException {
    if (retryThreads <= 0) {
      logger.warning("You have no retry threads set up. Any points that get rejected will be lost.\n Change this by " +
          "setting retryThreads to a value > 0");
    }
    resubmissionTaskMarshaller = new GsonBuilder().
        registerTypeHierarchyAdapter(ResubmissionTask.class, new ResubmissionTaskDeserializer()).create();
    this.wrapped = service;
    this.taskQueues = Lists.newArrayListWithExpectedSize(retryThreads);
    for (int i = 0; i < retryThreads; i++) {
      final int threadId = i;
      File buffer = new File(bufferFile + "." + i);
      if (purge) {
        if (buffer.delete()) {
          logger.warning("Retry buffer has been purged: " + buffer.getAbsolutePath());
        }
      }
      final TaskQueue<ResubmissionTask> taskQueue = new TaskQueue<>(
          new FileObjectQueue<>(buffer, new FileObjectQueue.Converter<ResubmissionTask>() {
            @Override
            public ResubmissionTask from(byte[] bytes) throws IOException {
              try {
                Reader reader = new InputStreamReader(new GZIPInputStream(new ByteArrayInputStream(bytes)));
                return resubmissionTaskMarshaller.fromJson(reader, ResubmissionTask.class);
              } catch (Throwable t) {
                logger.warning("Failed to read a single retry submission from buffer, ignoring: " + t);
                return null;
              }
            }

            @Override
            public void toStream(ResubmissionTask o, OutputStream bytes) throws IOException {
              GZIPOutputStream gzipOutputStream = new GZIPOutputStream(bytes);
              Writer writer = new OutputStreamWriter(gzipOutputStream);
              resubmissionTaskMarshaller.toJson(o, writer);
              writer.close();
              gzipOutputStream.finish();
              gzipOutputStream.close();
            }
          }),
          new TaskInjector<ResubmissionTask>() {
            @Override
            public void injectMembers(ResubmissionTask task) {
              task.service = wrapped;
            }
          }
      );

      executorService.scheduleWithFixedDelay(new Runnable() {
        @Override
        public void run() {
          try {
            int failures = 0;
            while (taskQueue.size() > 0 && taskQueue.size() > failures) {
              synchronized (taskQueue) {
                ResubmissionTask task = taskQueue.peek();
                boolean removeTask = true;
                try {
                  if (task != null) {
                    task.execute(null);
                  }
                } catch (Exception ex) {
                  failures++;
                  //noinspection ThrowableResultOfMethodCallIgnored
                  if (Throwables.getRootCause(ex) instanceof QueuedPushTooLargeException) {
                    logger.warning("[RETRY THREAD " + threadId + "] Wavefront server rejected push (413 response). " +
                        "Split data and attempt later: " + ex);
                    List<? extends ResubmissionTask> splitTasks = task.splitTask();
                    for (ResubmissionTask smallerTask : splitTasks) {
                      taskQueue.add(smallerTask);
                    }
                    // this should remove the task from the queue
                    break;
                  } else //noinspection ThrowableResultOfMethodCallIgnored
                    if (Throwables.getRootCause(ex) instanceof RejectedExecutionException) {
                      logger.warning("[RETRY THREAD " + threadId + "] Wavefront server quiesced (406 response). Will " +
                          "re-attempt later: " + ex);
                      removeTask = false;
                      break;
                    } else {
                      logger.warning("[RETRY THREAD " + threadId + "] cannot submit data to Wavefront servers. Will " +
                          "re-attempt later: " + ex);
                    }
                  // this can potentially cause a duplicate task to be injected (but since submission is mostly
                  // idempotent it's not really a big deal)
                  task.service = null;
                  taskQueue.add(task);
                  if (failures > 10) {
                    logger.warning("[RETRY THREAD " + threadId + "] saw too many submission errors. Will re-attempt " +
                        "in 30s");
                    break;
                  }
                } finally {
                  if (removeTask) taskQueue.remove();
                }
              }
            }
          } catch (Throwable ex) {
            logger.log(Level.WARNING, "[RETRY THREAD " + threadId + "] unexpected exception", ex);
          }
        }
      }, (long) (Math.random() * 30), 30, TimeUnit.SECONDS);
      taskQueues.add(taskQueue);
    }

    if (retryThreads > 0) {
      executorService.scheduleAtFixedRate(new Runnable() {
        @Override
        public void run() {
          List<Integer> queueSizes = Lists.newArrayList(Lists.transform(taskQueues,
              new Function<TaskQueue<ResubmissionTask>, Integer>() {
                @Override
                public Integer apply(TaskQueue<ResubmissionTask> input) {
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
            logger.warning("current retry queue sizes: [" + Joiner.on("/").join(queueSizes) + "]");
          } else if (lastKnownQueueSizeIsPositive) {
            lastKnownQueueSizeIsPositive = false;
            logger.warning("retry queue has been cleared");
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

  public long getQueuedTasksCount() {
    long toReturn = 0;
    for (TaskQueue<ResubmissionTask> taskQueue : taskQueues) {
      toReturn += taskQueue.size();
    }
    return toReturn;
  }

  private TaskQueue<ResubmissionTask> getSmallestQueue() {
    int size = Integer.MAX_VALUE;
    TaskQueue<ResubmissionTask> toReturn = null;
    for (TaskQueue<ResubmissionTask> queue : taskQueues) {
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
    return new Runnable() {
      @Override
      public void run() {
        try {
          ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
          GZIPOutputStream gzipOutputStream = new GZIPOutputStream(outputStream);
          Writer writer = new OutputStreamWriter(gzipOutputStream);
          resubmissionTaskMarshaller.toJson(task, writer);
          writer.close();
          gzipOutputStream.finish();
          gzipOutputStream.close();
          resultPostingSizes.update(outputStream.size());
        } catch (Throwable t) {
          // ignored. this is a stats task.
        }
      }
    };
  }

  private void scheduleTaskForSizing(ResubmissionTask task) {
    try {
      resultPostingSizerExecutorService.submit(getPostingSizerTask(task));
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
                                    Boolean localAgent, JsonNode agentMetrics, Boolean pushAgent) {
    return wrapped.checkin(agentId, hostname, token, version, currentMillis, localAgent, agentMetrics, pushAgent);
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
          postPushData(splitTask.getAgentId(), splitTask.getWorkUnitId(), splitTask.getCurrentMillis(),
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
    logger.warning("Cannot post push data result to Wavefront servers. Will enqueue and retry later: " + failureException);
    addTaskToSmallestQueue(taskToRetry);
    return Collections.emptyList();
  }

  private void addTaskToSmallestQueue(ResubmissionTask taskToRetry) {
    TaskQueue<ResubmissionTask> queue = getSmallestQueue();
    if (queue != null) {
      synchronized (queue) {
        queue.add(taskToRetry);
      }
    } else {
      logger.warning("CRITICAL: No retry queues found. Losing points!");
    }
  }

  private static void parsePostingResponse(Response response) {
    if (response.getStatus() != Response.Status.OK.getStatusCode()) {
      if (response.getStatus() == Response.Status.NOT_ACCEPTABLE.getStatusCode()) {
        throw new RejectedExecutionException("Response not accepted by server: " + response.getStatus());
      } else if (response.getStatus() == Response.Status.REQUEST_ENTITY_TOO_LARGE.getStatusCode()) {
        throw new QueuedPushTooLargeException("Request too large: " + response.getStatus());
      } else {
        throw new RuntimeException("Server error: " + response.getStatus());
      }
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
      parsePostingResponse(service.postWorkUnitResult(agentId, workUnitId, hostId, shellOutputDTO));
    }

    @Override
    public List<PostWorkUnitResultTask> splitTask() {
      // doesn't make sense to split this, so just return a new task
      return of(new PostWorkUnitResultTask(agentId, workUnitId, hostId, shellOutputDTO));
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
      parsePostingResponse(service.postPushData(agentId, workUnitId, currentMillis, format, pushData));
    }

    @Override
    public List<PostPushDataResultTask> splitTask() {
      // pull the pushdata back apart to split and put back together
      List<PostPushDataResultTask> splitTasks = Lists.newArrayList();

      List<String> pushDatum = GraphiteStringHandler.unjoinPushData(pushData);

      // split data into 2 tasks w/ half the strings
      if (pushDatum.size() > 1) {
        int halfPoints = pushDatum.size() / 2;
        splitTasks.add(new PostPushDataResultTask(agentId, workUnitId, currentMillis, format,
            GraphiteStringHandler.joinPushData(new ArrayList<>(pushDatum.subList(0, halfPoints)))));
        splitTasks.add(new PostPushDataResultTask(agentId, workUnitId, currentMillis, format,
            GraphiteStringHandler.joinPushData(new ArrayList<>(pushDatum.subList(halfPoints,
                pushDatum.size())))
        ));
      } else {
        // 1 or 0
        splitTasks.add(new PostPushDataResultTask(agentId, workUnitId, currentMillis, format, pushData));
      }

      return splitTasks;
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
