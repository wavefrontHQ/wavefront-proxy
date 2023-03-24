package com.wavefront.agent.handlers;

import static com.wavefront.api.agent.Constants.PUSH_FORMAT_HISTOGRAM;
import static com.wavefront.api.agent.Constants.PUSH_FORMAT_TRACING;
import static com.wavefront.api.agent.Constants.PUSH_FORMAT_TRACING_SPAN_LOGS;
import static com.wavefront.api.agent.Constants.PUSH_FORMAT_WAVEFRONT;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Maps;
import com.wavefront.agent.api.APIContainer;
import com.wavefront.agent.data.EntityProperties;
import com.wavefront.agent.data.EntityPropertiesFactory;
import com.wavefront.agent.data.QueueingReason;
import com.wavefront.agent.queueing.QueueController;
import com.wavefront.agent.queueing.QueueingFactory;
import com.wavefront.agent.queueing.TaskQueueFactory;
import com.wavefront.agent.queueing.TaskSizeEstimator;
import com.wavefront.api.ProxyV2API;
import com.wavefront.common.Managed;
import com.wavefront.common.NamedThreadFactory;
import com.wavefront.common.TaggedMetricName;
import com.wavefront.data.ReportableEntityType;
import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.Gauge;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * Factory for {@link SenderTask} objects.
 *
 * @author vasily@wavefront.com
 */
public class SenderTaskFactoryImpl implements SenderTaskFactory {
  private final Logger log = Logger.getLogger(SenderTaskFactoryImpl.class.getCanonicalName());

  private final Map<String, List<ReportableEntityType>> entityTypes = new ConcurrentHashMap<>();
  private final Map<HandlerKey, ScheduledExecutorService> executors = new ConcurrentHashMap<>();
  private final Map<HandlerKey, List<SenderTask<?>>> managedTasks = new ConcurrentHashMap<>();
  private final Map<HandlerKey, QueueController> managedServices = new ConcurrentHashMap<>();

  /** Keep track of all {@link TaskSizeEstimator} instances to calculate global buffer fill rate. */
  private final Map<HandlerKey, TaskSizeEstimator> taskSizeEstimators = new ConcurrentHashMap<>();

  private final APIContainer apiContainer;
  private final UUID proxyId;
  private final TaskQueueFactory taskQueueFactory;
  private final QueueingFactory queueingFactory;
  private final Map<String, EntityPropertiesFactory> entityPropsFactoryMap;

  /**
   * Create new instance.
   *
   * @param apiContainer handles interaction with Wavefront servers as well as queueing.
   * @param proxyId proxy ID.
   * @param taskQueueFactory factory for backing queues.
   * @param queueingFactory factory for queueing.
   * @param entityPropsFactoryMap map of factory for entity-specific wrappers for multiple
   *     multicasting mutable proxy settings.
   */
  public SenderTaskFactoryImpl(
      final APIContainer apiContainer,
      final UUID proxyId,
      final TaskQueueFactory taskQueueFactory,
      @Nullable final QueueingFactory queueingFactory,
      final Map<String, EntityPropertiesFactory> entityPropsFactoryMap) {
    this.apiContainer = apiContainer;
    this.proxyId = proxyId;
    this.taskQueueFactory = taskQueueFactory;
    this.queueingFactory = queueingFactory;
    this.entityPropsFactoryMap = entityPropsFactoryMap;
    // global `~proxy.buffer.fill-rate` metric aggregated from all task size estimators
    Metrics.newGauge(
        new TaggedMetricName("buffer", "fill-rate"),
        new Gauge<Long>() {
          @Override
          public Long value() {
            List<Long> sizes =
                taskSizeEstimators.values().stream()
                    .map(TaskSizeEstimator::getBytesPerMinute)
                    .filter(Objects::nonNull)
                    .collect(Collectors.toList());
            return sizes.size() == 0 ? null : sizes.stream().mapToLong(x -> x).sum();
          }
        });
  }

  @SuppressWarnings("unchecked")
  public Map<String, Collection<SenderTask<?>>> createSenderTasks(@Nonnull HandlerKey handlerKey) {
    ReportableEntityType entityType = handlerKey.getEntityType();
    String handle = handlerKey.getHandle();

    ScheduledExecutorService scheduler;
    Map<String, Collection<SenderTask<?>>> toReturn = Maps.newHashMap();
    // MONIT-25479: HandlerKey(EntityType, Port) --> HandlerKey(EntityType, Port, TenantName)
    // Every SenderTask is tenant specific from this point
    for (String tenantName : apiContainer.getTenantNameList()) {
      int numThreads =
          1; // entityPropsFactoryMap.get(tenantName).get(entityType).getFlushThreads();
      HandlerKey tenantHandlerKey = HandlerKey.of(entityType, handle, tenantName);

      scheduler =
          executors.computeIfAbsent(
              tenantHandlerKey,
              x ->
                  Executors.newScheduledThreadPool(
                      numThreads,
                      new NamedThreadFactory(
                          "submitter-"
                              + tenantHandlerKey.getEntityType()
                              + "-"
                              + tenantHandlerKey.getHandle())));

      toReturn.put(tenantName, generateSenderTaskList(tenantHandlerKey, numThreads, scheduler));
    }
    return toReturn;
  }

  private Collection<SenderTask<?>> generateSenderTaskList(
      HandlerKey handlerKey, int numThreads, ScheduledExecutorService scheduler) {
    String tenantName = handlerKey.getTenantName();
    if (tenantName == null) {
      throw new IllegalArgumentException(
          "Tenant name in handlerKey should not be null when " + "generating sender task list.");
    }
    System.out.println("generateSenderTaskList, number of threads: " + numThreads);
    TaskSizeEstimator taskSizeEstimator = new TaskSizeEstimator(handlerKey.getHandle());
    taskSizeEstimators.put(handlerKey, taskSizeEstimator);
    ReportableEntityType entityType = handlerKey.getEntityType();
    List<SenderTask<?>> senderTaskList = new ArrayList<>(numThreads);
    ProxyV2API proxyV2API = apiContainer.getProxyV2APIForTenant(tenantName);
    EntityProperties properties = entityPropsFactoryMap.get(tenantName).get(entityType);
    System.out.println("Entity type: " + entityType);
    for (int threadNo = 0; threadNo < numThreads; threadNo++) {
      SenderTask<?> senderTask;
      switch (entityType) {
        case POINT:
        case DELTA_COUNTER:
          senderTask =
              new LineDelimitedSenderTask(
                  handlerKey,
                  PUSH_FORMAT_WAVEFRONT,
                  proxyV2API,
                  proxyId,
                  properties,
                  scheduler,
                  threadNo,
                  taskSizeEstimator,
                  taskQueueFactory.getTaskQueue(handlerKey, threadNo));
          break;
        case HISTOGRAM:
          senderTask =
              new LineDelimitedSenderTask(
                  handlerKey,
                  PUSH_FORMAT_HISTOGRAM,
                  proxyV2API,
                  proxyId,
                  properties,
                  scheduler,
                  threadNo,
                  taskSizeEstimator,
                  taskQueueFactory.getTaskQueue(handlerKey, threadNo));
          break;
        case SOURCE_TAG:
          // In MONIT-25479, SOURCE_TAG does not support tag based multicasting. But still
          // generated tasks for each tenant in case we have other multicasting mechanism
          senderTask =
              new SourceTagSenderTask(
                  handlerKey,
                  apiContainer.getSourceTagAPIForTenant(tenantName),
                  threadNo,
                  properties,
                  scheduler,
                  taskQueueFactory.getTaskQueue(handlerKey, threadNo));
          break;
        case TRACE:
          senderTask =
              new LineDelimitedSenderTask(
                  handlerKey,
                  PUSH_FORMAT_TRACING,
                  proxyV2API,
                  proxyId,
                  properties,
                  scheduler,
                  threadNo,
                  taskSizeEstimator,
                  taskQueueFactory.getTaskQueue(handlerKey, threadNo));
          break;
        case TRACE_SPAN_LOGS:
          // In MONIT-25479, TRACE_SPAN_LOGS does not support tag based multicasting. But
          // still
          // generated tasks for each tenant in case we have other multicasting mechanism
          senderTask =
              new LineDelimitedSenderTask(
                  handlerKey,
                  PUSH_FORMAT_TRACING_SPAN_LOGS,
                  proxyV2API,
                  proxyId,
                  properties,
                  scheduler,
                  threadNo,
                  taskSizeEstimator,
                  taskQueueFactory.getTaskQueue(handlerKey, threadNo));
          break;
        case EVENT:
          senderTask =
              new EventSenderTask(
                  handlerKey,
                  apiContainer.getEventAPIForTenant(tenantName),
                  proxyId,
                  threadNo,
                  properties,
                  scheduler,
                  taskQueueFactory.getTaskQueue(handlerKey, threadNo));
          break;
        case LOGS:
          System.out.println("Creating LogSenderTask");
          senderTask =
              new LogSenderTask(
                  handlerKey,
                  apiContainer.getLogAPI(),
                  proxyId,
                  threadNo,
                  entityPropsFactoryMap.get(tenantName).get(entityType),
                  scheduler,
                  taskQueueFactory.getTaskQueue(handlerKey, threadNo));
          break;
        default:
          throw new IllegalArgumentException(
              "Unexpected entity type "
                  + handlerKey.getEntityType().name()
                  + " for "
                  + handlerKey.getHandle());
      }
      senderTaskList.add(senderTask);
      senderTask.start();
    }
    if (queueingFactory != null) {
      QueueController<?> controller = queueingFactory.getQueueController(handlerKey, numThreads);
      managedServices.put(handlerKey, controller);
      controller.start();
    }
    managedTasks.put(handlerKey, senderTaskList);
    entityTypes
        .computeIfAbsent(handlerKey.getHandle(), x -> new ArrayList<>())
        .add(handlerKey.getEntityType());
    return senderTaskList;
  }

  @Override
  public void shutdown() {
    managedTasks.values().stream().flatMap(Collection::stream).forEach(Managed::stop);
    taskSizeEstimators.values().forEach(TaskSizeEstimator::shutdown);
    managedServices.values().forEach(Managed::stop);
    executors
        .values()
        .forEach(
            x -> {
              try {
                x.shutdown();
                x.awaitTermination(1000, TimeUnit.MILLISECONDS);
              } catch (InterruptedException e) {
                // ignore
              }
            });
  }

  /**
   * shutdown() is called from outside layer where handle is not tenant specific in order to
   * properly shut down all tenant specific tasks, iterate through the tenant list and shut down
   * correspondingly.
   *
   * @param handle pipeline's handle
   */
  @Override
  public void shutdown(@Nonnull String handle) {
    for (String tenantName : apiContainer.getTenantNameList()) {
      String tenantHandlerKey = HandlerKey.generateTenantSpecificHandle(handle, tenantName);
      List<ReportableEntityType> types = entityTypes.get(tenantHandlerKey);
      if (types == null) return;
      try {
        types.forEach(
            x -> taskSizeEstimators.remove(HandlerKey.of(x, handle, tenantName)).shutdown());
        types.forEach(x -> managedServices.remove(HandlerKey.of(x, handle, tenantName)).stop());
        types.forEach(
            x ->
                managedTasks
                    .remove(HandlerKey.of(x, handle, tenantName))
                    .forEach(
                        t -> {
                          t.stop();
                          t.drainBuffersToQueue(null);
                        }));
        types.forEach(x -> executors.remove(HandlerKey.of(x, handle, tenantName)).shutdown());
      } finally {
        entityTypes.remove(tenantHandlerKey);
      }
    }
  }

  @Override
  public void drainBuffersToQueue(QueueingReason reason) {
    managedTasks.values().stream()
        .flatMap(Collection::stream)
        .forEach(x -> x.drainBuffersToQueue(reason));
  }

  @Override
  public void truncateBuffers() {
    managedServices
        .entrySet()
        .forEach(
            handlerKeyManagedEntry -> {
              System.out.println(
                  "Truncating buffers: Queue with handlerKey " + handlerKeyManagedEntry.getKey());
              log.info(
                  "Truncating buffers: Queue with handlerKey " + handlerKeyManagedEntry.getKey());
              QueueController pp = handlerKeyManagedEntry.getValue();
              pp.truncateBuffers();
            });
  }

  @VisibleForTesting
  public void flushNow(@Nonnull HandlerKey handlerKey) {
    HandlerKey tenantHandlerKey;
    ReportableEntityType entityType = handlerKey.getEntityType();
    String handle = handlerKey.getHandle();
    for (String tenantName : apiContainer.getTenantNameList()) {
      tenantHandlerKey = HandlerKey.of(entityType, handle, tenantName);
      managedTasks
          .get(tenantHandlerKey)
          .forEach(
              task -> {
                if (task instanceof AbstractSenderTask) {
                  ((AbstractSenderTask<?>) task).run();
                }
              });
    }
  }
}
