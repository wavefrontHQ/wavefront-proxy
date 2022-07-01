package com.wavefront.agent.handlers;

import static com.wavefront.agent.api.APIContainer.CENTRAL_TENANT_NAME;
import static com.wavefront.api.agent.Constants.*;

import com.google.common.collect.Maps;
import com.wavefront.agent.api.APIContainer;
import com.wavefront.agent.buffer.Buffer;
import com.wavefront.agent.buffer.QueueInfo;
import com.wavefront.agent.data.EntityProperties;
import com.wavefront.agent.data.EntityPropertiesFactory;
import com.wavefront.api.ProxyV2API;
import com.wavefront.common.Managed;
import com.wavefront.common.NamedThreadFactory;
import com.wavefront.data.ReportableEntityType;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;
import javax.annotation.Nonnull;

/**
 * Factory for {@link SenderTask} objects.
 *
 * @author vasily@wavefront.com
 */
public class SenderTaskFactoryImpl implements SenderTaskFactory {
  private final Logger log = Logger.getLogger(SenderTaskFactoryImpl.class.getCanonicalName());

  private final Map<String, ScheduledExecutorService> executors = new ConcurrentHashMap<>();
  private final Map<QueueInfo, List<SenderTask>> managedTasks = new ConcurrentHashMap<>();

  private final APIContainer apiContainer;
  private final UUID proxyId;
  private final Map<String, EntityPropertiesFactory> entityPropsFactoryMap;

  /**
   * Create new instance.
   *
   * @param apiContainer handles interaction with Wavefront servers as well as queueing.
   * @param proxyId proxy ID.
   * @param entityPropsFactoryMap map of factory for entity-specific wrappers for multiple
   *     multicasting mutable proxy settings.
   */
  public SenderTaskFactoryImpl(
      final APIContainer apiContainer,
      final UUID proxyId,
      final Map<String, EntityPropertiesFactory> entityPropsFactoryMap) {
    this.apiContainer = apiContainer;
    this.proxyId = proxyId;
    this.entityPropsFactoryMap = entityPropsFactoryMap;
    // global `~proxy.buffer.fill-rate` metric aggregated from all task size estimators
    // TODO: create this metric
    //    Metrics.newGauge(
    //        new TaggedMetricName("buffer", "fill-rate"),
    //        new Gauge<Long>() {
    //          @Override
    //          public Long value() {
    //            List<Long> sizes =
    //                taskSizeEstimators.values().stream()
    //                    .map(TaskSizeEstimator::getBytesPerMinute)
    //                    .filter(Objects::nonNull)
    //                    .collect(Collectors.toList());
    //            return sizes.size() == 0 ? null : sizes.stream().mapToLong(x -> x).sum();
    //          }
    //        });
  }

  public void createSenderTasks(@Nonnull QueueInfo info, Buffer buffer) {
    ReportableEntityType entityType = info.getEntityType();

    ScheduledExecutorService scheduler;
    Map<String, Collection<SenderTask>> toReturn = Maps.newHashMap();
    for (String tenantName : apiContainer.getTenantNameList()) {
      int numThreads = entityPropsFactoryMap.get(tenantName).get(entityType).getFlushThreads();
      scheduler =
          executors.computeIfAbsent(
              info.getQueue(),
              x ->
                  Executors.newScheduledThreadPool(
                      numThreads, new NamedThreadFactory("submitter-" + info.getQueue())));

      generateSenderTaskList(info, numThreads, scheduler, buffer);
    }
  }

  private Collection<SenderTask> generateSenderTaskList(
      QueueInfo queue, int numThreads, ScheduledExecutorService scheduler, Buffer buffer) {
    String tenantName = queue.getTenantName();
    if (tenantName == null) {
      tenantName = CENTRAL_TENANT_NAME;
    }
    ReportableEntityType entityType = queue.getEntityType();
    List<SenderTask> senderTaskList = new ArrayList<>(numThreads);
    ProxyV2API proxyV2API = apiContainer.getProxyV2APIForTenant(tenantName);
    EntityProperties properties = entityPropsFactoryMap.get(tenantName).get(entityType);
    for (int threadNo = 0; threadNo < numThreads; threadNo++) {
      SenderTask senderTask;
      switch (entityType) {
        case POINT:
        case DELTA_COUNTER:
          senderTask =
              new LineDelimitedSenderTask(
                  queue,
                  PUSH_FORMAT_WAVEFRONT,
                  proxyV2API,
                  proxyId,
                  properties,
                  scheduler,
                  threadNo,
                  buffer);
          break;
        case HISTOGRAM:
          senderTask =
              new LineDelimitedSenderTask(
                  queue,
                  PUSH_FORMAT_HISTOGRAM,
                  proxyV2API,
                  proxyId,
                  properties,
                  scheduler,
                  threadNo,
                  buffer);
          break;
        case SOURCE_TAG:
          // In MONIT-25479, SOURCE_TAG does not support tag based multicasting. But still
          // generated tasks for each tenant in case we have other multicasting mechanism
          senderTask =
              new SourceTagSenderTask(
                  queue,
                  apiContainer.getSourceTagAPIForTenant(tenantName),
                  properties,
                  scheduler,
                  buffer);
          break;
        case TRACE:
          senderTask =
              new LineDelimitedSenderTask(
                  queue,
                  PUSH_FORMAT_TRACING,
                  proxyV2API,
                  proxyId,
                  properties,
                  scheduler,
                  threadNo,
                  buffer);
          break;
        case TRACE_SPAN_LOGS:
          // In MONIT-25479, TRACE_SPAN_LOGS does not support tag based multicasting. But still
          // generated tasks for each tenant in case we have other multicasting mechanism
          senderTask =
              new LineDelimitedSenderTask(
                  queue,
                  PUSH_FORMAT_TRACING_SPAN_LOGS,
                  proxyV2API,
                  proxyId,
                  properties,
                  scheduler,
                  threadNo,
                  buffer);
          break;
        case EVENT:
          senderTask =
              new EventSenderTask(
                  queue,
                  apiContainer.getEventAPIForTenant(tenantName),
                  proxyId,
                  properties,
                  scheduler,
                  buffer);
          break;
        case LOGS:
          senderTask =
              new LogSenderTask(
                  queue,
                  apiContainer.getLogAPI(),
                  proxyId,
                  entityPropsFactoryMap.get(tenantName).get(entityType),
                  scheduler,
                  buffer);
          break;
        default:
          throw new IllegalArgumentException(
              "Unexpected entity type " + queue.getEntityType().name());
      }
      senderTaskList.add(senderTask);
      senderTask.start();
    }
    managedTasks.put(queue, senderTaskList);
    return senderTaskList;
  }

  @Override
  public void shutdown() {
    managedTasks.values().stream().flatMap(Collection::stream).forEach(Managed::stop);
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
    // TODO: review
    //    for (String tenantName : apiContainer.getTenantNameList()) {
    //      String tenantHandlerKey = HandlerKey.generateTenantSpecificHandle(handle, tenantName);
    //      List<ReportableEntityType> types = entityTypes.get(tenantHandlerKey);
    //      if (types == null) return;
    //      try {
    //        types.forEach(
    //            x -> taskSizeEstimators.remove(new HandlerKey(x, handle, tenantName)).shutdown());
    //        types.forEach(x -> managedServices.remove(new HandlerKey(x, handle,
    // tenantName)).stop());
    //        types.forEach(
    //            x ->
    //                managedTasks
    //                    .remove(new HandlerKey(x, handle, tenantName))
    //                    .forEach(
    //                        t -> {
    //                          t.stop();
    //                        }));
    //        types.forEach(x -> executors.remove(new HandlerKey(x, handle,
    // tenantName)).shutdown());
    //      } finally {
    //        entityTypes.remove(tenantHandlerKey);
    //      }
    //    }
  }

  // TODO: review
  @Override
  public void truncateBuffers() {}
}
