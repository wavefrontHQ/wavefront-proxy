package com.wavefront.agent.core.senders;

import static com.wavefront.agent.ProxyContext.entityPropertiesFactoryMap;
import static com.wavefront.api.agent.Constants.*;

import com.wavefront.agent.api.APIContainer;
import com.wavefront.agent.core.buffers.Buffer;
import com.wavefront.agent.core.queues.QueueInfo;
import com.wavefront.agent.data.EntityProperties;
import com.wavefront.api.ProxyV2API;
import com.wavefront.common.NamedThreadFactory;
import com.wavefront.data.ReportableEntityType;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;
import javax.annotation.Nonnull;

/** Factory for {@link SenderTask} objects. */
public class SenderTasksManager {
  private static final Map<String, ScheduledExecutorService> executors = new ConcurrentHashMap<>();
  private static APIContainer apiContainer;
  private static UUID proxyId;
  private static final Logger log = Logger.getLogger(SenderTasksManager.class.getCanonicalName());

  /**
   * @param apiContainer handles interaction with Wavefront servers as well as queueing.
   * @param proxyId proxy ID.
   */
  public static void init(final APIContainer apiContainer, final UUID proxyId) {
    SenderTasksManager.apiContainer = apiContainer;
    SenderTasksManager.proxyId = proxyId;
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

  public static void createSenderTasks(@Nonnull QueueInfo info, Buffer buffer, double factor) {
    ReportableEntityType entityType = info.getEntityType();
    String tenantName = info.getTenant();

    int numThreads = entityPropertiesFactoryMap.get(tenantName).get(entityType).getFlushThreads();
    int interval =
        entityPropertiesFactoryMap.get(tenantName).get(entityType).getPushFlushInterval();
    ScheduledExecutorService scheduler =
        executors.computeIfAbsent(
            info.getName(),
            x ->
                Executors.newScheduledThreadPool(
                    numThreads, new NamedThreadFactory("submitter-" + info.getName())));

    for (int i = 0; i < numThreads * factor; i++) {
      SenderTask sender = generateSenderTask(info, i, buffer);
      scheduler.scheduleAtFixedRate(sender, interval, interval, TimeUnit.MILLISECONDS);
    }
  }

  public static void shutdown() {
    // TODO: stop the executor and flush all points to disk
    executors.forEach(
        (s, scheduler) -> {
          try {
            System.out.println("Stopping '" + s + "' threads");
            scheduler.shutdown();
            scheduler.awaitTermination(1, TimeUnit.MINUTES);
          } catch (InterruptedException e) {
            throw new RuntimeException(e);
          }
        });
    executors.clear();
  }

  private static SenderTask generateSenderTask(QueueInfo queue, int idx, Buffer buffer) {
    String tenantName = queue.getTenant();
    ReportableEntityType entityType = queue.getEntityType();
    ProxyV2API proxyV2API = apiContainer.getProxyV2APIForTenant(tenantName);
    EntityProperties properties = entityPropertiesFactoryMap.get(tenantName).get(entityType);
    SenderTask senderTask;
    switch (entityType) {
      case POINT:
      case DELTA_COUNTER:
        senderTask =
            new LineDelimitedSenderTask(
                queue, idx, PUSH_FORMAT_WAVEFRONT, proxyV2API, proxyId, properties, buffer);
        break;
      case HISTOGRAM:
        senderTask =
            new LineDelimitedSenderTask(
                queue, idx, PUSH_FORMAT_HISTOGRAM, proxyV2API, proxyId, properties, buffer);
        break;
      case SOURCE_TAG:
        // In MONIT-25479, SOURCE_TAG does not support tag based multicasting. But still
        // generated tasks for each tenant in case we have other multicasting mechanism
        senderTask =
            new SourceTagSenderTask(
                queue, idx, apiContainer.getSourceTagAPIForTenant(tenantName), properties, buffer);
        break;
      case TRACE:
        senderTask =
            new LineDelimitedSenderTask(
                queue, idx, PUSH_FORMAT_TRACING, proxyV2API, proxyId, properties, buffer);
        break;
      case TRACE_SPAN_LOGS:
        // In MONIT-25479, TRACE_SPAN_LOGS does not support tag based multicasting. But still
        // generated tasks for each tenant in case we have other multicasting mechanism
        senderTask =
            new LineDelimitedSenderTask(
                queue, idx, PUSH_FORMAT_TRACING_SPAN_LOGS, proxyV2API, proxyId, properties, buffer);
        break;
      case EVENT:
        senderTask =
            new EventSenderTask(
                queue,
                idx,
                apiContainer.getEventAPIForTenant(tenantName),
                proxyId,
                properties,
                buffer);
        break;
      case LOGS:
        senderTask =
            new LogSenderTask(
                queue,
                idx,
                apiContainer.getLogAPI(),
                proxyId,
                entityPropertiesFactoryMap.get(tenantName).get(entityType),
                buffer);
        break;
      default:
        throw new IllegalArgumentException(
            "Unexpected entity type " + queue.getEntityType().name());
    }
    return senderTask;
  }

  // TODO: review and move to BuffersManager
  public static void truncateBuffers() {}
}
