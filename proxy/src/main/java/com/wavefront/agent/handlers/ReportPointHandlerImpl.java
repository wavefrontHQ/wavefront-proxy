package com.wavefront.agent.handlers;

import static com.wavefront.data.Validation.validatePoint;

import com.wavefront.agent.TokenManager;
import com.wavefront.agent.api.APIContainer;
import com.wavefront.agent.sampler.MetricBloomFilterSampler;
import com.wavefront.api.agent.ValidationConfiguration;
import com.wavefront.api.agent.preprocessor.ReportPointSampleInclude;
import com.wavefront.common.Clock;
import com.wavefront.common.Utils;
import com.wavefront.data.DeltaCounterValueException;
import com.wavefront.ingester.ReportPointSerializer;
import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.BurstRateTrackingCounter;
import com.yammer.metrics.core.Counter;
import com.yammer.metrics.core.MetricName;
import com.yammer.metrics.core.MetricsRegistry;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.logging.Logger;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import wavefront.report.Histogram;
import wavefront.report.ReportPoint;

/**
 * Handler that processes incoming ReportPoint objects, validates them and hands them over to one of
 * the {@link SenderTask} threads.
 *
 * @author vasily@wavefront.com
 */
class ReportPointHandlerImpl extends AbstractReportableEntityHandler<ReportPoint, String> {
  private static final Logger logger =
          Logger.getLogger(ReportPointHandlerImpl.class.getCanonicalName());

  final Logger validItemsLogger;
  final ValidationConfiguration validationConfig;
  final Function<Histogram, Histogram> recompressor;
  final com.yammer.metrics.core.Histogram receivedPointLag;
  final com.yammer.metrics.core.Histogram receivedTagCount;
  final Supplier<Counter> discardedCounterSupplier;
  final Supplier<Counter> sampledCounterSupplier;
  final BurstRateTrackingCounter sampledOutStats;
  final MetricBloomFilterSampler metricBloomFilterSampler;

  /**
   * Precomputed forward-routing table built once at handler initialization.
   *
   * <p>Key = every alias an operator may write in a {@code wf_forward_tenants} annotation
   * (multicastingTenant names, "central", and the {@code defaultTenant} alias).
   * Value = ordered list of sender-task collections, one element per physical endpoint to route
   * to. For the common case of one task per endpoint the list contains exactly one singleton.
   *
   * <p>This collapses the per-point chain of
   * {@code resolveTenantName → TokenManager.getInternalKeysForName → senderTaskMap.get}
   * into a single {@link HashMap#get} call, eliminating repeated static-map reads and
   * branch evaluation from the 200K-points/s hot path.
   *
   * <p>Empty when {@link TokenManager} is not populated (unit-test scenarios); in that case
   * the handler falls back to the direct {@link #getTask} path.
   */
  private final Map<String, List<Collection<SenderTask<String>>>> forwardingTable;

  /**
   * Creates a new instance that handles either histograms or points.
   *
   * @param handlerKey handler key for the metrics pipeline.
   * @param blockedItemsPerBatch controls sample rate of how many blocked points are written into
   *     the main log file.
   * @param senderTaskMap map of tenant name and tasks actually handling data transfer to the
   *     Wavefront endpoint corresponding to the tenant name
   * @param validationConfig validation configuration.
   * @param setupMetrics Whether we should report counter metrics.
   * @param receivedRateSink Where to report received rate.
   * @param blockedItemLogger logger for blocked items (optional).
   * @param validItemsLogger sampling logger for valid items (optional).
   * @param recompressor histogram recompressor (optional)
   * @param metricBloomFilterSampler metric sampler (optional, points only).
   */
  ReportPointHandlerImpl(
      final HandlerKey handlerKey,
      final int blockedItemsPerBatch,
      @Nullable final Map<String, Collection<SenderTask<String>>> senderTaskMap,
      @Nonnull final ValidationConfiguration validationConfig,
      final boolean setupMetrics,
      @Nullable final BiConsumer<String, Long> receivedRateSink,
      @Nullable final Logger blockedItemLogger,
      @Nullable final Logger validItemsLogger,
      @Nullable final Function<Histogram, Histogram> recompressor,
      @Nullable final MetricBloomFilterSampler metricBloomFilterSampler) {
    this(handlerKey, blockedItemsPerBatch, senderTaskMap, validationConfig, setupMetrics,
        receivedRateSink, blockedItemLogger, validItemsLogger, recompressor, metricBloomFilterSampler,
        null);
  }

  ReportPointHandlerImpl(
      final HandlerKey handlerKey,
      final int blockedItemsPerBatch,
      @Nullable final Map<String, Collection<SenderTask<String>>> senderTaskMap,
      @Nonnull final ValidationConfiguration validationConfig,
      final boolean setupMetrics,
      @Nullable final BiConsumer<String, Long> receivedRateSink,
      @Nullable final Logger blockedItemLogger,
      @Nullable final Logger validItemsLogger,
      @Nullable final Function<Histogram, Histogram> recompressor,
      @Nullable final MetricBloomFilterSampler metricBloomFilterSampler,
      @Nullable final String defaultTenant) {
    super(
        handlerKey,
        blockedItemsPerBatch,
        new ReportPointSerializer(),
        senderTaskMap,
        setupMetrics,
        receivedRateSink,
        blockedItemLogger,
        defaultTenant);
    super.initializeCounters();
    this.validationConfig = validationConfig;
    this.validItemsLogger = validItemsLogger;
    this.recompressor = recompressor;
    this.metricBloomFilterSampler = metricBloomFilterSampler;
    MetricsRegistry registry = setupMetrics ? Metrics.defaultRegistry() : LOCAL_REGISTRY;
    this.receivedPointLag =
        registry.newHistogram(
            new MetricName(handlerKey.toString() + ".received", "", "lag"), false);
    this.receivedTagCount =
        registry.newHistogram(
            new MetricName(handlerKey.toString() + ".received", "", "tagCount"), false);
    this.discardedCounterSupplier =
        Utils.lazySupplier(
            () -> Metrics.newCounter(new MetricName(handlerKey.toString(), "", "discarded")));
    this.sampledCounterSupplier =
            Utils.lazySupplier(
                    () -> Metrics.newCounter(new MetricName(handlerKey.toString(), "", "sampled")));
    this.sampledOutStats =
            new BurstRateTrackingCounter(
                    new MetricName(handlerKey.toString(), "", "sampled-burst-rate"), registry, 1000);
    this.forwardingTable = buildForwardingTable(senderTaskMap, defaultTenant);
  }

  /**
   * Builds the precomputed forwarding table from the current {@link TokenManager} state and the
   * supplied sender-task map. Called once at construction time; the result is immutable.
   *
   * <p>Routing rules encoded into the table:
   * <ul>
   *   <li>Normal alias ("tenant1"): all endpoints registered under that alias (fan-out).
   *   <li>"central" written explicitly with a multicasting "central" endpoint: routes only to the
   *       synthetic "central~N" endpoints, not the primary cluster.
   *   <li>{@code defaultTenant} alias (e.g. "Localdev"): always routes to the primary cluster
   *       only — inserted last so it wins over any TokenManager entry for the same string.
   * </ul>
   */
  private static Map<String, List<Collection<SenderTask<String>>>> buildForwardingTable(
      @Nullable Map<String, Collection<SenderTask<String>>> senderTaskMap,
      @Nullable String defaultTenant) {
    if (senderTaskMap == null) {
      return Collections.emptyMap();
    }
    Map<String, List<Collection<SenderTask<String>>>> table = new HashMap<>();

    for (String alias : TokenManager.getRegisteredAliases()) {
      List<String> internalKeys = TokenManager.getInternalKeysForName(alias);

      // Apply the "central" edge-case: if the operator wrote "central" explicitly and a
      // multicasting tenant named "central" exists, route to the synthetic keys only.
      List<String> keysToRoute;
      if (alias.equals(APIContainer.CENTRAL_TENANT_NAME) && internalKeys.size() > 1) {
        keysToRoute = internalKeys.subList(1, internalKeys.size());
      } else {
        keysToRoute = internalKeys;
      }

      List<Collection<SenderTask<String>>> endpointTaskCollections = new ArrayList<>(keysToRoute.size());
      for (String key : keysToRoute) {
        Collection<SenderTask<String>> tasks = senderTaskMap.get(key);
        if (tasks != null && !tasks.isEmpty()) {
          endpointTaskCollections.add(tasks);
        }
      }
      if (!endpointTaskCollections.isEmpty()) {
        table.put(alias, Collections.unmodifiableList(endpointTaskCollections));
      }
    }

    // The defaultTenant alias must always resolve to the primary cluster only, regardless of the
    // "central" edge case above. Insert last so it overwrites any TokenManager entry for the same
    // string (e.g. if someone set defaultTenant="central").
    if (defaultTenant != null) {
      Collection<SenderTask<String>> primaryTasks =
          senderTaskMap.get(APIContainer.CENTRAL_TENANT_NAME);
      if (primaryTasks != null && !primaryTasks.isEmpty()) {
        table.put(defaultTenant, Collections.singletonList(primaryTasks));
      }
    }

    return Collections.unmodifiableMap(table);
  }

  /**
   * Selects the least-loaded task from a collection of tasks for the same endpoint.
   * For single-element collections (the common case) this avoids {@link java.util.stream.Stream}
   * allocation entirely.
   */
  @Nullable
  private static SenderTask<String> selectBestTask(Collection<SenderTask<String>> tasks) {
    if (tasks.isEmpty()) return null;
    if (tasks.size() == 1) return tasks.iterator().next();
    return tasks.stream()
        .min(Comparator.comparingLong(SenderTask::getTaskRelativeScore))
        .orElse(null);
  }

  @Override
  void reportInternal(ReportPoint point) {
    receivedTagCount.update(point.getAnnotations().size());
    try {
      validatePoint(point, validationConfig);
    } catch (DeltaCounterValueException e) {
      discardedCounterSupplier.get().inc();
      return;
    }
    receivedPointLag.update(Clock.now() - point.getTimestamp());
    if (point.getValue() instanceof Histogram && recompressor != null) {
      Histogram histogram = (Histogram) point.getValue();
      point.setValue(recompressor.apply(histogram));
    }
    // Count as received once proxy accepts and validates point, even if sampled out later.
    getReceivedCounter().inc();
    if (metricBloomFilterSampler != null && !metricBloomFilterSampler.isForceDisableSampling() &&
            metricBloomFilterSampler.shouldSampleOut(point)) {
      discardedCounterSupplier.get().inc();
      sampledCounterSupplier.get().inc();
      sampledOutStats.inc();
      return;
    }

    // always try remove sample tag
    point.getAnnotations().remove(ReportPointSampleInclude.SAMPLING_TAG);

    // Forward routing (new): check for a preprocessor-injected forward annotation first.
    // Data goes ONLY to the specified tenants (not automatically to central).
    String forwardAnnotation = point.getAnnotations().remove(FORWARD_ROUTING_KEY);
    // Serialize once with all port-level (defaultTenant) transformations already applied.
    // Per-tenant registry preprocessors are intentionally skipped in the forwarding path so that
    // only the defaultTenant's rules govern both routing and transforms.
    final String serializedPoint = serializer.apply(point);
    if (forwardAnnotation != null) {
      // Deduplicate while preserving insertion order.
      String[] rawTenantNames = forwardAnnotation.split(",");
      LinkedHashSet<String> uniqueTenantNames = new LinkedHashSet<>(rawTenantNames.length);
      for (String rawName : rawTenantNames) uniqueTenantNames.add(rawName.trim());
      for (String tenantName : uniqueTenantNames) {
        // Fast path: single HashMap lookup into the precomputed forwarding table.
        // The table encodes all routing rules (alias resolution, "central" edge-case, fan-out)
        // at construction time, eliminating per-point calls to TokenManager and senderTaskMap.
        List<Collection<SenderTask<String>>> endpointTasks = forwardingTable.get(tenantName);
        if (endpointTasks != null) {
          for (Collection<SenderTask<String>> taskCollection : endpointTasks) {
            SenderTask<String> task = selectBestTask(taskCollection);
            if (task != null) {
              task.add(serializedPoint);
            }
          }
        } else {
          // Fallback: forwardingTable is empty (unit tests where TokenManager is not populated)
          // or the annotation contains an alias not present in the table.
          // Attempt alias resolution + direct senderTaskMap lookup.
          String resolvedAlias = resolveTenantName(tenantName);
          SenderTask<String> task = getTask(resolvedAlias);
          if (task != null) {
            task.add(serializedPoint);
          } else {
            logger.warning("Forward routing: tenant '" + tenantName
                + "' is not registered. Known tenants: " + senderTaskMap.keySet());
          }
        }
      }
    } else {
      // No forward routing annotation: use legacy routing.
      // All unmatched traffic falls through to the central (default) tenant.
      // In multi-tenant deployments this means the central SenderTask queue carries
      // both "legacy fallback" traffic and any explicitly-forwarded Localdev/central
      // points, so its queue depth will typically be higher than other tenants' queues.
      // This is expected behaviour, not a bug — if you observe the central tenant
      // appearing slightly "behind" in ingestion latency, consider adding a catch-all
      // forward rule so that all traffic is routed explicitly.
      getTask(APIContainer.CENTRAL_TENANT_NAME).add(serializedPoint);
      // Legacy multicast: if the point carries a multicastingTenantName tag, also fan out
      // to those additional tenants (kept for backward compatibility with existing configs).
      if (isMulticastingActive
          && point.getAnnotations() != null
          && point.getAnnotations().containsKey(MULTICASTING_TENANT_TAG_KEY)) {
        String[] multicastingTenantNames =
            point.getAnnotations().get(MULTICASTING_TENANT_TAG_KEY).trim().split(",");
        point.getAnnotations().remove(MULTICASTING_TENANT_TAG_KEY);
        for (String multicastingTenantName : multicastingTenantNames) {
          if (getTask(multicastingTenantName) != null) {
            getTask(multicastingTenantName).add(serializer.apply(point));
          }
        }
      }
    }
    if (validItemsLogger != null) validItemsLogger.info(serializedPoint);
  }

  @Override
  protected void printStats() {
    super.printStats();
    if (!reportReceivedStats) return;
    if (receivedStats.getFiveMinuteCount() == 0 && sampledOutStats.getFiveMinuteCount() == 0) {
      return;
    }
    logger.info(
            "["
                    + handlerKey.getHandle()
                    + "] "
                    + handlerKey.getEntityType().toCapitalizedString()
                    + " sampled out rate: "
                    + sampledOutStats.getOneMinutePrintableRate()
                    + " "
                    + rateUnit
                    + " (1 min), "
                    + sampledOutStats.getFiveMinutePrintableRate()
                    + " "
                    + rateUnit
                    + " (5 min), "
                    + sampledOutStats.getCurrentRate()
                    + " "
                    + rateUnit
                    + " (current).");
  }
}
