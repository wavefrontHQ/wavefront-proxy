package com.wavefront.agent.core.handlers;

import static com.wavefront.agent.ProxyContext.entityPropertiesFactoryMap;
import static com.wavefront.agent.ProxyContext.queuesManager;
import static com.wavefront.data.ReportableEntityType.*;

import com.wavefront.agent.core.queues.QueueInfo;
import com.wavefront.api.agent.ValidationConfiguration;
import com.wavefront.common.Utils;
import com.wavefront.common.logger.SamplingLogger;
import com.wavefront.data.ReportableEntityType;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;
import java.util.logging.Logger;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import org.apache.commons.lang.math.NumberUtils;
import wavefront.report.Histogram;

/**
 * Caching factory for {@link ReportableEntityHandler} objects. Makes sure there's only one handler
 * for each {@link com.wavefront.agent.core.queues.QueueInfo}, which makes it possible to spin up
 * handlers on demand at runtime, as well as redirecting traffic to a different pipeline.
 */
public class ReportableEntityHandlerFactoryImpl implements ReportableEntityHandlerFactory {
  private static final Logger logger = Logger.getLogger("sampling");

  public static final Logger VALID_POINTS_LOGGER =
      new SamplingLogger(
          ReportableEntityType.POINT,
          Logger.getLogger("RawValidPoints"),
          getSystemPropertyAsDouble("wavefront.proxy.logpoints.sample-rate"),
          "true".equalsIgnoreCase(System.getProperty("wavefront.proxy.logpoints")),
          logger::info);
  public static final Logger VALID_HISTOGRAMS_LOGGER =
      new SamplingLogger(
          ReportableEntityType.HISTOGRAM,
          Logger.getLogger("RawValidHistograms"),
          getSystemPropertyAsDouble("wavefront.proxy.logpoints.sample-rate"),
          "true".equalsIgnoreCase(System.getProperty("wavefront.proxy.logpoints")),
          logger::info);
  private static final Logger VALID_SPANS_LOGGER =
      new SamplingLogger(
          ReportableEntityType.TRACE,
          Logger.getLogger("RawValidSpans"),
          getSystemPropertyAsDouble("wavefront.proxy.logspans.sample-rate"),
          false,
          logger::info);
  private static final Logger VALID_SPAN_LOGS_LOGGER =
      new SamplingLogger(
          TRACE_SPAN_LOGS,
          Logger.getLogger("RawValidSpanLogs"),
          getSystemPropertyAsDouble("wavefront.proxy.logspans.sample-rate"),
          false,
          logger::info);
  private static final Logger VALID_EVENTS_LOGGER =
      new SamplingLogger(
          EVENT,
          Logger.getLogger("RawValidEvents"),
          getSystemPropertyAsDouble("wavefront.proxy.logevents.sample-rate"),
          false,
          logger::info);
  private static final Logger VALID_LOGS_LOGGER =
      new SamplingLogger(
          LOGS,
          Logger.getLogger("RawValidLogs"),
          getSystemPropertyAsDouble("wavefront.proxy.loglogs.sample-rate"),
          false,
          logger::info);

  protected final Map<String, Map<ReportableEntityType, ReportableEntityHandler<?>>> handlers =
      new ConcurrentHashMap<>();

  private final int blockedItemsPerBatch;
  private final ValidationConfiguration validationConfig;
  private final Logger blockedPointsLogger;
  private final Logger blockedHistogramsLogger;
  private final Logger blockedSpansLogger;
  private final Logger blockedLogsLogger;
  private final Function<Histogram, Histogram> histogramRecompressor;

  /**
   * Create new instance.
   *
   * @param blockedItemsPerBatch controls sample rate of how many blocked points are written into
   *     the main log file.
   * @param validationConfig validation configuration.
   */
  public ReportableEntityHandlerFactoryImpl(
      final int blockedItemsPerBatch,
      @Nonnull final ValidationConfiguration validationConfig,
      final Logger blockedPointsLogger,
      final Logger blockedHistogramsLogger,
      final Logger blockedSpansLogger,
      @Nullable Function<Histogram, Histogram> histogramRecompressor,
      final Logger blockedLogsLogger) {
    this.blockedItemsPerBatch = blockedItemsPerBatch;
    this.validationConfig = validationConfig;
    this.blockedPointsLogger = blockedPointsLogger;
    this.blockedHistogramsLogger = blockedHistogramsLogger;
    this.blockedSpansLogger = blockedSpansLogger;
    this.histogramRecompressor = histogramRecompressor;
    this.blockedLogsLogger = blockedLogsLogger;
  }

  private static double getSystemPropertyAsDouble(String propertyName) {
    String sampleRateProperty = propertyName == null ? null : System.getProperty(propertyName);
    return NumberUtils.isNumber(sampleRateProperty) ? Double.parseDouble(sampleRateProperty) : 1.0d;
  }

  @SuppressWarnings("unchecked")
  // TODO: review all implementations of this method
  @Override
  public <T> ReportableEntityHandler<T> getHandler(String handler, QueueInfo queue) {
    return (ReportableEntityHandler<T>)
        handlers
            .computeIfAbsent(handler + "." + queue.getName(), h -> new ConcurrentHashMap<>())
            .computeIfAbsent(
                queue.getEntityType(),
                k -> {
                  switch (queue.getEntityType()) {
                    case POINT:
                      return new ReportPointHandlerImpl(
                          handler,
                          queue,
                          blockedItemsPerBatch,
                          validationConfig,
                          blockedPointsLogger,
                          VALID_POINTS_LOGGER,
                          null);
                    case HISTOGRAM:
                      return new ReportPointHandlerImpl(
                          handler,
                          queue,
                          blockedItemsPerBatch,
                          validationConfig,
                          blockedHistogramsLogger,
                          VALID_HISTOGRAMS_LOGGER,
                          histogramRecompressor);
                    case SOURCE_TAG:
                      return new ReportSourceTagHandlerImpl(
                          handler, queue, blockedItemsPerBatch, blockedPointsLogger);
                    case TRACE:
                      return new SpanHandlerImpl(
                          handler,
                          queue,
                          blockedItemsPerBatch,
                          validationConfig,
                          blockedSpansLogger,
                          VALID_SPANS_LOGGER,
                          (tenantName) ->
                              entityPropertiesFactoryMap
                                  .get(tenantName)
                                  .getGlobalProperties()
                                  .getDropSpansDelayedMinutes(),
                          Utils.lazySupplier(
                              () ->
                                  getHandler(
                                      handler, queuesManager.initQueue(queue.getEntityType()))));
                    case TRACE_SPAN_LOGS:
                      return new SpanLogsHandlerImpl(
                          handler,
                          queue,
                          blockedItemsPerBatch,
                          blockedSpansLogger,
                          VALID_SPAN_LOGS_LOGGER);
                    case EVENT:
                      return new EventHandlerImpl(
                          handler,
                          queue,
                          blockedItemsPerBatch,
                          blockedPointsLogger,
                          VALID_EVENTS_LOGGER);
                    case LOGS:
                      return new ReportLogHandlerImpl(
                          handler,
                          queue,
                          blockedItemsPerBatch,
                          validationConfig,
                          blockedLogsLogger,
                          VALID_LOGS_LOGGER);
                    default:
                      throw new IllegalArgumentException(
                          "Unexpected entity type "
                              + queue.getEntityType().name()
                              + " for "
                              + handler);
                  }
                });
  }

  @Override
  public void shutdown(@Nonnull int handle) {
    if (handlers.containsKey(String.valueOf(handle))) {
      handlers.get(String.valueOf(handle)).values().forEach(ReportableEntityHandler::shutdown);
    }
  }
}
