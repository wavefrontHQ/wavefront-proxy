package com.wavefront.agent.core.handlers;

import static com.wavefront.agent.ProxyContext.entityPropertiesFactoryMap;
import static com.wavefront.agent.ProxyContext.queuesManager;
import static com.wavefront.data.ReportableEntityType.*;

import com.wavefront.agent.core.queues.QueueInfo;
import com.wavefront.api.agent.ValidationConfiguration;
import com.wavefront.common.Utils;
import com.wavefront.data.ReportableEntityType;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import org.apache.commons.lang.math.NumberUtils;
import org.slf4j.Logger;
import wavefront.report.Histogram;

/**
 * Caching factory for {@link ReportableEntityHandler} objects. Makes sure there's only one handler
 * for each {@link com.wavefront.agent.core.queues.QueueInfo}, which makes it possible to spin up
 * handlers on demand at runtime, as well as redirecting traffic to a different pipeline.
 */
public class ReportableEntityHandlerFactoryImpl implements ReportableEntityHandlerFactory {

  protected final Map<String, Map<ReportableEntityType, ReportableEntityHandler<?>>> handlers =
      new ConcurrentHashMap<>();

  private final ValidationConfiguration validationConfig;
  private final Logger blockedPointsLogger;
  private final Logger blockedHistogramsLogger;
  private final Logger blockedSpansLogger;
  private final Logger blockedLogsLogger;
  private final Function<Histogram, Histogram> histogramRecompressor;

  /**
   * Create new instance.
   *
   * @param validationConfig validation configuration.
   */
  public ReportableEntityHandlerFactoryImpl(
      @Nonnull final ValidationConfiguration validationConfig,
      final Logger blockedPointsLogger,
      final Logger blockedHistogramsLogger,
      final Logger blockedSpansLogger,
      @Nullable Function<Histogram, Histogram> histogramRecompressor,
      final Logger blockedLogsLogger) {
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
                          handler, queue, validationConfig, blockedPointsLogger, null);
                    case HISTOGRAM:
                      return new ReportPointHandlerImpl(
                          handler,
                          queue,
                          validationConfig,
                          blockedHistogramsLogger,
                          histogramRecompressor);
                    case SOURCE_TAG:
                      return new ReportSourceTagHandlerImpl(handler, queue, blockedPointsLogger);
                    case TRACE:
                      return new SpanHandlerImpl(
                          handler,
                          queue,
                          validationConfig,
                          blockedSpansLogger,
                          (tenantName) ->
                              entityPropertiesFactoryMap
                                  .get(tenantName)
                                  .getGlobalProperties()
                                  .getDropSpansDelayedMinutes(),
                          Utils.lazySupplier(
                              () -> getHandler(handler, queuesManager.initQueue(TRACE_SPAN_LOGS))));
                    case TRACE_SPAN_LOGS:
                      return new SpanLogsHandlerImpl(handler, queue, blockedSpansLogger);
                    case EVENT:
                      return new EventHandlerImpl(handler, queue, blockedPointsLogger);
                    case LOGS:
                      return new ReportLogHandlerImpl(
                          handler, queue, validationConfig, blockedLogsLogger);
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
  public void shutdown(int handle) {
    if (handlers.containsKey(String.valueOf(handle))) {
      handlers.get(String.valueOf(handle)).values().forEach(ReportableEntityHandler::shutdown);
    }
  }
}
