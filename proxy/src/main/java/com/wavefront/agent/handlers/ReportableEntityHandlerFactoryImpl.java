package com.wavefront.agent.handlers;

import static com.wavefront.data.ReportableEntityType.TRACE_SPAN_LOGS;

import com.wavefront.agent.data.EntityPropertiesFactory;
import com.wavefront.agent.sampler.MetricBloomFilterSampler;
import com.wavefront.api.agent.ValidationConfiguration;
import com.wavefront.common.Utils;
import com.wavefront.common.logger.SamplingLogger;
import com.wavefront.data.ReportableEntityType;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.logging.Logger;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import org.apache.commons.lang.math.NumberUtils;
import wavefront.report.Histogram;

/**
 * Caching factory for {@link ReportableEntityHandler} objects. Makes sure there's only one handler
 * for each {@link HandlerKey}, which makes it possible to spin up handlers on demand at runtime, as
 * well as redirecting traffic to a different pipeline.
 *
 * @author vasily@wavefront.com
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
          ReportableEntityType.TRACE_SPAN_LOGS,
          Logger.getLogger("RawValidSpanLogs"),
          getSystemPropertyAsDouble("wavefront.proxy.logspans.sample-rate"),
          false,
          logger::info);
  private static final Logger VALID_EVENTS_LOGGER =
      new SamplingLogger(
          ReportableEntityType.EVENT,
          Logger.getLogger("RawValidEvents"),
          getSystemPropertyAsDouble("wavefront.proxy.logevents.sample-rate"),
          false,
          logger::info);
  private static final Logger VALID_LOGS_LOGGER =
      new SamplingLogger(
          ReportableEntityType.LOGS,
          Logger.getLogger("RawValidLogs"),
          getSystemPropertyAsDouble("wavefront.proxy.loglogs.sample-rate"),
          false,
          logger::info);

  protected final Map<String, Map<ReportableEntityType, ReportableEntityHandler<?, ?>>> handlers =
      new ConcurrentHashMap<>();

  private final SenderTaskFactory senderTaskFactory;
  private final int blockedItemsPerBatch;
  private final ValidationConfiguration validationConfig;
  private final Logger blockedPointsLogger;
  private final Logger blockedHistogramsLogger;
  private final Logger blockedSpansLogger;
  private final Logger blockedLogsLogger;
  private final Function<Histogram, Histogram> histogramRecompressor;
  private final MetricBloomFilterSampler metricBloomFilterSampler;
  private final Map<String, EntityPropertiesFactory> entityPropsFactoryMap;
  /**
   * Human-readable alias for the central/primary tenant. Passed to each handler's
   * {@code resolveTenantName()} so that forward-rule targets written as the alias string
   * resolve to {@link com.wavefront.agent.api.APIContainer#CENTRAL_TENANT_NAME}.
   */
  @Nullable
  private final String defaultTenant;

  private ReportableEntityHandlerFactoryImpl(Builder builder) {
    this.senderTaskFactory = Objects.requireNonNull(builder.senderTaskFactory, "senderTaskFactory");
    this.blockedItemsPerBatch = builder.blockedItemsPerBatch;
    this.validationConfig = Objects.requireNonNull(builder.validationConfig, "validationConfig");
    this.blockedPointsLogger = builder.blockedPointsLogger;
    this.blockedHistogramsLogger = builder.blockedHistogramsLogger;
    this.blockedSpansLogger = builder.blockedSpansLogger;
    this.blockedLogsLogger = builder.blockedLogsLogger;
    this.histogramRecompressor = builder.histogramRecompressor;
    this.metricBloomFilterSampler = builder.metricBloomFilterSampler;
    this.entityPropsFactoryMap = Objects.requireNonNull(builder.entityPropsFactoryMap, "entityPropsFactoryMap");
    this.defaultTenant = builder.defaultTenant;
  }

  /** Returns a new {@link Builder} for this factory. */
  public static Builder builder() {
    return new Builder();
  }

  /**
   * Builder for {@link ReportableEntityHandlerFactoryImpl}.
   *
   * <p>Required fields: {@code senderTaskFactory}, {@code validationConfig},
   * {@code entityPropsFactoryMap}. All others are optional and default to {@code null}.
   */
  public static final class Builder {
    private SenderTaskFactory senderTaskFactory;
    private int blockedItemsPerBatch;
    private ValidationConfiguration validationConfig;
    private Logger blockedPointsLogger;
    private Logger blockedHistogramsLogger;
    private Logger blockedSpansLogger;
    private Logger blockedLogsLogger;
    private Function<Histogram, Histogram> histogramRecompressor;
    private MetricBloomFilterSampler metricBloomFilterSampler;
    private Map<String, EntityPropertiesFactory> entityPropsFactoryMap;
    private String defaultTenant;

    private Builder() {}

    public Builder senderTaskFactory(@Nonnull SenderTaskFactory value) { this.senderTaskFactory = value; return this; }
    public Builder blockedItemsPerBatch(int value) { this.blockedItemsPerBatch = value; return this; }
    public Builder validationConfig(@Nonnull ValidationConfiguration value) { this.validationConfig = value; return this; }
    public Builder blockedPointsLogger(@Nullable Logger value) { this.blockedPointsLogger = value; return this; }
    public Builder blockedHistogramsLogger(@Nullable Logger value) { this.blockedHistogramsLogger = value; return this; }
    public Builder blockedSpansLogger(@Nullable Logger value) { this.blockedSpansLogger = value; return this; }
    public Builder blockedLogsLogger(@Nullable Logger value) { this.blockedLogsLogger = value; return this; }
    public Builder histogramRecompressor(@Nullable Function<Histogram, Histogram> value) { this.histogramRecompressor = value; return this; }
    public Builder metricBloomFilterSampler(@Nullable MetricBloomFilterSampler value) { this.metricBloomFilterSampler = value; return this; }
    public Builder entityPropsFactoryMap(@Nonnull Map<String, EntityPropertiesFactory> value) { this.entityPropsFactoryMap = value; return this; }
    public Builder defaultTenant(@Nullable String value) { this.defaultTenant = value; return this; }

    public ReportableEntityHandlerFactoryImpl build() {
      return new ReportableEntityHandlerFactoryImpl(this);
    }
  }

  @SuppressWarnings("unchecked")
  @Override
  public <T, U> ReportableEntityHandler<T, U> getHandler(HandlerKey handlerKey) {
    BiConsumer<String, Long> receivedRateSink =
        (tenantName, rate) ->
            entityPropsFactoryMap
                .get(tenantName)
                .get(handlerKey.getEntityType())
                .reportReceivedRate(handlerKey.getHandle(), rate);
    return (ReportableEntityHandler<T, U>)
        handlers
            .computeIfAbsent(handlerKey.getHandle(), h -> new ConcurrentHashMap<>())
            .computeIfAbsent(
                handlerKey.getEntityType(),
                k -> {
                  switch (handlerKey.getEntityType()) {
                    case POINT:
                      return new ReportPointHandlerImpl(
                          handlerKey,
                          blockedItemsPerBatch,
                          senderTaskFactory.createSenderTasks(handlerKey),
                          validationConfig,
                          true,
                          receivedRateSink,
                          blockedPointsLogger,
                          VALID_POINTS_LOGGER,
                          null,
                          metricBloomFilterSampler,
                          defaultTenant);
                    case HISTOGRAM:
                      return new ReportPointHandlerImpl(
                          handlerKey,
                          blockedItemsPerBatch,
                          senderTaskFactory.createSenderTasks(handlerKey),
                          validationConfig,
                          true,
                          receivedRateSink,
                          blockedHistogramsLogger,
                          VALID_HISTOGRAMS_LOGGER,
                          histogramRecompressor,
                          null,
                          defaultTenant);
                    case SOURCE_TAG:
                      return new ReportSourceTagHandlerImpl(
                          handlerKey,
                          blockedItemsPerBatch,
                          senderTaskFactory.createSenderTasks(handlerKey),
                          receivedRateSink,
                          blockedPointsLogger);
                    case TRACE:
                      return new SpanHandlerImpl(
                          handlerKey,
                          blockedItemsPerBatch,
                          senderTaskFactory.createSenderTasks(handlerKey),
                          validationConfig,
                          receivedRateSink,
                          blockedSpansLogger,
                          VALID_SPANS_LOGGER,
                          (tenantName) ->
                              entityPropsFactoryMap
                                  .get(tenantName)
                                  .getGlobalProperties()
                                  .getDropSpansDelayedMinutes(),
                          Utils.lazySupplier(
                              () ->
                                  getHandler(
                                      HandlerKey.of(TRACE_SPAN_LOGS, handlerKey.getHandle()))),
                          defaultTenant);
                    case TRACE_SPAN_LOGS:
                      return new SpanLogsHandlerImpl(
                          handlerKey,
                          blockedItemsPerBatch,
                          senderTaskFactory.createSenderTasks(handlerKey),
                          receivedRateSink,
                          blockedSpansLogger,
                          VALID_SPAN_LOGS_LOGGER);
                    case EVENT:
                      return new EventHandlerImpl(
                          handlerKey,
                          blockedItemsPerBatch,
                          senderTaskFactory.createSenderTasks(handlerKey),
                          receivedRateSink,
                          blockedPointsLogger,
                          VALID_EVENTS_LOGGER);
                    case LOGS:
                      return new ReportLogHandlerImpl(
                          handlerKey,
                          blockedItemsPerBatch,
                          senderTaskFactory.createSenderTasks(handlerKey),
                          validationConfig,
                          true,
                          receivedRateSink,
                          blockedLogsLogger,
                          VALID_LOGS_LOGGER,
                          defaultTenant);
                    default:
                      throw new IllegalArgumentException(
                          "Unexpected entity type "
                              + handlerKey.getEntityType().name()
                              + " for "
                              + handlerKey.getHandle());
                  }
                });
  }

  @Override
  public void shutdown(@Nonnull String handle) {
    if (handlers.containsKey(handle)) {
      handlers.get(handle).values().forEach(ReportableEntityHandler::shutdown);
    }
  }

  private static double getSystemPropertyAsDouble(String propertyName) {
    String sampleRateProperty = propertyName == null ? null : System.getProperty(propertyName);
    return NumberUtils.isNumber(sampleRateProperty) ? Double.parseDouble(sampleRateProperty) : 1.0d;
  }
}
