package com.wavefront.agent.data;

import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;
import com.google.common.collect.ImmutableMap;
import com.wavefront.agent.ProxyConfig;
import com.wavefront.data.ReportableEntityType;

import javax.annotation.Nullable;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static com.wavefront.agent.config.ReportableConfig.reportSettingAsGauge;
import static org.apache.commons.lang3.ObjectUtils.firstNonNull;

/**
 * Generates entity-specific wrappers for dynamic proxy settings.
 *
 * @author vasily@wavefront.com
 */
public class EntityPropertiesFactoryImpl implements EntityPropertiesFactory {

  private final Map<ReportableEntityType, EntityProperties> wrappers;
  private final GlobalProperties global;

  /** @param proxyConfig proxy settings container */
  public EntityPropertiesFactoryImpl(ProxyConfig proxyConfig) {
    global = new GlobalPropertiesImpl(proxyConfig);
    EntityProperties pointProperties = new PointsProperties(proxyConfig);
    wrappers =
        ImmutableMap.<ReportableEntityType, EntityProperties>builder()
            .put(ReportableEntityType.POINT, pointProperties)
            .put(ReportableEntityType.DELTA_COUNTER, pointProperties)
            .put(ReportableEntityType.HISTOGRAM, new HistogramsProperties(proxyConfig))
            .put(ReportableEntityType.SOURCE_TAG, new SourceTagsProperties(proxyConfig))
            .put(ReportableEntityType.TRACE, new SpansProperties(proxyConfig))
            .put(ReportableEntityType.TRACE_SPAN_LOGS, new SpanLogsProperties(proxyConfig))
            .put(ReportableEntityType.EVENT, new EventsProperties(proxyConfig))
            .put(ReportableEntityType.LOGS, new LogsProperties(proxyConfig))
            .build();
  }

  @Override
  public EntityProperties get(ReportableEntityType entityType) {
    return wrappers.get(entityType);
  }

  @Override
  public GlobalProperties getGlobalProperties() {
    return global;
  }

  /** Common base for all wrappers (to avoid code duplication) */
  private abstract static class AbstractEntityProperties implements EntityProperties {
    protected final ProxyConfig wrapped;
    private final EntityRateLimiter rateLimiter;
    private final LoadingCache<String, AtomicInteger> backlogSizeCache =
        Caffeine.newBuilder()
            .expireAfterAccess(10, TimeUnit.SECONDS)
            .build(x -> new AtomicInteger());
    private final LoadingCache<String, AtomicLong> receivedRateCache =
        Caffeine.newBuilder().expireAfterAccess(10, TimeUnit.SECONDS).build(x -> new AtomicLong());
    private Integer dataPerBatch = null;

    public AbstractEntityProperties(ProxyConfig wrapped) {
      this.wrapped = wrapped;
//      this.rateLimiter = new RecyclableRateLimiterWithMetrics(
//                  RecyclableRateLimiterImpl.create(getRateLimit(), getRateLimitMaxBurstSeconds()),
//                  getRateLimiterName());
      rateLimiter = new EntityRateLimiter(getRateLimit(), getRateLimitMaxBurstSeconds(), getRateLimiterName());

      reportSettingAsGauge(this::getPushFlushInterval, "dynamic.pushFlushInterval");
    }

    @Override
    public int getDataPerBatch() {
      return firstNonNull(dataPerBatch, getDataPerBatchOriginal());
    }

    @Override
    public void setDataPerBatch(@Nullable Integer dataPerBatch) {
      this.dataPerBatch = dataPerBatch;
    }

    @Override
    public int getRateLimitMaxBurstSeconds() {
      return wrapped.getPushRateLimitMaxBurstSeconds();
    }

    @Override
    public EntityRateLimiter getRateLimiter() {
      return rateLimiter;
    }

    protected abstract String getRateLimiterName();

    @Override
    public int getFlushThreads() {
      return wrapped.getFlushThreads();
    }

    @Override
    public int getPushFlushInterval() {
      return wrapped.getPushFlushInterval();
    }

    public int getMemoryBufferLimit() {
      return wrapped.getPushMemoryBufferLimit();
    }
  }

  /** Base class for entity types that do not require separate subscriptions. */
  private abstract static class CoreEntityProperties extends AbstractEntityProperties {
    public CoreEntityProperties(ProxyConfig wrapped) {
      super(wrapped);
    }

    @Override
    public boolean isFeatureDisabled() {
      return false;
    }

    @Override
    public void setFeatureDisabled(boolean featureDisabledFlag) {
      throw new UnsupportedOperationException("Can't disable this feature");
    }
  }

  /**
   * Base class for entity types that do require a separate subscription and can be controlled
   * remotely.
   */
  private abstract static class SubscriptionBasedEntityProperties extends AbstractEntityProperties {
    private boolean featureDisabled = false;

    public SubscriptionBasedEntityProperties(ProxyConfig wrapped) {
      super(wrapped);
    }

    @Override
    public boolean isFeatureDisabled() {
      return featureDisabled;
    }

    @Override
    public void setFeatureDisabled(boolean featureDisabledFlag) {
      this.featureDisabled = featureDisabledFlag;
    }
  }

  /** Runtime properties wrapper for points */
  private static final class PointsProperties extends CoreEntityProperties {
    public PointsProperties(ProxyConfig wrapped) {
      super(wrapped);
      reportSettingAsGauge(this::getDataPerBatch, "dynamic.pushFlushMaxPoints");
      reportSettingAsGauge(this::getMemoryBufferLimit, "dynamic.pushMemoryBufferLimit");
    }

    @Override
    protected String getRateLimiterName() {
      return "limiter";
    }

    @Override
    public int getDataPerBatchOriginal() {
      return wrapped.getPushFlushMaxPoints();
    }

    @Override
    public double getRateLimit() {
      return wrapped.getPushRateLimit();
    }
  }

  /** Runtime properties wrapper for histograms */
  private static final class HistogramsProperties extends SubscriptionBasedEntityProperties {
    public HistogramsProperties(ProxyConfig wrapped) {
      super(wrapped);
      reportSettingAsGauge(this::getDataPerBatch, "dynamic.pushFlushMaxHistograms");
      reportSettingAsGauge(this::getMemoryBufferLimit, "dynamic.pushMemoryBufferLimit");
    }

    @Override
    protected String getRateLimiterName() {
      return "limiter.histograms";
    }

    @Override
    public int getDataPerBatchOriginal() {
      return wrapped.getPushFlushMaxHistograms();
    }

    @Override
    public double getRateLimit() {
      return wrapped.getPushRateLimitHistograms();
    }
  }

  /** Runtime properties wrapper for source tags */
  private static final class SourceTagsProperties extends CoreEntityProperties {
    public SourceTagsProperties(ProxyConfig wrapped) {
      super(wrapped);
      reportSettingAsGauge(this::getDataPerBatch, "dynamic.pushFlushMaxSourceTags");
      reportSettingAsGauge(this::getMemoryBufferLimit, "dynamic.pushMemoryBufferLimitSourceTags");
    }

    @Override
    protected String getRateLimiterName() {
      return "limiter.sourceTags";
    }

    @Override
    public int getDataPerBatchOriginal() {
      return wrapped.getPushFlushMaxSourceTags();
    }

    @Override
    public double getRateLimit() {
      return wrapped.getPushRateLimitSourceTags();
    }

    @Override
    public int getMemoryBufferLimit() {
      return 16 * wrapped.getPushFlushMaxSourceTags();
    }

    @Override
    public int getFlushThreads() {
      return wrapped.getFlushThreadsSourceTags();
    }
  }

  /** Runtime properties wrapper for spans */
  private static final class SpansProperties extends SubscriptionBasedEntityProperties {
    public SpansProperties(ProxyConfig wrapped) {
      super(wrapped);
      reportSettingAsGauge(this::getDataPerBatch, "dynamic.pushFlushMaxSpans");
      reportSettingAsGauge(this::getMemoryBufferLimit, "dynamic.pushMemoryBufferLimit");
    }

    @Override
    protected String getRateLimiterName() {
      return "limiter.spans";
    }

    @Override
    public int getDataPerBatchOriginal() {
      return wrapped.getPushFlushMaxSpans();
    }

    @Override
    public double getRateLimit() {
      return wrapped.getPushRateLimitSpans();
    }
  }

  /** Runtime properties wrapper for span logs */
  private static final class SpanLogsProperties extends SubscriptionBasedEntityProperties {
    public SpanLogsProperties(ProxyConfig wrapped) {
      super(wrapped);
      reportSettingAsGauge(this::getDataPerBatch, "dynamic.pushFlushMaxSpanLogs");
      reportSettingAsGauge(this::getMemoryBufferLimit, "dynamic.pushMemoryBufferLimit");
    }

    @Override
    protected String getRateLimiterName() {
      return "limiter.spanLogs";
    }

    @Override
    public int getDataPerBatchOriginal() {
      return wrapped.getPushFlushMaxSpanLogs();
    }

    @Override
    public double getRateLimit() {
      return wrapped.getPushRateLimitSpanLogs();
    }
  }

  /** Runtime properties wrapper for events */
  private static final class EventsProperties extends CoreEntityProperties {
    public EventsProperties(ProxyConfig wrapped) {
      super(wrapped);
      reportSettingAsGauge(this::getDataPerBatch, "dynamic.pushFlushMaxEvents");
      reportSettingAsGauge(this::getMemoryBufferLimit, "dynamic.pushMemoryBufferLimitEvents");
    }

    @Override
    protected String getRateLimiterName() {
      return "limiter.events";
    }

    @Override
    public int getDataPerBatchOriginal() {
      return wrapped.getPushFlushMaxEvents();
    }

    @Override
    public double getRateLimit() {
      return wrapped.getPushRateLimitEvents();
    }

    @Override
    public int getMemoryBufferLimit() {
      return 16 * wrapped.getPushFlushMaxEvents();
    }

    @Override
    public int getFlushThreads() {
      return wrapped.getFlushThreadsEvents();
    }
  }

  /** Runtime properties wrapper for logs */
  private static final class LogsProperties extends SubscriptionBasedEntityProperties {
    public LogsProperties(ProxyConfig wrapped) {
      super(wrapped);
      reportSettingAsGauge(this::getDataPerBatch, "dynamic.pushFlushMaxLogs");
      reportSettingAsGauge(this::getMemoryBufferLimit, "dynamic.pushMemoryBufferLimitLogs");
    }

    @Override
    protected String getRateLimiterName() {
      return "limiter.logs";
    }

    @Override
    public int getDataPerBatchOriginal() {
      return wrapped.getPushFlushMaxLogs();
    }

    @Override
    public int getMemoryBufferLimit() {
      return wrapped.getPushMemoryBufferLimitLogs();
    }

    @Override
    public double getRateLimit() {
      return wrapped.getPushRateLimitLogs();
    }

    @Override
    public int getFlushThreads() {
      return wrapped.getFlushThreadsLogs();
    }

    @Override
    public int getPushFlushInterval() {
      return wrapped.getPushFlushIntervalLogs();
    }
  }
}
