package com.wavefront.agent.data;

import com.google.common.collect.ImmutableMap;
import com.wavefront.data.ReportableEntityType;

import java.util.Map;

/**
 * TODO (VV): javadoc
 *
 * @author vasily@wavefront.com
 */
public class EntityWrapper {
  private final Map<ReportableEntityType, EntityProperties> wrappers;

  public EntityWrapper(ProxyRuntimeProperties container) {
    EntityProperties pointProperties = new PointsProperties(container);
    wrappers = ImmutableMap.<ReportableEntityType, EntityProperties>builder().
        put(ReportableEntityType.POINT, pointProperties).
        put(ReportableEntityType.DELTA_COUNTER, pointProperties).
        put(ReportableEntityType.HISTOGRAM, new HistogramsProperties(container)).
        put(ReportableEntityType.SOURCE_TAG, new SourceTagsProperties(container)).
        put(ReportableEntityType.TRACE, new SpansProperties(container)).
        put(ReportableEntityType.TRACE_SPAN_LOGS, new SpanLogsProperties(container)).
        put(ReportableEntityType.EVENT, new EventsProperties(container)).build();
  }

  /**
   * TODO (VV): javadoc
   *
   * @param entityType
   * @return
   */
  public EntityProperties get(ReportableEntityType entityType) {
    return wrappers.get(entityType);
  }

  public interface EntityProperties {
    int getItemsPerBatchOriginal();

    double getRetryBackoffBaseSecondsOriginal();

    boolean isSplitPushWhenRateLimited();

    void setSplitPushWhenRateLimited(boolean splitPushWhenRateLimited);

    double getRetryBackoffBaseSeconds();

    void setRetryBackoffBaseSeconds(double retryBackoffBaseSeconds);

    int getPushRateLimitMaxBurstSeconds();

    void setPushRateLimitMaxBurstSeconds(int pushRateLimitMaxBurstSeconds);

    int getPushFlushInterval();

    void setPushFlushInterval(int pushFlushInterval);

    int getItemsPerBatch();

    void setItemsPerBatch(int itemsPerBatch);

    int getMinBatchSplitSize();

    void setMinBatchSplitSize(int minBatchSplitSize);

    int getMemoryBufferLimit();

    void setMemoryBufferLimit(int memoryBufferLimit);

    TaskQueueLevel getTaskQueueLevel();
  }

  /**
   * Wrapper for common methods that are not entity-specific
   */
  private static abstract class AbstractEntityProperties implements EntityProperties {
    protected final ProxyRuntimeProperties wrapped;

    public AbstractEntityProperties(ProxyRuntimeProperties wrapped) {
      this.wrapped = wrapped;
    }

    @Override
    public double getRetryBackoffBaseSecondsOriginal() {
      return wrapped.getRetryBackoffBaseSecondsOriginal();
    }

    @Override
    public boolean isSplitPushWhenRateLimited() {
      return wrapped.isSplitPushWhenRateLimited();
    }

    @Override
    public void setSplitPushWhenRateLimited(boolean splitPushWhenRateLimited) {
      wrapped.setSplitPushWhenRateLimited(splitPushWhenRateLimited);
    }

    @Override
    public double getRetryBackoffBaseSeconds() {
      return wrapped.getRetryBackoffBaseSeconds();
    }

    @Override
    public void setRetryBackoffBaseSeconds(double retryBackoffBaseSeconds) {
      wrapped.setRetryBackoffBaseSeconds(retryBackoffBaseSeconds);
    }

    @Override
    public int getPushRateLimitMaxBurstSeconds() {
      return wrapped.getPushRateLimitMaxBurstSeconds();
    }

    @Override
    public void setPushRateLimitMaxBurstSeconds(int pushRateLimitMaxBurstSeconds) {
      wrapped.setPushRateLimitMaxBurstSeconds(pushRateLimitMaxBurstSeconds);
    }

    @Override
    public int getPushFlushInterval() {
      return wrapped.getPushFlushInterval();
    }

    @Override
    public void setPushFlushInterval(int pushFlushInterval) {
      wrapped.setPushFlushInterval(pushFlushInterval);
    }

    @Override
    public int getMinBatchSplitSize() {
      return wrapped.getMinBatchSplitSize();
    }

    @Override
    public void setMinBatchSplitSize(int minBatchSplitSize) {
      wrapped.setMinBatchSplitSize(minBatchSplitSize);
    }

    @Override
    public int getMemoryBufferLimit() {
      return wrapped.getMemoryBufferLimit();
    }

    @Override
    public void setMemoryBufferLimit(int memoryBufferLimit) {
      wrapped.setMemoryBufferLimit(memoryBufferLimit);
    }

    @Override
    public TaskQueueLevel getTaskQueueLevel() {
      return wrapped.getTaskQueueLevel();
    }
  }

  /**
   * Runtime properties wrapper for points
   */
  private static final class PointsProperties extends AbstractEntityProperties {
    public PointsProperties(ProxyRuntimeProperties wrapped) {
      super(wrapped);
    }

    @Override
    public int getItemsPerBatchOriginal() {
      return wrapped.getItemsPerBatchOriginal();
    }

    @Override
    public int getItemsPerBatch() {
      return wrapped.getItemsPerBatch();
    }

    @Override
    public void setItemsPerBatch(int itemsPerBatch) {
      wrapped.setItemsPerBatch(itemsPerBatch);
    }
  }

  /**
   * Runtime properties wrapper for histograms
   */
  private static final class HistogramsProperties extends AbstractEntityProperties {
    public HistogramsProperties(ProxyRuntimeProperties wrapped) {
      super(wrapped);
    }

    @Override
    public int getItemsPerBatchOriginal() {
      return wrapped.getItemsPerBatchHistogramsOriginal();
    }

    @Override
    public int getItemsPerBatch() {
      return wrapped.getItemsPerBatchHistograms();
    }

    @Override
    public void setItemsPerBatch(int itemsPerBatch) {
      wrapped.setItemsPerBatchHistograms(itemsPerBatch);
    }
  }

  /**
   * Runtime properties wrapper for source tags
   */
  private static final class SourceTagsProperties extends AbstractEntityProperties {
    public SourceTagsProperties(ProxyRuntimeProperties wrapped) {
      super(wrapped);
    }

    @Override
    public int getItemsPerBatchOriginal() {
      return wrapped.getItemsPerBatchSourceTagsOriginal();
    }

    @Override
    public int getItemsPerBatch() {
      return wrapped.getItemsPerBatchSourceTags();
    }

    @Override
    public void setItemsPerBatch(int itemsPerBatch) {
      wrapped.setItemsPerBatchSourceTags(itemsPerBatch);
    }

    @Override
    public int getMemoryBufferLimit() {
      return wrapped.getMemoryBufferLimitSourceTags();
    }

    @Override
    public void setMemoryBufferLimit(int memoryBufferLimit) {
      wrapped.setMemoryBufferLimitSourceTags(memoryBufferLimit);
    }
  }

  /**
   * Runtime properties wrapper for spans
   */
  private static final class SpansProperties extends AbstractEntityProperties {
    public SpansProperties(ProxyRuntimeProperties wrapped) {
      super(wrapped);
    }

    @Override
    public int getItemsPerBatchOriginal() {
      return wrapped.getItemsPerBatchSpansOriginal();
    }

    @Override
    public int getItemsPerBatch() {
      return wrapped.getItemsPerBatchSpans();
    }

    @Override
    public void setItemsPerBatch(int itemsPerBatch) {
      wrapped.setItemsPerBatchSpans(itemsPerBatch);
    }
  }

  /**
   * Runtime properties wrapper for span logs
   */
  private static final class SpanLogsProperties extends AbstractEntityProperties {
    public SpanLogsProperties(ProxyRuntimeProperties wrapped) {
      super(wrapped);
    }

    @Override
    public int getItemsPerBatchOriginal() {
      return wrapped.getItemsPerBatchSpanLogsOriginal();
    }

    @Override
    public int getItemsPerBatch() {
      return wrapped.getItemsPerBatchSpanLogs();
    }

    @Override
    public void setItemsPerBatch(int itemsPerBatch) {
      wrapped.setItemsPerBatchSpanLogs(itemsPerBatch);
    }
  }

  /**
   * Runtime properties wrapper for events
   */
  private static final class EventsProperties extends AbstractEntityProperties {
    public EventsProperties(ProxyRuntimeProperties wrapped) {
      super(wrapped);
    }

    @Override
    public int getItemsPerBatchOriginal() {
      return wrapped.getItemsPerBatchEventsOriginal();
    }

    @Override
    public int getItemsPerBatch() {
      return wrapped.getItemsPerBatchEvents();
    }

    @Override
    public void setItemsPerBatch(int itemsPerBatch) {
      wrapped.setItemsPerBatchEvents(itemsPerBatch);
    }

    @Override
    public int getMemoryBufferLimit() {
      return wrapped.getMemoryBufferLimitEvents();
    }

    @Override
    public void setMemoryBufferLimit(int memoryBufferLimit) {
      wrapped.setMemoryBufferLimitEvents(memoryBufferLimit);
    }
  }
}
