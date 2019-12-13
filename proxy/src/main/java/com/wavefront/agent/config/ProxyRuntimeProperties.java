package com.wavefront.agent.config;

import com.google.common.base.Preconditions;
import com.wavefront.data.ReportableEntityType;

import java.util.function.Supplier;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Container object for mutable settings that can change at runtime
 *
 * @author vasily@wavefront.com
 */
public class ProxyRuntimeProperties {
  public static final boolean DEFAULT_SPLIT_PUSH_WHEN_RATE_LIMITED = false;
  public static final double DEFAULT_RETRY_BACKOFF_BASE_SECONDS = 2.0d;
  public static final int DEFAULT_MAX_BURST_SECONDS = 10;
  public static final int DEFAULT_FLUSH_INTERVAL = 1000;
  public static final int DEFAULT_BATCH_SIZE = 40000;
  public static final int DEFAULT_BATCH_SIZE_HISTOGRAMS = 10000;
  public static final int DEFAULT_BATCH_SIZE_SOURCE_TAGS = 50;
  public static final int DEFAULT_BATCH_SIZE_SPANS = 5000;
  public static final int DEFAULT_BATCH_SIZE_SPAN_LOGS = 1000;
  public static final int DEFAULT_BATCH_SIZE_EVENTS = 50;
  public static final int DEFAULT_MIN_SPLIT_BATCH_SIZE = 100;

  public static final ProxyRuntimeProperties DEFAULT_SETTINGS = ProxyRuntimeProperties.newBuilder().
      setSplitPushWhenRateLimited(DEFAULT_SPLIT_PUSH_WHEN_RATE_LIMITED).
      setRetryBackoffBaseSeconds(DEFAULT_RETRY_BACKOFF_BASE_SECONDS).
      setPushRateLimitMaxBurstSeconds(DEFAULT_MAX_BURST_SECONDS).
      setPushFlushInterval(DEFAULT_FLUSH_INTERVAL).
      setItemsPerBatch(DEFAULT_BATCH_SIZE).
      setItemsPerBatchHistograms(DEFAULT_BATCH_SIZE_HISTOGRAMS).
      setItemsPerBatchSourceTags(DEFAULT_BATCH_SIZE_SOURCE_TAGS).
      setItemsPerBatchSpans(DEFAULT_BATCH_SIZE_SPANS).
      setItemsPerBatchSpanLogs(DEFAULT_BATCH_SIZE_SPAN_LOGS).
      setItemsPerBatchEvents(DEFAULT_BATCH_SIZE_EVENTS).
      setMinBatchSplitSize(DEFAULT_MIN_SPLIT_BATCH_SIZE).
      setMemoryBufferLimit(16 * DEFAULT_BATCH_SIZE).
      setMemoryBufferLimitSourceTags(16 * DEFAULT_BATCH_SIZE_SOURCE_TAGS).
      setMemoryBufferLimitEvents(16 * DEFAULT_BATCH_SIZE_EVENTS).
      build();


  /**
   * Initial values provided with the builder
   */
  private int itemsPerBatchOriginal;
  private int itemsPerBatchHistogramsOriginal;
  private int itemsPerBatchSourceTagsOriginal;
  private int itemsPerBatchSpansOriginal;
  private int itemsPerBatchSpanLogsOriginal;
  private int itemsPerBatchEventsOriginal;
  private double retryBackoffBaseSecondsOriginal;

  private volatile boolean splitPushWhenRateLimited;
  private volatile double retryBackoffBaseSeconds;
  private volatile int pushRateLimitMaxBurstSeconds;
  private volatile int pushFlushInterval;
  private volatile int itemsPerBatch;
  private volatile int itemsPerBatchHistograms;
  private volatile int itemsPerBatchSourceTags;
  private volatile int itemsPerBatchSpans;
  private volatile int itemsPerBatchSpanLogs;
  private volatile int itemsPerBatchEvents;
  private volatile int minBatchSplitSize;
  private volatile int memoryBufferLimit;
  private volatile int memoryBufferLimitSourceTags;
  private volatile int memoryBufferLimitEvents;

  private ProxyRuntimeProperties() {
  }

  public int getItemsPerBatchOriginal() {
    return itemsPerBatchOriginal;
  }

  public int getItemsPerBatchHistogramsOriginal() {
    return itemsPerBatchHistogramsOriginal;
  }

  public int getItemsPerBatchSourceTagsOriginal() {
    return itemsPerBatchSourceTagsOriginal;
  }

  public int getItemsPerBatchSpansOriginal() {
    return itemsPerBatchSpansOriginal;
  }

  public int getItemsPerBatchSpanLogsOriginal() {
    return itemsPerBatchSpanLogsOriginal;
  }

  public int getItemsPerBatchEventsOriginal() {
    return itemsPerBatchEventsOriginal;
  }

  public double getRetryBackoffBaseSecondsOriginal() {
    return retryBackoffBaseSecondsOriginal;
  }

  public boolean isSplitPushWhenRateLimited() {
    return splitPushWhenRateLimited;
  }

  public void setSplitPushWhenRateLimited(boolean splitPushWhenRateLimited) {
    this.splitPushWhenRateLimited = splitPushWhenRateLimited;
  }

  public double getRetryBackoffBaseSeconds() {
    return retryBackoffBaseSeconds;
  }

  public void setRetryBackoffBaseSeconds(double retryBackoffBaseSeconds) {
    this.retryBackoffBaseSeconds = retryBackoffBaseSeconds;
  }

  public int getPushRateLimitMaxBurstSeconds() {
    return pushRateLimitMaxBurstSeconds;
  }

  public void setPushRateLimitMaxBurstSeconds(int pushRateLimitMaxBurstSeconds) {
    this.pushRateLimitMaxBurstSeconds = pushRateLimitMaxBurstSeconds;
  }

  public int getPushFlushInterval() {
    return pushFlushInterval;
  }

  public void setPushFlushInterval(int pushFlushInterval) {
    this.pushFlushInterval = pushFlushInterval;
  }

  public int getItemsPerBatch() {
    return itemsPerBatch;
  }

  public void setItemsPerBatch(int itemsPerBatch) {
    this.itemsPerBatch = itemsPerBatch;
  }

  public int getItemsPerBatchHistograms() {
    return itemsPerBatchHistograms;
  }

  public void setItemsPerBatchHistograms(int itemsPerBatchHistograms) {
    this.itemsPerBatchHistograms = itemsPerBatchHistograms;
  }

  public int getItemsPerBatchSourceTags() {
    return itemsPerBatchSourceTags;
  }

  public void setItemsPerBatchSourceTags(int itemsPerBatchSourceTags) {
    this.itemsPerBatchSourceTags = itemsPerBatchSourceTags;
  }

  public int getItemsPerBatchSpans() {
    return itemsPerBatchSpans;
  }

  public void setItemsPerBatchSpans(int itemsPerBatchSpans) {
    this.itemsPerBatchSpans = itemsPerBatchSpans;
  }

  public int getItemsPerBatchSpanLogs() {
    return itemsPerBatchSpanLogs;
  }

  public void setItemsPerBatchSpanLogs(int itemsPerBatchSpanLogs) {
    this.itemsPerBatchSpanLogs = itemsPerBatchSpanLogs;
  }

  public int getItemsPerBatchEvents() {
    return itemsPerBatchEvents;
  }

  public void setItemsPerBatchEvents(int itemsPerBatchEvents) {
    this.itemsPerBatchEvents = itemsPerBatchEvents;
  }

  public int getMinBatchSplitSize() {
    return minBatchSplitSize;
  }

  public void setMinBatchSplitSize(int minBatchSplitSize) {
    this.minBatchSplitSize = minBatchSplitSize;
  }

  public int getMemoryBufferLimit() {
    return memoryBufferLimit;
  }

  public void setMemoryBufferLimit(int memoryBufferLimit) {
    this.memoryBufferLimit = memoryBufferLimit;
  }

  public int getMemoryBufferLimitSourceTags() {
    return memoryBufferLimitSourceTags;
  }

  public void setMemoryBufferLimitSourceTags(int memoryBufferLimitSourceTags) {
    this.memoryBufferLimitSourceTags = memoryBufferLimitSourceTags;
  }

  public int getMemoryBufferLimitEvents() {
    return memoryBufferLimitEvents;
  }

  public void setMemoryBufferLimitEvents(int memoryBufferLimitEvents) {
    this.memoryBufferLimitEvents = memoryBufferLimitEvents;
  }

  public Supplier<Integer> getItemsPerBatchForEntityType(ReportableEntityType entityType) {
    switch (entityType) {
      case POINT:
      case DELTA_COUNTER:
      case HISTOGRAM:
        return this::getItemsPerBatch;
      case SOURCE_TAG:
        return this::getItemsPerBatchSourceTags;
      case TRACE:
        return this::getItemsPerBatchSpans;
      case TRACE_SPAN_LOGS:
        return this::getItemsPerBatchSpanLogs;
      case EVENT:
        return this::getItemsPerBatchEvents;
      default:
        throw new IllegalArgumentException("Unexpected entity type " + entityType.name());
    }
  }

  public Supplier<Integer> getMemoryBufferLimitForEntityType(ReportableEntityType entityType) {
    switch (entityType) {
      case POINT:
      case DELTA_COUNTER:
      case HISTOGRAM:
      case TRACE:
      case TRACE_SPAN_LOGS:
        return this::getMemoryBufferLimit;
      case SOURCE_TAG:
        return this::getMemoryBufferLimitSourceTags;
      case EVENT:
        return this::getMemoryBufferLimitEvents;
      default:
        throw new IllegalArgumentException("Unexpected entity type " + entityType.name());
    }
  }

  public static Builder newBuilder() {
    return new Builder();
  }

  public static class Builder {
    private Boolean splitPushWhenRateLimited;
    private Double retryBackoffBaseSeconds;
    private Integer pushFlushInterval;
    private Integer itemsPerBatch;
    private Integer itemsPerBatchHistograms;
    private Integer itemsPerBatchSourceTags;
    private Integer itemsPerBatchSpans;
    private Integer itemsPerBatchSpanLogs;
    private Integer itemsPerBatchEvents;
    private Integer pushRateLimitMaxBurstSeconds;
    private Integer minBatchSplitSize;
    private Integer memoryBufferLimit;
    private Integer memoryBufferLimitSourceTags;
    private Integer memoryBufferLimitEvents;

    private Builder() {
    }

    public Builder setSplitPushWhenRateLimited(boolean splitPushWhenRateLimited) {
      this.splitPushWhenRateLimited = splitPushWhenRateLimited;
      return this;
    }

    public Builder setRetryBackoffBaseSeconds(double retryBackoffBaseSeconds) {
      this.retryBackoffBaseSeconds = retryBackoffBaseSeconds;
      return this;
    }

    public Builder setPushFlushInterval(int pushFlushInterval) {
      this.pushFlushInterval = pushFlushInterval;
      return this;
    }

    public Builder setItemsPerBatch(int itemsPerBatch) {
      this.itemsPerBatch = itemsPerBatch;
      return this;
    }

    public Builder setItemsPerBatchHistograms(int itemsPerBatchHistograms) {
      this.itemsPerBatchHistograms = itemsPerBatchHistograms;
      return this;
    }

    public Builder setItemsPerBatchSourceTags(int itemsPerBatchSourceTags) {
      this.itemsPerBatchSourceTags = itemsPerBatchSourceTags;
      return this;
    }

    public Builder setItemsPerBatchSpans(int itemsPerBatchSpans) {
      this.itemsPerBatchSpans = itemsPerBatchSpans;
      return this;
    }

    public Builder setItemsPerBatchSpanLogs(int itemsPerBatchSpanLogs) {
      this.itemsPerBatchSpanLogs = itemsPerBatchSpanLogs;
      return this;
    }

    public Builder setItemsPerBatchEvents(int itemsPerBatchEvents) {
      this.itemsPerBatchEvents = itemsPerBatchEvents;
      return this;
    }

    public Builder setPushRateLimitMaxBurstSeconds(int pushRateLimitMaxBurstSeconds) {
      this.pushRateLimitMaxBurstSeconds = pushRateLimitMaxBurstSeconds;
      return this;
    }

    public Builder setMinBatchSplitSize(int minBatchSplitSize) {
      this.minBatchSplitSize = minBatchSplitSize;
      return this;
    }

    public Builder setMemoryBufferLimit(int memoryBufferLimit) {
      this.memoryBufferLimit = memoryBufferLimit;
      return this;
    }

    public Builder setMemoryBufferLimitSourceTags(int memoryBufferLimitSourceTags) {
      this.memoryBufferLimitSourceTags = memoryBufferLimitSourceTags;
      return this;
    }

    public Builder setMemoryBufferLimitEvents(int memoryBufferLimitEvents) {
      this.memoryBufferLimitEvents = memoryBufferLimitEvents;
      return this;
    }

    public ProxyRuntimeProperties build() {
      checkNotNull(itemsPerBatch, "itemsPerBatch not set!");
      checkNotNull(itemsPerBatchHistograms, "itemsPerBatchHistograms not set!");
      checkNotNull(itemsPerBatchSourceTags, "itemsPerBatchSourceTags not set!");
      checkNotNull(itemsPerBatchSpans, "itemsPerBatchSpans not set!");
      checkNotNull(itemsPerBatchSpanLogs, "itemsPerBatchSpanLogs not set!");
      checkNotNull(itemsPerBatchEvents, "itemsPerBatchEvents not set!");
      checkNotNull(retryBackoffBaseSeconds, "retryBackoffBaseSeconds not set!");
      checkNotNull(splitPushWhenRateLimited, "splitPushWhenRateLimited not set!");
      checkNotNull(pushRateLimitMaxBurstSeconds, "pushRateLimitMaxBurstSeconds not set!");
      checkNotNull(pushFlushInterval, "pushFlushInterval not set!");
      checkNotNull(minBatchSplitSize, "minBatchSplitSize not set!");
      checkNotNull(memoryBufferLimit, "memoryBufferLimit not set!");
      checkNotNull(memoryBufferLimitSourceTags, "memoryBufferLimitSourceTags not set!");
      checkNotNull(memoryBufferLimitEvents, "memoryBufferLimitEvents not set!");
      ProxyRuntimeProperties prop = new ProxyRuntimeProperties();
      prop.itemsPerBatchOriginal = prop.itemsPerBatch = itemsPerBatch;
      prop.itemsPerBatchHistogramsOriginal = prop.itemsPerBatchHistograms = itemsPerBatchHistograms;
      prop.itemsPerBatchSourceTagsOriginal = prop.itemsPerBatchSourceTags = itemsPerBatchSourceTags;
      prop.itemsPerBatchSpansOriginal = prop.itemsPerBatchSpans = itemsPerBatchSpans;
      prop.itemsPerBatchSpanLogsOriginal = prop.itemsPerBatchSpanLogs = itemsPerBatchSpanLogs;
      prop.itemsPerBatchEventsOriginal = prop.itemsPerBatchEvents = itemsPerBatchEvents;
      prop.retryBackoffBaseSecondsOriginal = prop.retryBackoffBaseSeconds = retryBackoffBaseSeconds;
      prop.splitPushWhenRateLimited = splitPushWhenRateLimited;
      prop.pushRateLimitMaxBurstSeconds = pushRateLimitMaxBurstSeconds;
      prop.pushFlushInterval = pushFlushInterval;
      prop.minBatchSplitSize = minBatchSplitSize;
      prop.memoryBufferLimit = memoryBufferLimit;
      prop.memoryBufferLimitSourceTags = memoryBufferLimitSourceTags;
      prop.memoryBufferLimitEvents = memoryBufferLimitEvents;
      return prop;
    }
  }
}
