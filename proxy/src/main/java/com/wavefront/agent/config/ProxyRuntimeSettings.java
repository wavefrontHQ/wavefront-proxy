package com.wavefront.agent.config;

import com.wavefront.data.ReportableEntityType;

import java.util.function.Supplier;

/**
 * Container object for mutable settings that can change at runtime
 *
 * @author vasily@wavefront.com
 */
public class ProxyRuntimeSettings {
  public static final ProxyRuntimeSettings DEFAULT_SETTINGS = new ProxyRuntimeSettings();

  static {
    DEFAULT_SETTINGS.setSplitPushWhenRateLimited(false);
    DEFAULT_SETTINGS.setRetryBackoffBaseSeconds(2.0d);
    DEFAULT_SETTINGS.setPushFlushInterval(1000);
    DEFAULT_SETTINGS.setItemsPerBatch(40000);
    DEFAULT_SETTINGS.setItemsPerBatchSourceTags(50);
    DEFAULT_SETTINGS.setItemsPerBatchSpans(5000);
    DEFAULT_SETTINGS.setItemsPerBatchSpanLogs(1000);
    DEFAULT_SETTINGS.setItemsPerBatchEvents(50);
    DEFAULT_SETTINGS.setMinBatchSplitSize(500);
    DEFAULT_SETTINGS.setMemoryBufferLimit(16 * 40000);
    DEFAULT_SETTINGS.setMemoryBufferLimitSourceTags(16 * 50);
    DEFAULT_SETTINGS.setMemoryBufferLimitEvents(16 * 50);
  }

  private Integer itemsPerBatchOriginal;
  private Integer itemsPerBatchSourceTagsOriginal;
  private Integer itemsPerBatchSpansOriginal;
  private Integer itemsPerBatchSpanLogsOriginal;
  private Integer itemsPerBatchEventsOriginal;
  private Double retryBackoffBaseSecondsOriginal;

  private volatile boolean splitPushWhenRateLimited;
  private volatile double retryBackoffBaseSeconds;
  private volatile int pushFlushInterval;
  private volatile int itemsPerBatch;
  private volatile int itemsPerBatchSourceTags;
  private volatile int itemsPerBatchSpans;
  private volatile int itemsPerBatchSpanLogs;
  private volatile int itemsPerBatchEvents;
  private volatile int minBatchSplitSize;
  private volatile int memoryBufferLimit;
  private volatile int memoryBufferLimitSourceTags;
  private volatile int memoryBufferLimitEvents;

  public ProxyRuntimeSettings() {
  }

  public int getItemsPerBatchOriginal() {
    return itemsPerBatchOriginal;
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
    if (retryBackoffBaseSecondsOriginal == null) {
      retryBackoffBaseSecondsOriginal = retryBackoffBaseSeconds;
    }
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
    if (itemsPerBatchOriginal == null) {
      itemsPerBatchOriginal = itemsPerBatch;
    }
  }

  public int getItemsPerBatchSourceTags() {
    return itemsPerBatchSourceTags;
  }

  public void setItemsPerBatchSourceTags(int itemsPerBatchSourceTags) {
    this.itemsPerBatchSourceTags = itemsPerBatchSourceTags;
    if (itemsPerBatchSourceTagsOriginal == null) {
      itemsPerBatchSourceTagsOriginal = itemsPerBatchSourceTags;
    }
  }

  public int getItemsPerBatchSpans() {
    return itemsPerBatchSpans;
  }

  public void setItemsPerBatchSpans(int itemsPerBatchSpans) {
    this.itemsPerBatchSpans = itemsPerBatchSpans;
    if (itemsPerBatchSpansOriginal == null) {
      itemsPerBatchSpansOriginal = itemsPerBatchSpans;
    }
  }

  public int getItemsPerBatchSpanLogs() {
    return itemsPerBatchSpanLogs;
  }

  public void setItemsPerBatchSpanLogs(int itemsPerBatchSpanLogs) {
    this.itemsPerBatchSpanLogs = itemsPerBatchSpanLogs;
    if (itemsPerBatchSpanLogsOriginal == null) {
      itemsPerBatchSpanLogsOriginal = itemsPerBatchSpanLogs;
    }
  }

  public int getItemsPerBatchEvents() {
    return itemsPerBatchEvents;
  }

  public void setItemsPerBatchEvents(int itemsPerBatchEvents) {
    this.itemsPerBatchEvents = itemsPerBatchEvents;
    if (itemsPerBatchEventsOriginal == null) {
      itemsPerBatchEventsOriginal = itemsPerBatchEvents;
    }
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
}
