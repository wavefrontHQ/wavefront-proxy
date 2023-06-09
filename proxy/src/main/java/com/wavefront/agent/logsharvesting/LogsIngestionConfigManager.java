package com.wavefront.agent.logsharvesting;

import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;
import com.google.common.annotations.VisibleForTesting;
import com.wavefront.agent.config.ConfigurationException;
import com.wavefront.agent.config.LogsIngestionConfig;
import com.wavefront.agent.config.MetricMatcher;
import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.Counter;
import com.yammer.metrics.core.MetricName;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Supplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Wrapper for a {@link LogsIngestionConfig} that supports hot-loading and removal notifications.
 */
public class LogsIngestionConfigManager {
  protected static final Logger logger =
      LoggerFactory.getLogger(LogsIngestionConfigManager.class.getCanonicalName());
  private static final Counter configReloads =
      Metrics.newCounter(new MetricName("logsharvesting", "", "config-reloads.successful"));
  private static final Counter failedConfigReloads =
      Metrics.newCounter(new MetricName("logsharvesting", "", "config-reloads.failed"));
  // The only key in this cache is "true". Basically we want the cache expiry and reloading logic.
  private final LoadingCache<Boolean, LogsIngestionConfig> logsIngestionConfigLoadingCache;
  private final Consumer<MetricMatcher> removalListener;
  private LogsIngestionConfig lastParsedConfig;

  public LogsIngestionConfigManager(
      Supplier<LogsIngestionConfig> logsIngestionConfigSupplier,
      Consumer<MetricMatcher> removalListener)
      throws ConfigurationException {
    this.removalListener = removalListener;
    lastParsedConfig = logsIngestionConfigSupplier.get();
    if (lastParsedConfig == null)
      throw new ConfigurationException("Could not load initial config.");
    lastParsedConfig.verifyAndInit();
    this.logsIngestionConfigLoadingCache =
        Caffeine.newBuilder()
            .expireAfterWrite(lastParsedConfig.configReloadIntervalSeconds, TimeUnit.SECONDS)
            .build(
                (ignored) -> {
                  LogsIngestionConfig nextConfig = logsIngestionConfigSupplier.get();
                  if (nextConfig == null) {
                    logger.warn("Unable to reload logs ingestion config file!");
                    failedConfigReloads.inc();
                  } else if (!lastParsedConfig.equals(nextConfig)) {
                    nextConfig.verifyAndInit(); // If it throws, we keep the last
                    // (good) config.
                    processConfigChange(nextConfig);
                    logger.info("Loaded new config: " + lastParsedConfig.toString());
                    configReloads.inc();
                  }
                  return lastParsedConfig;
                });

    // Force reload every N seconds.
    new Timer("Timer-logsingestion-configmanager")
        .schedule(
            new TimerTask() {
              @Override
              public void run() {
                try {
                  logsIngestionConfigLoadingCache.get(true);
                } catch (Exception e) {
                  logger.error("Cannot load a new logs ingestion config.", e);
                }
              }
            },
            lastParsedConfig.aggregationIntervalSeconds,
            lastParsedConfig.aggregationIntervalSeconds);
  }

  public LogsIngestionConfig getConfig() {
    return logsIngestionConfigLoadingCache.get(true);
  }

  /** Forces the next call to {@link #getConfig()} to call the config supplier. */
  @VisibleForTesting
  public void forceConfigReload() {
    logsIngestionConfigLoadingCache.invalidate(true);
  }

  private void processConfigChange(LogsIngestionConfig nextConfig) {
    if (nextConfig.useWavefrontHistograms != lastParsedConfig.useWavefrontHistograms) {
      logger.warn(
          "useWavefrontHistograms property cannot be changed at runtime, "
              + "proxy restart required!");
    }
    if (nextConfig.useDeltaCounters != lastParsedConfig.useDeltaCounters) {
      logger.warn(
          "useDeltaCounters property cannot be changed at runtime, " + "proxy restart required!");
    }
    if (nextConfig.reportEmptyHistogramStats != lastParsedConfig.reportEmptyHistogramStats) {
      logger.warn(
          "reportEmptyHistogramStats property cannot be changed at runtime, "
              + "proxy restart required!");
    }
    if (!nextConfig.aggregationIntervalSeconds.equals(
        lastParsedConfig.aggregationIntervalSeconds)) {
      logger.warn(
          "aggregationIntervalSeconds property cannot be changed at runtime, "
              + "proxy restart required!");
    }
    if (nextConfig.configReloadIntervalSeconds != lastParsedConfig.configReloadIntervalSeconds) {
      logger.warn(
          "configReloadIntervalSeconds property cannot be changed at runtime, "
              + "proxy restart required!");
    }
    if (nextConfig.expiryMillis != lastParsedConfig.expiryMillis) {
      logger.warn(
          "expiryMillis property cannot be changed at runtime, " + "proxy restart required!");
    }
    for (MetricMatcher oldMatcher : lastParsedConfig.counters) {
      if (!nextConfig.counters.contains(oldMatcher)) removalListener.accept(oldMatcher);
    }
    for (MetricMatcher oldMatcher : lastParsedConfig.gauges) {
      if (!nextConfig.gauges.contains(oldMatcher)) removalListener.accept(oldMatcher);
    }
    for (MetricMatcher oldMatcher : lastParsedConfig.histograms) {
      if (!nextConfig.histograms.contains(oldMatcher)) removalListener.accept(oldMatcher);
    }
    lastParsedConfig = nextConfig;
  }
}
