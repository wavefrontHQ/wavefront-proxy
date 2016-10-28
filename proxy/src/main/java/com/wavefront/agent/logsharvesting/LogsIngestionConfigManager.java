package com.wavefront.agent.logsharvesting;

import com.google.common.annotations.VisibleForTesting;

import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;
import com.wavefront.agent.config.ConfigurationException;
import com.wavefront.agent.config.LogsIngestionConfig;
import com.wavefront.agent.config.MetricMatcher;

import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.logging.Logger;

/**
 * Wrapper for a {@link LogsIngestionConfig} that supports hotloading and removal notifications.
 *
 * @author Mori Bellamy (mori@wavefront.com)
 */
public class LogsIngestionConfigManager {
  protected static final Logger logger = Logger.getLogger(LogsIngestionConfigManager.class.getCanonicalName());
  private LogsIngestionConfig lastParsedConfig;
  // The only key in this cache is "true". Basically we want the cache expiry and reloading logic.
  private final LoadingCache<Boolean, LogsIngestionConfig> logsIngestionConfigLoadingCache;
  private final Consumer<MetricMatcher> removalListener;

  public LogsIngestionConfigManager(Supplier<LogsIngestionConfig> logsIngestionConfigSupplier,
                                    Consumer<MetricMatcher> removalListener) throws ConfigurationException {
    this.removalListener = removalListener;
    lastParsedConfig = logsIngestionConfigSupplier.get();
    if (lastParsedConfig == null) throw new ConfigurationException("Could not load initial config.");
    this.logsIngestionConfigLoadingCache = Caffeine.<Boolean, LogsIngestionConfig>newBuilder()
        .expireAfterWrite(lastParsedConfig.configReloadIntervalSeconds, TimeUnit.SECONDS)
        .build((ignored) -> {
          LogsIngestionConfig nextConfig = logsIngestionConfigSupplier.get();
          if (nextConfig == null) {
            logger.warning("Could not load a new logs ingestion config file, check above for a stack trace.");
          } else if (!lastParsedConfig.equals(nextConfig)) {
            nextConfig.verifyAndInit();  // If it throws, we keep the last (good) config.
            processConfigChange(nextConfig);
            logger.info("Loaded new config: " + lastParsedConfig.toString());
          }
          return lastParsedConfig;
        });

    // Force reload every N seconds.
    new Timer().schedule(new TimerTask() {
      @Override
      public void run() {
        logsIngestionConfigLoadingCache.get(true);
      }
    }, lastParsedConfig.aggregationIntervalSeconds, lastParsedConfig.aggregationIntervalSeconds);
  }

  public LogsIngestionConfig getConfig() {
    return logsIngestionConfigLoadingCache.get(true);
  }

  /**
   * Forces the next call to {@link #getConfig()} to call the config supplier.
   */
  @VisibleForTesting
  public void forceConfigReload() {
    logsIngestionConfigLoadingCache.invalidate(true);
  }

  private void processConfigChange(LogsIngestionConfig nextConfig) {
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
