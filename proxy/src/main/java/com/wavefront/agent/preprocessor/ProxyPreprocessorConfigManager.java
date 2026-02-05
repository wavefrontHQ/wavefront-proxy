package com.wavefront.agent.preprocessor;

import com.google.common.annotations.VisibleForTesting;
import com.wavefront.agent.ProxyCheckInScheduler;
import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.Counter;
import com.yammer.metrics.core.MetricName;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;
import java.util.function.Supplier;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.annotation.Nonnull;
import com.wavefront.api.agent.preprocessor.PreprocessorConfigManager;
import com.wavefront.api.agent.preprocessor.ReportableEntityPreprocessor;

/**
 * Extends the PreprocessorConfigManager in java-lib, which parses preprocessor rules (organized by listening port)
 *
 * <p>Created by Vasily on 9/15/16.
 */
public class ProxyPreprocessorConfigManager extends PreprocessorConfigManager {
  private static final Logger logger =
      Logger.getLogger(ProxyPreprocessorConfigManager.class.getCanonicalName());
  private static final Counter configReloads =
      Metrics.newCounter(new MetricName("preprocessor", "", "config-reloads.successful"));
  private static final Counter failedConfigReloads =
      Metrics.newCounter(new MetricName("preprocessor", "", "config-reloads.failed"));

  private final Supplier<Long> timeSupplier;

  @VisibleForTesting public Map<String, ReportableEntityPreprocessor> userPreprocessors;

  private volatile long userPreprocessorsTs;
  private static String proxyConfigRules;

  public ProxyPreprocessorConfigManager() {
    this(System::currentTimeMillis);
  }

  /** @param timeSupplier Supplier for current time (in millis). */
  @VisibleForTesting
  ProxyPreprocessorConfigManager(@Nonnull Supplier<Long> timeSupplier) {
    this.timeSupplier = timeSupplier;
    userPreprocessorsTs = timeSupplier.get();
    userPreprocessors = Collections.emptyMap();
  }

  /**
   * Schedules periodic checks for config file modification timestamp and performs hot-reload
   *
   * @param fileName Path name of the file to be monitored.
   *
   * @param fileCheckIntervalMillis Timestamp check interval.
   */
  public void setUpConfigFileMonitoring(String fileName, int fileCheckIntervalMillis) {
    new Timer("Timer-preprocessor-configmanager")
        .schedule(
            new TimerTask() {
              @Override
              public void run() {
                loadFileIfModified(fileName);
              }
            },
            fileCheckIntervalMillis,
            fileCheckIntervalMillis);
  }

  @VisibleForTesting
  void loadFileIfModified(String fileName) {
    // skip loading file completely if rules are already set from FE
    if (ProxyCheckInScheduler.isRulesSetInFE.get()) return;
    try {
      File file = new File(fileName);
      long lastModified = file.lastModified();
      if (lastModified > userPreprocessorsTs) {
        logger.info("File " + file + " has been modified on disk, reloading preprocessor rules");
        loadFile(fileName);
        configReloads.inc();
      }
    } catch (Exception e) {
      logger.log(Level.SEVERE, "Unable to load preprocessor rules", e);
      failedConfigReloads.inc();
    }
  }

  public void loadFile(String filename) throws FileNotFoundException {
    File file = new File(filename);
    super.loadFromStream(new FileInputStream(file));
    proxyConfigRules = getFileRules(filename);
    ProxyCheckInScheduler.preprocessorRulesNeedUpdate.set(true);
  }

  public void loadFERules(String rules) {
    logger.info("New preprocessor rules detected! Loading preprocessor rules from FE Configuration");
    InputStream is = new ByteArrayInputStream(rules.getBytes(StandardCharsets.UTF_8));
    super.loadFromStream(is);
    proxyConfigRules = rules;
    ProxyCheckInScheduler.preprocessorRulesNeedUpdate.set(true);
  }

  public static String getProxyConfigRules() {
    return proxyConfigRules;
  }

  public static String getFileRules(String filename) {
    try {
      if (filename == null || filename.isEmpty()) return null;
      return new String(
              Files.readAllBytes(Paths.get(filename)),
              StandardCharsets.UTF_8
      );
    } catch (IOException e) {
      throw new RuntimeException("Unable to read file rules as string", e);
    }
  }

  // For testing purposes
  @VisibleForTesting
  public static void clearProxyConfigRules() {
    proxyConfigRules = null;
  }
}
