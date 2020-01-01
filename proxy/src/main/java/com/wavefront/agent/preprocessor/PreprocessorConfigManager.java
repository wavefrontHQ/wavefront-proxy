package com.wavefront.agent.preprocessor;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Sets;

import com.wavefront.common.TaggedMetricName;
import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.Counter;
import com.yammer.metrics.core.MetricName;

import org.apache.commons.lang.StringUtils;
import org.yaml.snakeyaml.Yaml;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStream;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;
import java.util.function.Supplier;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * Parses preprocessor rules (organized by listening port)
 *
 * Created by Vasily on 9/15/16.
 */
public class PreprocessorConfigManager {
  private static final Logger logger = Logger.getLogger(
      PreprocessorConfigManager.class.getCanonicalName());
  private static final Counter configReloads = Metrics.newCounter(
      new MetricName("preprocessor", "", "config-reloads.successful"));
  private static final Counter failedConfigReloads = Metrics.newCounter(
      new MetricName("preprocessor", "", "config-reloads.failed"));

  private final Supplier<Long> timeSupplier;
  private final Map<String, ReportableEntityPreprocessor> systemPreprocessors = new HashMap<>();

  private Map<String, ReportableEntityPreprocessor> userPreprocessors;
  private Map<String, ReportableEntityPreprocessor> preprocessors = null;
  private volatile long systemPreprocessorsTs = Long.MIN_VALUE;
  private volatile long userPreprocessorsTs;
  private volatile long lastBuild = Long.MIN_VALUE;

  @VisibleForTesting
  int totalInvalidRules = 0;
  @VisibleForTesting
  int totalValidRules = 0;

  public PreprocessorConfigManager() {
    this(null, null, System::currentTimeMillis);
  }

  public PreprocessorConfigManager(@Nullable String fileName) throws FileNotFoundException {
    this(fileName, fileName == null ? null : new FileInputStream(fileName),
        System::currentTimeMillis);
  }

  @VisibleForTesting
  PreprocessorConfigManager(@Nullable String fileName,
                            @Nullable InputStream inputStream,
                            @Nonnull Supplier<Long> timeSupplier) {
    this.timeSupplier = timeSupplier;

    if (inputStream != null) {
      // if input stream is specified, perform initial load from the stream
      try {
        userPreprocessorsTs = timeSupplier.get();
        userPreprocessors = loadFromStream(inputStream);
      } catch (RuntimeException ex) {
        throw new RuntimeException(ex.getMessage() + " - aborting start-up");
      }
    }
    if (fileName != null) {
      // if there is a file name with preprocessor rules, load it and schedule periodic reloads
      new Timer("Timer-preprocessor-configmanager").schedule(new TimerTask() {
        @Override
        public void run() {
          try {
            File file = new File(fileName);
            long lastModified = file.lastModified();
            if (lastModified > userPreprocessorsTs) {
              logger.info("File " + file +
                  " has been modified on disk, reloading preprocessor rules");
              userPreprocessorsTs = timeSupplier.get();
              userPreprocessors = loadFromStream(new FileInputStream(file));
              configReloads.inc();
            }
          } catch (Exception e) {
            logger.log(Level.SEVERE, "Unable to load preprocessor rules", e);
            failedConfigReloads.inc();
          }
        }
      }, 5, 5);
    } else if (inputStream == null){
      userPreprocessorsTs = timeSupplier.get();
      userPreprocessors = Collections.emptyMap();
    }
  }

  public ReportableEntityPreprocessor getSystemPreprocessor(String key) {
    systemPreprocessorsTs = timeSupplier.get();
    return systemPreprocessors.computeIfAbsent(key, x -> new ReportableEntityPreprocessor());
  }

  public Supplier<ReportableEntityPreprocessor> get(String handle) {
    return () -> getPreprocessor(handle);
  }

  private ReportableEntityPreprocessor getPreprocessor(String key) {
    if ((lastBuild < userPreprocessorsTs || lastBuild < systemPreprocessorsTs) &&
        userPreprocessors != null) {
      synchronized (this) {
        if ((lastBuild < userPreprocessorsTs || lastBuild < systemPreprocessorsTs) &&
            userPreprocessors != null) {
          this.preprocessors = Stream.of(this.systemPreprocessors, this.userPreprocessors).
              flatMap(x -> x.entrySet().stream()).
              collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue,
                  ReportableEntityPreprocessor::merge));
          this.lastBuild = timeSupplier.get();
        }
      }
    }
    return this.preprocessors.computeIfAbsent(key, x -> new ReportableEntityPreprocessor());
  }
  private void requireArguments(@Nonnull Map<String, String> rule, String... arguments) {
    if (rule.isEmpty())
      throw new IllegalArgumentException("Rule is empty");
    for (String argument : arguments) {
      if (rule.get(argument) == null || rule.get(argument).replaceAll("[^a-z0-9_-]", "").isEmpty())
        throw new IllegalArgumentException("'" + argument + "' is missing or empty");
    }
  }

  private void allowArguments(@Nonnull Map<String, String> rule, String... arguments) {
    Sets.SetView<String> invalidArguments = Sets.difference(rule.keySet(),
        Sets.newHashSet(arguments));
    if (invalidArguments.size() > 0) {
      throw new IllegalArgumentException("Invalid or not applicable argument(s): " +
          StringUtils.join(invalidArguments, ","));
    }
  }

  @VisibleForTesting
  Map<String, ReportableEntityPreprocessor> loadFromStream(InputStream stream) {
    totalValidRules = 0;
    totalInvalidRules = 0;
    Yaml yaml = new Yaml();
    Map<String, ReportableEntityPreprocessor> portMap = new HashMap<>();
    try {
      //noinspection unchecked
      Map<String, Object> rulesByPort = (Map<String, Object>) yaml.load(stream);
      if (rulesByPort == null) {
        logger.warning("Empty preprocessor rule file detected!");
        logger.info("Total 0 rules loaded");
        return Collections.emptyMap();
      }
      for (String strPort : rulesByPort.keySet()) {
        portMap.put(strPort, new ReportableEntityPreprocessor());
        int validRules = 0;
        //noinspection unchecked
        List<Map<String, String>> rules = (List<Map<String, String>>) rulesByPort.get(strPort);
        for (Map<String, String> rule : rules) {
          try {
            requireArguments(rule, "rule", "action");
            allowArguments(rule, "rule", "action", "scope", "search", "replace", "match", "tag",
                "key", "newtag", "newkey", "value", "source", "input", "iterations", "replaceSource",
                "replaceInput", "actionSubtype", "maxLength", "firstMatchOnly");
            String ruleName = rule.get("rule").replaceAll("[^a-z0-9_-]", "");
            PreprocessorRuleMetrics ruleMetrics = new PreprocessorRuleMetrics(
                Metrics.newCounter(new TaggedMetricName("preprocessor." + ruleName,
                    "count", "port", strPort)),
                Metrics.newCounter(new TaggedMetricName("preprocessor." + ruleName,
                    "cpu_nanos", "port", strPort)),
                Metrics.newCounter(new TaggedMetricName("preprocessor." + ruleName,
                    "checked-count", "port", strPort)));

            if (rule.get("scope") != null && rule.get("scope").equals("pointLine")) {
              switch (rule.get("action")) {
                case "replaceRegex":
                  allowArguments(rule, "rule", "action", "scope", "search", "replace", "match",
                      "iterations");
                  portMap.get(strPort).forPointLine().addTransformer(
                      new PointLineReplaceRegexTransformer(rule.get("search"), rule.get("replace"),
                          rule.get("match"), Integer.parseInt(rule.getOrDefault("iterations", "1")),
                          ruleMetrics));
                  break;
                case "blacklistRegex":
                  allowArguments(rule, "rule", "action", "scope", "match");
                  portMap.get(strPort).forPointLine().addFilter(
                      new PointLineBlacklistRegexFilter(rule.get("match"), ruleMetrics));
                  break;
                case "whitelistRegex":
                  allowArguments(rule, "rule", "action", "scope", "match");
                  portMap.get(strPort).forPointLine().addFilter(
                      new PointLineWhitelistRegexFilter(rule.get("match"), ruleMetrics));
                  break;
                default:
                  throw new IllegalArgumentException("Action '" + rule.get("action") +
                      "' is not valid or cannot be applied to pointLine");
              }
            } else {
              switch (rule.get("action")) {

                // Rules for ReportPoint objects
                case "replaceRegex":
                  allowArguments(rule, "rule", "action", "scope", "search", "replace", "match",
                      "iterations");
                  portMap.get(strPort).forReportPoint().addTransformer(
                      new ReportPointReplaceRegexTransformer(rule.get("scope"), rule.get("search"),
                          rule.get("replace"), rule.get("match"),
                          Integer.parseInt(rule.getOrDefault("iterations", "1")), ruleMetrics));
                  break;
                case "forceLowercase":
                  allowArguments(rule, "rule", "action", "scope", "match");
                  portMap.get(strPort).forReportPoint().addTransformer(
                      new ReportPointForceLowercaseTransformer(rule.get("scope"), rule.get("match"),
                          ruleMetrics));
                  break;
                case "addTag":
                  allowArguments(rule, "rule", "action", "tag", "value");
                  portMap.get(strPort).forReportPoint().addTransformer(
                      new ReportPointAddTagTransformer(rule.get("tag"), rule.get("value"),
                          ruleMetrics));
                  break;
                case "addTagIfNotExists":
                  allowArguments(rule, "rule", "action", "tag", "value");
                  portMap.get(strPort).forReportPoint().addTransformer(
                      new ReportPointAddTagIfNotExistsTransformer(rule.get("tag"),
                          rule.get("value"), ruleMetrics));
                  break;
                case "dropTag":
                  allowArguments(rule, "rule", "action", "tag", "match");
                  portMap.get(strPort).forReportPoint().addTransformer(
                      new ReportPointDropTagTransformer(rule.get("tag"), rule.get("match"),
                          ruleMetrics));
                  break;
                case "extractTag":
                  allowArguments(rule, "rule", "action", "tag", "source", "search", "replace",
                      "replaceSource", "replaceInput", "match");
                  portMap.get(strPort).forReportPoint().addTransformer(
                      new ReportPointExtractTagTransformer(rule.get("tag"), rule.get("source"),
                          rule.get("search"), rule.get("replace"),
                          rule.getOrDefault("replaceInput", rule.get("replaceSource")),
                          rule.get("match"), ruleMetrics));
                  break;
                case "extractTagIfNotExists":
                  allowArguments(rule, "rule", "action", "tag", "source", "search", "replace",
                      "replaceSource", "replaceInput", "match");
                  portMap.get(strPort).forReportPoint().addTransformer(
                      new ReportPointExtractTagIfNotExistsTransformer(rule.get("tag"),
                          rule.get("source"), rule.get("search"), rule.get("replace"),
                          rule.getOrDefault("replaceInput", rule.get("replaceSource")),
                          rule.get("match"), ruleMetrics));
                  break;
                case "renameTag":
                  allowArguments(rule, "rule", "action", "tag", "newtag", "match");
                  portMap.get(strPort).forReportPoint().addTransformer(
                      new ReportPointRenameTagTransformer(
                          rule.get("tag"), rule.get("newtag"), rule.get("match"), ruleMetrics));
                  break;
                case "limitLength":
                  allowArguments(rule, "rule", "action", "scope", "actionSubtype", "maxLength",
                      "match");
                  portMap.get(strPort).forReportPoint().addTransformer(
                      new ReportPointLimitLengthTransformer(rule.get("scope"),
                          Integer.parseInt(rule.get("maxLength")),
                          LengthLimitActionType.fromString(rule.get("actionSubtype")),
                          rule.get("match"), ruleMetrics));
                  break;
                case "blacklistRegex":
                  allowArguments(rule, "rule", "action", "scope", "match");
                  portMap.get(strPort).forReportPoint().addFilter(
                      new ReportPointBlacklistRegexFilter(rule.get("scope"), rule.get("match"),
                          ruleMetrics));
                  break;
                case "whitelistRegex":
                  allowArguments(rule, "rule", "action", "scope", "match");
                  portMap.get(strPort).forReportPoint().addFilter(
                      new ReportPointWhitelistRegexFilter(rule.get("scope"), rule.get("match"),
                          ruleMetrics));
                  break;

                // Rules for Span objects
                case "spanReplaceRegex":
                  allowArguments(rule, "rule", "action", "scope", "search", "replace", "match",
                      "iterations", "firstMatchOnly");
                  portMap.get(strPort).forSpan().addTransformer(
                      new SpanReplaceRegexTransformer(rule.get("scope"), rule.get("search"),
                          rule.get("replace"), rule.get("match"),
                          Integer.parseInt(rule.getOrDefault("iterations", "1")),
                          Boolean.parseBoolean(rule.getOrDefault("firstMatchOnly", "false")),
                          ruleMetrics));
                  break;
                case "spanForceLowercase":
                  allowArguments(rule, "rule", "action", "scope", "match", "firstMatchOnly");
                  portMap.get(strPort).forSpan().addTransformer(
                      new SpanForceLowercaseTransformer(rule.get("scope"), rule.get("match"),
                          Boolean.parseBoolean(rule.getOrDefault("firstMatchOnly", "false")),
                          ruleMetrics));
                  break;
                case "spanAddAnnotation":
                case "spanAddTag":
                  allowArguments(rule, "rule", "action", "key", "value");
                  portMap.get(strPort).forSpan().addTransformer(
                      new SpanAddAnnotationTransformer(rule.get("key"), rule.get("value"),
                          ruleMetrics));
                  break;
                case "spanAddAnnotationIfNotExists":
                case "spanAddTagIfNotExists":
                  allowArguments(rule, "rule", "action", "key", "value");
                  portMap.get(strPort).forSpan().addTransformer(
                      new SpanAddAnnotationIfNotExistsTransformer(rule.get("key"),
                          rule.get("value"), ruleMetrics));
                  break;
                case "spanDropAnnotation":
                case "spanDropTag":
                  allowArguments(rule, "rule", "action", "key", "match", "firstMatchOnly");
                  portMap.get(strPort).forSpan().addTransformer(
                      new SpanDropAnnotationTransformer(rule.get("key"), rule.get("match"),
                          Boolean.parseBoolean(rule.getOrDefault("firstMatchOnly", "false")),
                          ruleMetrics));
                  break;
                case "spanExtractAnnotation":
                case "spanExtractTag":
                  allowArguments(rule, "rule", "action", "key", "input", "search", "replace",
                      "replaceInput", "match", "firstMatchOnly");
                  portMap.get(strPort).forSpan().addTransformer(
                      new SpanExtractAnnotationTransformer(rule.get("key"), rule.get("input"),
                          rule.get("search"), rule.get("replace"), rule.get("replaceInput"),
                          rule.get("match"), Boolean.parseBoolean(
                              rule.getOrDefault("firstMatchOnly", "false")), ruleMetrics));
                  break;
                case "spanExtractAnnotationIfNotExists":
                case "spanExtractTagIfNotExists":
                  allowArguments(rule, "rule", "action", "key", "input", "search", "replace",
                      "replaceInput", "match", "firstMatchOnly");
                  portMap.get(strPort).forSpan().addTransformer(
                      new SpanExtractAnnotationIfNotExistsTransformer(rule.get("key"),
                          rule.get("input"), rule.get("search"), rule.get("replace"),
                          rule.get("replaceInput"), rule.get("match"), Boolean.parseBoolean(
                              rule.getOrDefault("firstMatchOnly", "false")), ruleMetrics));
                  break;
                case "spanRenameAnnotation":
                case "spanRenameTag":
                  allowArguments(rule, "rule", "action", "key", "newkey", "match", "firstMatchOnly");
                  portMap.get(strPort).forSpan().addTransformer(
                      new SpanRenameAnnotationTransformer(
                          rule.get("key"), rule.get("newkey"), rule.get("match"),
                          Boolean.parseBoolean(rule.getOrDefault("firstMatchOnly", "false")),
                          ruleMetrics));
                  break;
                case "spanLimitLength":
                  allowArguments(rule, "rule", "action", "scope", "actionSubtype", "maxLength",
                      "match", "firstMatchOnly");
                  portMap.get(strPort).forSpan().addTransformer(
                      new SpanLimitLengthTransformer(rule.get("scope"),
                          Integer.parseInt(rule.get("maxLength")),
                          LengthLimitActionType.fromString(rule.get("actionSubtype")),
                          rule.get("match"), Boolean.parseBoolean(
                              rule.getOrDefault("firstMatchOnly", "false")), ruleMetrics));
                  break;
                case "spanBlacklistRegex":
                  allowArguments(rule, "rule", "action", "scope", "match");
                  portMap.get(strPort).forSpan().addFilter(
                      new SpanBlacklistRegexFilter(rule.get("scope"), rule.get("match"),
                          ruleMetrics));
                  break;
                case "spanWhitelistRegex":
                  allowArguments(rule, "rule", "action", "scope", "match");
                  portMap.get(strPort).forSpan().addFilter(
                      new SpanWhitelistRegexFilter(rule.get("scope"), rule.get("match"),
                          ruleMetrics));
                  break;
                default:
                  throw new IllegalArgumentException("Action '" + rule.get("action") +
                      "' is not valid");
              }
            }
            validRules++;
          } catch (IllegalArgumentException | NullPointerException ex) {
            logger.warning("Invalid rule " + (rule == null ? "" : rule.getOrDefault("rule", "")) +
                " (port " + strPort + "): " + ex);
            totalInvalidRules++;
          }
        }
        logger.info("Loaded " + validRules + " rules for port " + strPort);
        totalValidRules += validRules;
      }
      logger.info("Total " + totalValidRules + " rules loaded");
      if (totalInvalidRules > 0) {
        throw new RuntimeException("Total " + totalInvalidRules + " invalid rules detected");
      }
    } catch (ClassCastException e) {
      throw new RuntimeException("Can't parse preprocessor configuration");
    }
    return portMap;
  }
}
