package com.wavefront.agent.preprocessor;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;

import com.wavefront.common.TaggedMetricName;
import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.Counter;
import com.yammer.metrics.core.MetricName;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.StringUtils;
import org.yaml.snakeyaml.Yaml;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.Timer;
import java.util.TimerTask;
import java.util.function.Supplier;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.annotation.Nonnull;

import static com.wavefront.agent.preprocessor.PreprocessorUtil.getBoolean;
import static com.wavefront.agent.preprocessor.PreprocessorUtil.getInteger;
import static com.wavefront.agent.preprocessor.PreprocessorUtil.getPredicate;
import static com.wavefront.agent.preprocessor.PreprocessorUtil.getString;
import static com.wavefront.agent.preprocessor.PreprocessorUtil.v2PredicateKey;

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
  private static final Set<String> ALLOWED_RULE_ARGUMENTS = ImmutableSet.of("rule", "action");
  private static final String GLOBAL_PORT_KEY = "global";

  private final Supplier<Long> timeSupplier;
  private final Map<String, ReportableEntityPreprocessor> systemPreprocessors = new HashMap<>();
  private final Map<String, PreprocessorRuleMetrics> preprocessorRuleMetricsMap = new HashMap<>();

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
    this(System::currentTimeMillis);
  }

  /**
   * @param timeSupplier Supplier for current time (in millis).
   */
  @VisibleForTesting
  PreprocessorConfigManager(@Nonnull Supplier<Long> timeSupplier) {
    this.timeSupplier = timeSupplier;
    userPreprocessorsTs = timeSupplier.get();
    userPreprocessors = Collections.emptyMap();
  }

  /**
   * Schedules periodic checks for config file modification timestamp and performs hot-reload
   *
   * @param fileName                Path name of the file to be monitored.
   * @param fileCheckIntervalMillis Timestamp check interval.
   */
  public void setUpConfigFileMonitoring(String fileName, int fileCheckIntervalMillis) {
    new Timer("Timer-preprocessor-configmanager").schedule(new TimerTask() {
      @Override
      public void run() {
        loadFileIfModified(fileName);
      }
    }, fileCheckIntervalMillis, fileCheckIntervalMillis);
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

  private void requireArguments(@Nonnull Map<String, Object> rule, String... arguments) {
    if (rule.isEmpty())
      throw new IllegalArgumentException("Rule is empty");
    for (String argument : arguments) {
      if (rule.get(argument) == null || ((rule.get(argument) instanceof String) &&
          ((String) rule.get(argument)).replaceAll("[^a-z0-9_-]", "").isEmpty()))
        throw new IllegalArgumentException("'" + argument + "' is missing or empty");
    }
  }

  private void allowArguments(@Nonnull Map<String, Object> rule, String... arguments) {
    Sets.SetView<String> invalidArguments = Sets.difference(rule.keySet(),
        Sets.union(ALLOWED_RULE_ARGUMENTS, Sets.newHashSet(arguments)));
    if (invalidArguments.size() > 0) {
      throw new IllegalArgumentException("Invalid or not applicable argument(s): " +
          StringUtils.join(invalidArguments, ","));
    }
  }

  @VisibleForTesting
  void loadFileIfModified(String fileName) {
    try {
      File file = new File(fileName);
      long lastModified = file.lastModified();
      if (lastModified > userPreprocessorsTs) {
        logger.info("File " + file +
            " has been modified on disk, reloading preprocessor rules");
        loadFile(fileName);
        configReloads.inc();
      }
    } catch (Exception e) {
      logger.log(Level.SEVERE, "Unable to load preprocessor rules", e);
      failedConfigReloads.inc();
    }
  }

  public void loadFile(String filename) throws FileNotFoundException {
    loadFromStream(new FileInputStream(new File(filename)));
  }

  @VisibleForTesting
  void loadFromStream(InputStream stream) {
    totalValidRules = 0;
    totalInvalidRules = 0;
    Yaml yaml = new Yaml();
    Map<String, ReportableEntityPreprocessor> portMap = new HashMap<>();
    try {
      //noinspection unchecked
      Map<String, Object> rulesByPort = (Map<String, Object>) yaml.load(stream);
      if (rulesByPort == null || rulesByPort.isEmpty()) {
        logger.warning("Empty preprocessor rule file detected!");
        logger.info("Total 0 rules loaded");
        synchronized (this) {
          this.userPreprocessorsTs = timeSupplier.get();
          this.userPreprocessors = Collections.emptyMap();
        }
        return;
      }
      for (String strPortKey : rulesByPort.keySet()) {
        // Handle comma separated ports and global ports.
        // Note: Global ports need to be specified at the end of the file, inorder to be
        // applicable to all the explicitly specified ports in preprocessor_rules.yaml file.
        List<String> strPortList = new ArrayList<>();
        if (strPortKey.equalsIgnoreCase(GLOBAL_PORT_KEY)) {
          strPortList.addAll(portMap.keySet());
        } else {
          strPortList = Arrays.asList(strPortKey.trim().split("\\s*,\\s*"));
        }
        for (String strPort : strPortList) {
          portMap.putIfAbsent(strPort, new ReportableEntityPreprocessor());
          int validRules = 0;
          //noinspection unchecked
          List<Map<String, Object>> rules = (List<Map<String, Object>>) rulesByPort.get(strPortKey);
          for (Map<String, Object> rule : rules) {
            try {
              requireArguments(rule, "rule", "action");
              allowArguments(rule, "scope", "search", "replace", "match", "tag", "key", "newtag",
                  "newkey", "value", "source", "input", "iterations", "replaceSource",
                  "replaceInput", "actionSubtype", "maxLength", "firstMatchOnly", "whitelist", v2PredicateKey);
              String ruleName = Objects.requireNonNull(getString(rule, "rule")).
                  replaceAll("[^a-z0-9_-]", "");
              PreprocessorRuleMetrics ruleMetrics = preprocessorRuleMetricsMap.computeIfAbsent(
                  strPort, k -> new PreprocessorRuleMetrics(
                      Metrics.newCounter(new TaggedMetricName("preprocessor." + ruleName,
                      "count", "port", strPort)),
                      Metrics.newCounter(new TaggedMetricName("preprocessor." + ruleName,
                      "cpu_nanos", "port", strPort)),
                      Metrics.newCounter(new TaggedMetricName("preprocessor." + ruleName,
                      "checked-count", "port", strPort))));
              if ("pointLine".equals(getString(rule, "scope"))) {
                if (getPredicate(rule, v2PredicateKey) != null) {
                  throw new IllegalArgumentException("Argument ["+v2PredicateKey+"] is not " +
                      "allowed in [scope] = pointline.");
                }
                switch (Objects.requireNonNull(getString(rule, "action"))) {
                  case "replaceRegex":
                    allowArguments(rule, "scope", "search", "replace", "match", "iterations");
                    portMap.get(strPort).forPointLine().addTransformer(
                        new PointLineReplaceRegexTransformer(getString(rule, "search"),
                            getString(rule, "replace"), getString(rule, "match"),
                            getInteger(rule, "iterations", 1), ruleMetrics));
                    break;
                  case "blacklistRegex":
                    allowArguments(rule, "scope", "match", v2PredicateKey);
                    portMap.get(strPort).forPointLine().addFilter(
                        new PointLineBlacklistRegexFilter(getString(rule, "match"), ruleMetrics));
                    break;
                  case "whitelistRegex":
                    allowArguments(rule, "scope", "match");
                    portMap.get(strPort).forPointLine().addFilter(
                        new PointLineWhitelistRegexFilter(getString(rule, "match"), ruleMetrics));
                    break;
                  default:
                    throw new IllegalArgumentException("Action '" + getString(rule, "action") +
                        "' is not valid or cannot be applied to pointLine");
                }
              } else {
                switch (Objects.requireNonNull(getString(rule, "action"))) {

                  // Rules for ReportPoint objects
                  case "replaceRegex":
                    allowArguments(rule, "scope", "search", "replace", "match", "iterations", v2PredicateKey);
                    portMap.get(strPort).forReportPoint().addTransformer(
                        new ReportPointReplaceRegexTransformer(getString(rule, "scope"),
                            getString(rule, "search"), getString(rule, "replace"),
                            getString(rule, "match"), getInteger(rule, "iterations", 1),
                            getPredicate(rule, v2PredicateKey), ruleMetrics));
                    break;
                  case "forceLowercase":
                    allowArguments(rule, "scope", "match", v2PredicateKey);
                    portMap.get(strPort).forReportPoint().addTransformer(
                        new ReportPointForceLowercaseTransformer(getString(rule, "scope"),
                            getString(rule, "match"), getPredicate(rule, v2PredicateKey),
                            ruleMetrics));
                    break;
                  case "addTag":
                    allowArguments(rule, "tag", "value", v2PredicateKey);
                    portMap.get(strPort).forReportPoint().addTransformer(
                        new ReportPointAddTagTransformer(getString(rule, "tag"),
                            getString(rule, "value"), getPredicate(rule, v2PredicateKey),
                            ruleMetrics));
                    break;
                  case "addTagIfNotExists":
                    allowArguments(rule, "tag", "value", v2PredicateKey);
                    portMap.get(strPort).forReportPoint().addTransformer(
                        new ReportPointAddTagIfNotExistsTransformer(getString(rule, "tag"),
                            getString(rule, "value"), getPredicate(rule, v2PredicateKey),
                            ruleMetrics));
                    break;
                  case "dropTag":
                    allowArguments(rule, "tag", "match", v2PredicateKey);
                    portMap.get(strPort).forReportPoint().addTransformer(
                        new ReportPointDropTagTransformer(getString(rule, "tag"),
                            getString(rule, "match"), getPredicate(rule, v2PredicateKey), ruleMetrics));
                    break;
                  case "extractTag":
                    allowArguments(rule, "tag", "source", "search", "replace", "replaceSource",
                        "replaceInput", "match", v2PredicateKey);
                    portMap.get(strPort).forReportPoint().addTransformer(
                        new ReportPointExtractTagTransformer(getString(rule, "tag"),
                            getString(rule, "source"), getString(rule, "search"),
                            getString(rule, "replace"),
                            (String) rule.getOrDefault("replaceInput", rule.get("replaceSource")),
                            getString(rule, "match"), getPredicate(rule, v2PredicateKey), ruleMetrics));
                    break;
                  case "extractTagIfNotExists":
                    allowArguments(rule, "tag", "source", "search", "replace", "replaceSource",
                        "replaceInput", "match", v2PredicateKey);
                    portMap.get(strPort).forReportPoint().addTransformer(
                        new ReportPointExtractTagIfNotExistsTransformer(getString(rule, "tag"),
                            getString(rule, "source"), getString(rule, "search"),
                            getString(rule, "replace"),
                            (String) rule.getOrDefault("replaceInput", rule.get("replaceSource")),
                            getString(rule, "match"), getPredicate(rule, v2PredicateKey), ruleMetrics));
                    break;
                  case "renameTag":
                    allowArguments(rule, "tag", "newtag", "match", v2PredicateKey);
                    portMap.get(strPort).forReportPoint().addTransformer(
                        new ReportPointRenameTagTransformer(getString(rule, "tag"),
                            getString(rule, "newtag"), getString(rule, "match"),
                            getPredicate(rule, v2PredicateKey), ruleMetrics));
                    break;
                  case "limitLength":
                    allowArguments(rule, "scope", "actionSubtype", "maxLength", "match",
                        v2PredicateKey);
                    portMap.get(strPort).forReportPoint().addTransformer(
                        new ReportPointLimitLengthTransformer(
                            Objects.requireNonNull(getString(rule, "scope")),
                            getInteger(rule, "maxLength", 0),
                            LengthLimitActionType.fromString(getString(rule, "actionSubtype")),
                            getString(rule, "match"), getPredicate(rule, v2PredicateKey),
                            ruleMetrics));
                    break;
                  case "blacklistRegex":
                    allowArguments(rule, "scope", "match", v2PredicateKey);
                    portMap.get(strPort).forReportPoint().addFilter(
                        new ReportPointBlacklistRegexFilter(getString(rule, "scope"),
                            getString(rule, "match"), getPredicate(rule, v2PredicateKey),
                            ruleMetrics));
                    break;
                  case "whitelistRegex":
                    allowArguments(rule, "scope", "match", v2PredicateKey);
                    portMap.get(strPort).forReportPoint().addFilter(
                        new ReportPointWhitelistRegexFilter(getString(rule, "scope"),
                            getString(rule, "match"), getPredicate(rule, v2PredicateKey), ruleMetrics));
                    break;

                  // Rules for Span objects
                  case "spanReplaceRegex":
                    allowArguments(rule, "scope", "search", "replace", "match", "iterations",
                        "firstMatchOnly", v2PredicateKey);
                    portMap.get(strPort).forSpan().addTransformer(
                        new SpanReplaceRegexTransformer(getString(rule, "scope"),
                            getString(rule, "search"), getString(rule, "replace"),
                            getString(rule, "match"), getInteger(rule, "iterations", 1),
                            getBoolean(rule, "firstMatchOnly", false),
                            getPredicate(rule, v2PredicateKey), ruleMetrics));
                    break;
                  case "spanForceLowercase":
                    allowArguments(rule, "scope", "match", "firstMatchOnly", v2PredicateKey);
                    portMap.get(strPort).forSpan().addTransformer(
                        new SpanForceLowercaseTransformer(getString(rule, "scope"),
                            getString(rule, "match"), getBoolean(rule, "firstMatchOnly", false),
                            getPredicate(rule, v2PredicateKey), ruleMetrics));
                    break;
                  case "spanAddAnnotation":
                  case "spanAddTag":
                    allowArguments(rule, "key", "value", v2PredicateKey);
                    portMap.get(strPort).forSpan().addTransformer(
                        new SpanAddAnnotationTransformer(getString(rule, "key"),
                            getString(rule, "value"), getPredicate(rule, v2PredicateKey),
                            ruleMetrics));
                    break;
                  case "spanAddAnnotationIfNotExists":
                  case "spanAddTagIfNotExists":
                    allowArguments(rule, "key", "value", v2PredicateKey);
                    portMap.get(strPort).forSpan().addTransformer(
                        new SpanAddAnnotationIfNotExistsTransformer(getString(rule, "key"),
                            getString(rule, "value"), getPredicate(rule, v2PredicateKey),
                            ruleMetrics));
                    break;
                  case "spanDropAnnotation":
                  case "spanDropTag":
                    allowArguments(rule, "key", "match", "firstMatchOnly", v2PredicateKey);
                    portMap.get(strPort).forSpan().addTransformer(
                        new SpanDropAnnotationTransformer(getString(rule, "key"),
                            getString(rule, "match"), getBoolean(rule, "firstMatchOnly", false),
                            getPredicate(rule, v2PredicateKey), ruleMetrics));
                    break;
                  case "spanWhitelistAnnotation":
                  case "spanWhitelistTag":
                    allowArguments(rule, "whitelist", v2PredicateKey);
                    portMap.get(strPort).forSpan().addTransformer(
                        SpanWhitelistAnnotationTransformer.create(rule,
                            getPredicate(rule, v2PredicateKey), ruleMetrics));
                    break;
                  case "spanExtractAnnotation":
                  case "spanExtractTag":
                    allowArguments(rule, "key", "input", "search", "replace", "replaceInput", "match",
                        "firstMatchOnly", v2PredicateKey);
                    portMap.get(strPort).forSpan().addTransformer(
                        new SpanExtractAnnotationTransformer(getString(rule, "key"),
                            getString(rule, "input"), getString(rule, "search"),
                            getString(rule, "replace"), getString(rule, "replaceInput"),
                            getString(rule, "match"), getBoolean(rule, "firstMatchOnly", false),
                            getPredicate(rule, v2PredicateKey), ruleMetrics));
                    break;
                  case "spanExtractAnnotationIfNotExists":
                  case "spanExtractTagIfNotExists":
                    allowArguments(rule, "key", "input", "search", "replace", "replaceInput", "match",
                        "firstMatchOnly", v2PredicateKey);
                    portMap.get(strPort).forSpan().addTransformer(
                        new SpanExtractAnnotationIfNotExistsTransformer(getString(rule, "key"),
                            getString(rule, "input"), getString(rule, "search"),
                            getString(rule, "replace"), getString(rule, "replaceInput"),
                            getString(rule, "match"), getBoolean(rule, "firstMatchOnly", false),
                            getPredicate(rule, v2PredicateKey), ruleMetrics));
                    break;
                  case "spanRenameAnnotation":
                  case "spanRenameTag":
                    allowArguments(rule, "key", "newkey", "match", "firstMatchOnly", v2PredicateKey);
                    portMap.get(strPort).forSpan().addTransformer(
                        new SpanRenameAnnotationTransformer(
                            getString(rule, "key"), getString(rule, "newkey"),
                            getString(rule, "match"), getBoolean(rule, "firstMatchOnly", false),
                            getPredicate(rule, v2PredicateKey), ruleMetrics));
                    break;
                  case "spanLimitLength":
                    allowArguments(rule, "scope", "actionSubtype", "maxLength", "match",
                        "firstMatchOnly", v2PredicateKey);
                    portMap.get(strPort).forSpan().addTransformer(
                        new SpanLimitLengthTransformer(
                            Objects.requireNonNull(getString(rule, "scope")),
                            getInteger(rule, "maxLength", 0),
                            LengthLimitActionType.fromString(getString(rule, "actionSubtype")),
                            getString(rule, "match"), getBoolean(rule, "firstMatchOnly", false),
                            getPredicate(rule, v2PredicateKey), ruleMetrics));
                    break;
                  case "spanBlacklistRegex":
                    allowArguments(rule, "scope", "match", v2PredicateKey);
                    portMap.get(strPort).forSpan().addFilter(
                        new SpanBlacklistRegexFilter(
                            getString(rule, "scope"),
                            getString(rule, "match"), getPredicate(rule, v2PredicateKey), ruleMetrics));
                    break;
                  case "spanWhitelistRegex":
                    allowArguments(rule, "scope", "match", v2PredicateKey);
                    portMap.get(strPort).forSpan().addFilter(
                        new SpanWhitelistRegexFilter(getString(rule, "scope"),
                            getString(rule, "match"), getPredicate(rule, v2PredicateKey), ruleMetrics));
                    break;
                  default:
                    throw new IllegalArgumentException("Action '" + getString(rule, "action") +
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
          logger.info("Loaded " + validRules + " rules for port :: " + strPort);
          totalValidRules += validRules;
        }
        logger.info("Loaded Preprocessor rules for port key :: \"" + strPortKey + "\"");
      }
      logger.info("Total Preprocessor rules loaded :: " + totalValidRules);
      if (totalInvalidRules > 0) {
        throw new RuntimeException("Total Invalid Preprocessor rules detected :: " + totalInvalidRules);
      }
    } catch (ClassCastException e) {
      throw new RuntimeException("Can't parse preprocessor configuration", e);
    } finally {
      IOUtils.closeQuietly(stream);
    }
    synchronized (this) {
      this.userPreprocessorsTs = timeSupplier.get();
      this.userPreprocessors = portMap;
    }
  }
}
