package com.wavefront.agent.preprocessor;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;

import com.wavefront.common.TaggedMetricName;
import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.Counter;
import com.yammer.metrics.core.MetricName;

import org.apache.commons.codec.Charsets;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.StringUtils;
import org.yaml.snakeyaml.Yaml;

import java.io.BufferedInputStream;
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
import static com.wavefront.agent.preprocessor.PreprocessorUtil.getString;

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
  private static final String GLOBAL_PORT_KEY = "global";

  // rule keywords
  private static final String RULE = "rule";
  private static final String ACTION = "action";
  private static final String SCOPE = "scope";
  private static final String SEARCH = "search";
  private static final String REPLACE = "replace";
  private static final String MATCH = "match";
  private static final String TAG = "tag";
  private static final String KEY = "key";
  private static final String NEWTAG = "newtag";
  private static final String NEWKEY = "newkey";
  private static final String VALUE = "value";
  private static final String SOURCE = "source";
  private static final String INPUT = "input";
  private static final String ITERATIONS = "iterations";
  private static final String REPLACE_SOURCE = "replaceSource";
  private static final String REPLACE_INPUT = "replaceInput";
  private static final String ACTION_SUBTYPE = "actionSubtype";
  private static final String MAX_LENGTH = "maxLength";
  private static final String FIRST_MATCH_ONLY = "firstMatchOnly";
  private static final String ALLOW = "allow";
  private static final String IF = "if";
  private static final Set<String> ALLOWED_RULE_ARGUMENTS = ImmutableSet.of(RULE, ACTION);

  private final Supplier<Long> timeSupplier;
  private final Map<String, ReportableEntityPreprocessor> systemPreprocessors = new HashMap<>();

  @VisibleForTesting
  public Map<String, ReportableEntityPreprocessor> userPreprocessors;
  private Map<String, ReportableEntityPreprocessor> preprocessors = null;

  private volatile long systemPreprocessorsTs = Long.MIN_VALUE;
  private volatile long userPreprocessorsTs;
  private volatile long lastBuild = Long.MIN_VALUE;
  private String lastProcessedRules = "";
  
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

  public void processRemoteRules(@Nonnull String rules) {
     if (!rules.equals(lastProcessedRules)) {
       lastProcessedRules = rules;
       logger.info("Preprocessor rules received from remote, processing");
       loadFromStream(IOUtils.toInputStream(rules, Charsets.UTF_8));
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
      Map<String, Object> rulesByPort = yaml.load(stream);
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
        List<String> strPortList = strPortKey.equalsIgnoreCase(GLOBAL_PORT_KEY) ?
            new ArrayList<>(portMap.keySet()) :
            Arrays.asList(strPortKey.trim().split("\\s*,\\s*"));
        for (String strPort : strPortList) {
          portMap.putIfAbsent(strPort, new ReportableEntityPreprocessor());
          int validRules = 0;
          //noinspection unchecked
          List<Map<String, Object>> rules = (List<Map<String, Object>>) rulesByPort.get(strPortKey);
          for (Map<String, Object> rule : rules) {
            try {
              requireArguments(rule, RULE, ACTION);
              allowArguments(rule, SCOPE, SEARCH, REPLACE, MATCH, TAG, KEY, NEWTAG, NEWKEY, VALUE,
                  SOURCE, INPUT, ITERATIONS, REPLACE_SOURCE, REPLACE_INPUT, ACTION_SUBTYPE,
                  MAX_LENGTH, FIRST_MATCH_ONLY, ALLOW, IF);
              String ruleName = Objects.requireNonNull(getString(rule, RULE)).
                  replaceAll("[^a-z0-9_-]", "");
              PreprocessorRuleMetrics ruleMetrics = new PreprocessorRuleMetrics(
                      Metrics.newCounter(new TaggedMetricName("preprocessor." + ruleName,
                      "count", "port", strPort)),
                      Metrics.newCounter(new TaggedMetricName("preprocessor." + ruleName,
                      "cpu_nanos", "port", strPort)),
                      Metrics.newCounter(new TaggedMetricName("preprocessor." + ruleName,
                      "checked-count", "port", strPort)));
              String scope = getString(rule, SCOPE);
              if ("pointLine".equals(scope) || "inputText".equals(scope)) {
                if (Predicates.getPredicate(rule) != null) {
                  throw new IllegalArgumentException("Argument [if] is not " +
                      "allowed in [scope] = " + scope);
                }
                switch (Objects.requireNonNull(getString(rule, ACTION))) {
                  case "replaceRegex":
                    allowArguments(rule, SCOPE, SEARCH, REPLACE, MATCH, ITERATIONS);
                    portMap.get(strPort).forPointLine().addTransformer(
                        new LineBasedReplaceRegexTransformer(getString(rule, SEARCH),
                            getString(rule, REPLACE), getString(rule, MATCH),
                            getInteger(rule, ITERATIONS, 1), ruleMetrics));
                    break;
                  case "blacklistRegex":
                  case "block":
                    allowArguments(rule, SCOPE, MATCH);
                    portMap.get(strPort).forPointLine().addFilter(
                        new LineBasedBlockFilter(getString(rule, MATCH), ruleMetrics));
                    break;
                  case "whitelistRegex":
                  case "allow":
                    allowArguments(rule, SCOPE, MATCH);
                    portMap.get(strPort).forPointLine().addFilter(
                        new LineBasedAllowFilter(getString(rule, MATCH), ruleMetrics));
                    break;
                  default:
                    throw new IllegalArgumentException("Action '" + getString(rule, ACTION) +
                        "' is not valid or cannot be applied to pointLine");
                }
              } else {
                String action = Objects.requireNonNull(getString(rule, ACTION));
                switch (action) {

                  // Rules for ReportPoint objects
                  case "replaceRegex":
                    allowArguments(rule, SCOPE, SEARCH, REPLACE, MATCH, ITERATIONS, IF);
                    portMap.get(strPort).forReportPoint().addTransformer(
                        new ReportPointReplaceRegexTransformer(scope,
                            getString(rule, SEARCH), getString(rule, REPLACE),
                            getString(rule, MATCH), getInteger(rule, ITERATIONS, 1),
                            Predicates.getPredicate(rule),
                            ruleMetrics));
                    break;
                  case "forceLowercase":
                    allowArguments(rule, SCOPE, MATCH, IF);
                    portMap.get(strPort).forReportPoint().addTransformer(
                        new ReportPointForceLowercaseTransformer(scope,
                            getString(rule, MATCH), Predicates.getPredicate(rule),
                            ruleMetrics));
                    break;
                  case "addTag":
                    allowArguments(rule, TAG, VALUE, IF);
                    portMap.get(strPort).forReportPoint().addTransformer(
                        new ReportPointAddTagTransformer(getString(rule, TAG),
                            getString(rule, VALUE), Predicates.getPredicate(rule),
                            ruleMetrics));
                    break;
                  case "addTagIfNotExists":
                    allowArguments(rule, TAG, VALUE, IF);
                    portMap.get(strPort).forReportPoint().addTransformer(
                        new ReportPointAddTagIfNotExistsTransformer(getString(rule, TAG),
                            getString(rule, VALUE), Predicates.getPredicate(rule),
                            ruleMetrics));
                    break;
                  case "dropTag":
                    allowArguments(rule, TAG, MATCH, IF);
                    portMap.get(strPort).forReportPoint().addTransformer(
                        new ReportPointDropTagTransformer(getString(rule, TAG),
                            getString(rule, MATCH), Predicates.getPredicate(rule),
                            ruleMetrics));
                    break;
                  case "extractTag":
                    allowArguments(rule, TAG, "source", SEARCH, REPLACE, REPLACE_SOURCE,
                        REPLACE_INPUT, MATCH, IF);
                    portMap.get(strPort).forReportPoint().addTransformer(
                        new ReportPointExtractTagTransformer(getString(rule, TAG),
                            getString(rule, "source"), getString(rule, SEARCH),
                            getString(rule, REPLACE),
                            (String) rule.getOrDefault(REPLACE_INPUT, rule.get(REPLACE_SOURCE)),
                            getString(rule, MATCH), Predicates.getPredicate(rule),
                            ruleMetrics));
                    break;
                  case "extractTagIfNotExists":
                    allowArguments(rule, TAG, "source", SEARCH, REPLACE, REPLACE_SOURCE,
                        REPLACE_INPUT, MATCH, IF);
                    portMap.get(strPort).forReportPoint().addTransformer(
                        new ReportPointExtractTagIfNotExistsTransformer(getString(rule, TAG),
                            getString(rule, "source"), getString(rule, SEARCH),
                            getString(rule, REPLACE),
                            (String) rule.getOrDefault(REPLACE_INPUT, rule.get(REPLACE_SOURCE)),
                            getString(rule, MATCH), Predicates.getPredicate(rule),
                            ruleMetrics));
                    break;
                  case "renameTag":
                    allowArguments(rule, TAG, NEWTAG, MATCH, IF);
                    portMap.get(strPort).forReportPoint().addTransformer(
                        new ReportPointRenameTagTransformer(getString(rule, TAG),
                            getString(rule, NEWTAG), getString(rule, MATCH),
                            Predicates.getPredicate(rule), ruleMetrics));
                    break;
                  case "limitLength":
                    allowArguments(rule, SCOPE, ACTION_SUBTYPE, MAX_LENGTH, MATCH,
                        IF);
                    portMap.get(strPort).forReportPoint().addTransformer(
                        new ReportPointLimitLengthTransformer(
                            Objects.requireNonNull(scope),
                            getInteger(rule, MAX_LENGTH, 0),
                            LengthLimitActionType.fromString(getString(rule, ACTION_SUBTYPE)),
                            getString(rule, MATCH), Predicates.getPredicate(rule),
                            ruleMetrics));
                    break;
                  case "count":
                    allowArguments(rule, SCOPE, IF);
                    portMap.get(strPort).forReportPoint().addTransformer(
                        new CountTransformer<>(Predicates.getPredicate(rule), ruleMetrics));
                    break;
                  case "blacklistRegex":
                    logger.warning("Preprocessor rule using deprecated syntax (action: " + action +
                        "), use 'action: block' instead!");
                  case "block":
                    allowArguments(rule, SCOPE, MATCH, IF);
                    portMap.get(strPort).forReportPoint().addFilter(
                        new ReportPointBlockFilter(scope,
                            getString(rule, MATCH), Predicates.getPredicate(rule),
                            ruleMetrics));
                    break;
                  case "whitelistRegex":
                    logger.warning("Preprocessor rule using deprecated syntax (action: " + action +
                        "), use 'action: allow' instead!");
                  case "allow":
                    allowArguments(rule, SCOPE, MATCH, IF);
                    portMap.get(strPort).forReportPoint().addFilter(
                        new ReportPointAllowFilter(scope,
                            getString(rule, MATCH), Predicates.getPredicate(rule),
                            ruleMetrics));
                    break;

                  // Rules for Span objects
                  case "spanReplaceRegex":
                    allowArguments(rule, SCOPE, SEARCH, REPLACE, MATCH, ITERATIONS,
                        FIRST_MATCH_ONLY, IF);
                    portMap.get(strPort).forSpan().addTransformer(
                        new SpanReplaceRegexTransformer(scope,
                            getString(rule, SEARCH), getString(rule, REPLACE),
                            getString(rule, MATCH), getInteger(rule, ITERATIONS, 1),
                            getBoolean(rule, FIRST_MATCH_ONLY, false),
                            Predicates.getPredicate(rule), ruleMetrics));
                    break;
                  case "spanForceLowercase":
                    allowArguments(rule, SCOPE, MATCH, FIRST_MATCH_ONLY, IF);
                    portMap.get(strPort).forSpan().addTransformer(
                        new SpanForceLowercaseTransformer(scope,
                            getString(rule, MATCH), getBoolean(rule, FIRST_MATCH_ONLY, false),
                            Predicates.getPredicate(rule), ruleMetrics));
                    break;
                  case "spanAddAnnotation":
                  case "spanAddTag":
                    allowArguments(rule, KEY, VALUE, IF);
                    portMap.get(strPort).forSpan().addTransformer(
                        new SpanAddAnnotationTransformer(getString(rule, KEY),
                            getString(rule, VALUE), Predicates.getPredicate(rule),
                            ruleMetrics));
                    break;
                  case "spanAddAnnotationIfNotExists":
                  case "spanAddTagIfNotExists":
                    allowArguments(rule, KEY, VALUE, IF);
                    portMap.get(strPort).forSpan().addTransformer(
                        new SpanAddAnnotationIfNotExistsTransformer(getString(rule, KEY),
                            getString(rule, VALUE), Predicates.getPredicate(rule),
                            ruleMetrics));
                    break;
                  case "spanDropAnnotation":
                  case "spanDropTag":
                    allowArguments(rule, KEY, MATCH, FIRST_MATCH_ONLY, IF);
                    portMap.get(strPort).forSpan().addTransformer(
                        new SpanDropAnnotationTransformer(getString(rule, KEY),
                            getString(rule, MATCH), getBoolean(rule, FIRST_MATCH_ONLY, false),
                            Predicates.getPredicate(rule), ruleMetrics));
                    break;
                  case "spanWhitelistAnnotation":
                  case "spanWhitelistTag":
                    logger.warning("Preprocessor rule using deprecated syntax (action: " + action +
                        "), use 'action: spanAllowAnnotation' instead!");
                  case "spanAllowAnnotation":
                  case "spanAllowTag":
                    allowArguments(rule, ALLOW, IF);
                    portMap.get(strPort).forSpan().addTransformer(
                        SpanAllowAnnotationTransformer.create(rule,
                            Predicates.getPredicate(rule), ruleMetrics));
                    break;
                  case "spanExtractAnnotation":
                  case "spanExtractTag":
                    allowArguments(rule, KEY, INPUT, SEARCH, REPLACE, REPLACE_INPUT, MATCH,
                        FIRST_MATCH_ONLY, IF);
                    portMap.get(strPort).forSpan().addTransformer(
                        new SpanExtractAnnotationTransformer(getString(rule, KEY),
                            getString(rule, INPUT), getString(rule, SEARCH),
                            getString(rule, REPLACE), getString(rule, REPLACE_INPUT),
                            getString(rule, MATCH), getBoolean(rule, FIRST_MATCH_ONLY, false),
                            Predicates.getPredicate(rule), ruleMetrics));
                    break;
                  case "spanExtractAnnotationIfNotExists":
                  case "spanExtractTagIfNotExists":
                    allowArguments(rule, KEY, INPUT, SEARCH, REPLACE, REPLACE_INPUT, MATCH,
                        FIRST_MATCH_ONLY, IF);
                    portMap.get(strPort).forSpan().addTransformer(
                        new SpanExtractAnnotationIfNotExistsTransformer(getString(rule, KEY),
                            getString(rule, INPUT), getString(rule, SEARCH),
                            getString(rule, REPLACE), getString(rule, REPLACE_INPUT),
                            getString(rule, MATCH), getBoolean(rule, FIRST_MATCH_ONLY, false),
                            Predicates.getPredicate(rule), ruleMetrics));
                    break;
                  case "spanRenameAnnotation":
                  case "spanRenameTag":
                    allowArguments(rule, KEY, NEWKEY, MATCH, FIRST_MATCH_ONLY, IF);
                    portMap.get(strPort).forSpan().addTransformer(
                        new SpanRenameAnnotationTransformer(
                            getString(rule, KEY), getString(rule, NEWKEY),
                            getString(rule, MATCH), getBoolean(rule, FIRST_MATCH_ONLY, false),
                            Predicates.getPredicate(rule), ruleMetrics));
                    break;
                  case "spanLimitLength":
                    allowArguments(rule, SCOPE, ACTION_SUBTYPE, MAX_LENGTH, MATCH,
                        FIRST_MATCH_ONLY, IF);
                    portMap.get(strPort).forSpan().addTransformer(
                        new SpanLimitLengthTransformer(
                            Objects.requireNonNull(scope),
                            getInteger(rule, MAX_LENGTH, 0),
                            LengthLimitActionType.fromString(getString(rule, ACTION_SUBTYPE)),
                            getString(rule, MATCH), getBoolean(rule, FIRST_MATCH_ONLY, false),
                            Predicates.getPredicate(rule), ruleMetrics));
                    break;
                  case "spanCount":
                    allowArguments(rule, SCOPE, IF);
                    portMap.get(strPort).forSpan().addTransformer(
                        new CountTransformer<>(Predicates.getPredicate(rule), ruleMetrics));
                    break;
                  case "spanBlacklistRegex":
                    logger.warning("Preprocessor rule using deprecated syntax (action: " + action +
                        "), use 'action: spanBlock' instead!");
                  case "spanBlock":
                    allowArguments(rule, SCOPE, MATCH, IF);
                    portMap.get(strPort).forSpan().addFilter(
                        new SpanBlockFilter(
                            scope,
                            getString(rule, MATCH), Predicates.getPredicate(rule),
                            ruleMetrics));
                    break;
                  case "spanWhitelistRegex":
                    logger.warning("Preprocessor rule using deprecated syntax (action: " + action +
                        "), use 'action: spanAllow' instead!");
                  case "spanAllow":
                    allowArguments(rule, SCOPE, MATCH, IF);
                    portMap.get(strPort).forSpan().addFilter(
                        new SpanAllowFilter(scope,
                            getString(rule, MATCH), Predicates.getPredicate(rule),
                            ruleMetrics));
                    break;
                  default:
                    throw new IllegalArgumentException("Action '" + getString(rule, ACTION) +
                        "' is not valid");
                }
              }
              validRules++;
            } catch (IllegalArgumentException | NullPointerException ex) {
              logger.warning("Invalid rule " + (rule == null ? "" : rule.getOrDefault(RULE, "")) +
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
        throw new RuntimeException("Total Invalid Preprocessor rules detected :: " +
            totalInvalidRules);
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
