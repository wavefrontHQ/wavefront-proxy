package com.wavefront.agent.preprocessor;

import static com.wavefront.agent.preprocessor.PreprocessorUtil.*;
import static com.wavefront.common.Utils.csvToList;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import com.wavefront.common.TaggedMetricName;
import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.Counter;
import com.yammer.metrics.core.MetricName;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStream;
import java.util.*;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.annotation.Nonnull;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.yaml.snakeyaml.Yaml;

/**
 * Parses preprocessor rules (organized by listening port)
 *
 * <p>Created by Vasily on 9/15/16.
 */
public class PreprocessorConfigManager {
  public static final String NAMES = "names";
  public static final String FUNC = "function";
  public static final String OPTS = "opts";
  private static final Logger logger =
      LoggerFactory.getLogger(PreprocessorConfigManager.class.getCanonicalName());
  private static final Counter configReloads =
      Metrics.newCounter(new MetricName("preprocessor", "", "config-reloads.successful"));
  private static final Counter failedConfigReloads =
      Metrics.newCounter(new MetricName("preprocessor", "", "config-reloads.failed"));
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

  // rule type keywords: altering, filtering, and count
  public static final String POINT_ALTER = "pointAltering";
  public static final String POINT_FILTER = "pointFiltering";
  public static final String POINT_COUNT = "pointCount";
  public static final String SPAN_ALTER = "spanAltering";
  public static final String SPAN_FILTER = "spanFiltering";
  public static final String SPAN_COUNT = "spanCount";
  public static final String LOG_ALTER = "logAltering";
  public static final String LOG_FILTER = "logFiltering";
  public static final String LOG_COUNT = "logCount";

  private final Supplier<Long> timeSupplier;
  private final Map<Integer, ReportableEntityPreprocessor> systemPreprocessors = new HashMap<>();
  private final Map<Integer, MetricsFilter> lockMetricsFilter = new WeakHashMap<>();
  @VisibleForTesting public Map<Integer, ReportableEntityPreprocessor> userPreprocessors;
  @VisibleForTesting int totalInvalidRules = 0;
  @VisibleForTesting int totalValidRules = 0;
  private Map<Integer, ReportableEntityPreprocessor> preprocessors = null;
  private volatile long systemPreprocessorsTs = Long.MIN_VALUE;
  private volatile long userPreprocessorsTs;
  private volatile long lastBuild = Long.MIN_VALUE;
  private String lastProcessedRules = "";
  private static Map<String, Object> ruleNode = new HashMap<>();

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
   * @param fileName Path name of the file to be monitored.
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

  public ReportableEntityPreprocessor getSystemPreprocessor(Integer key) {
    systemPreprocessorsTs = timeSupplier.get();
    return systemPreprocessors.computeIfAbsent(key, x -> new ReportableEntityPreprocessor());
  }

  public Supplier<ReportableEntityPreprocessor> get(int port) {
    return () -> getPreprocessor(port);
  }

  private ReportableEntityPreprocessor getPreprocessor(int port) {
    if ((lastBuild < userPreprocessorsTs || lastBuild < systemPreprocessorsTs)
        && userPreprocessors != null) {
      synchronized (this) {
        if ((lastBuild < userPreprocessorsTs || lastBuild < systemPreprocessorsTs)
            && userPreprocessors != null) {
          this.preprocessors =
              Stream.of(this.systemPreprocessors, this.userPreprocessors)
                  .flatMap(x -> x.entrySet().stream())
                  .collect(
                      Collectors.toMap(
                          Map.Entry::getKey,
                          Map.Entry::getValue,
                          ReportableEntityPreprocessor::merge));
          this.lastBuild = timeSupplier.get();
        }
      }
    }
    return this.preprocessors.computeIfAbsent(port, x -> new ReportableEntityPreprocessor());
  }

  private void requireArguments(@Nonnull Map<String, Object> rule, String... arguments) {
    if (rule.isEmpty()) throw new IllegalArgumentException("Rule is empty");
    for (String argument : arguments) {
      if (rule.get(argument) == null
          || ((rule.get(argument) instanceof String)
              && ((String) rule.get(argument)).replaceAll("[^a-z0-9_-]", "").isEmpty()))
        throw new IllegalArgumentException("'" + argument + "' is missing or empty");
    }
  }

  private void allowArguments(@Nonnull Map<String, Object> rule, String... arguments) {
    Sets.SetView<String> invalidArguments =
        Sets.difference(
            rule.keySet(), Sets.union(ALLOWED_RULE_ARGUMENTS, Sets.newHashSet(arguments)));
    if (invalidArguments.size() > 0) {
      throw new IllegalArgumentException(
          "Invalid or not applicable argument(s): " + StringUtils.join(invalidArguments, ","));
    }
  }

  @VisibleForTesting
  void loadFileIfModified(String fileName) {
    try {
      File file = new File(fileName);
      long lastModified = file.lastModified();
      if (lastModified > userPreprocessorsTs) {
        logger.info("File " + file + " has been modified on disk, reloading preprocessor rules");
        loadFile(fileName);
        configReloads.inc();
      }
    } catch (Exception e) {
      logger.error("Unable to load preprocessor rules", e);
      failedConfigReloads.inc();
    }
  }

  public void loadFile(String filename) throws FileNotFoundException {
    File file = new File(filename);
    loadFromStream(new FileInputStream(file));
    ruleNode.put("path", file.getAbsolutePath());
  }

  @VisibleForTesting
  void loadFromStream(InputStream stream) {
    totalValidRules = 0;
    totalInvalidRules = 0;
    Yaml yaml = new Yaml();
    Map<Integer, ReportableEntityPreprocessor> portMap = new HashMap<>();
    lockMetricsFilter.clear();
    try {
      Map<String, Object> rulesByPort = yaml.load(stream);
      List<Map<String, Object>> validRulesList = new ArrayList<>();
      if (rulesByPort == null || rulesByPort.isEmpty()) {
        logger.warn("Empty preprocessor rule file detected!");
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

        List<Integer> ports =
            strPortKey.equalsIgnoreCase(GLOBAL_PORT_KEY)
                ? new ArrayList<>(portMap.keySet())
                : csvToList(strPortKey);

        for (int port : ports) {
          portMap.putIfAbsent(port, new ReportableEntityPreprocessor());
          int validRules = 0;
          //noinspection unchecked
          List<Map<String, Object>> rules = (List<Map<String, Object>>) rulesByPort.get(strPortKey);
          for (Map<String, Object> rule : rules) {
            try {
              requireArguments(rule, RULE, ACTION);
              allowArguments(
                  rule,
                  SCOPE,
                  SEARCH,
                  REPLACE,
                  MATCH,
                  TAG,
                  KEY,
                  NEWTAG,
                  NEWKEY,
                  VALUE,
                  SOURCE,
                  INPUT,
                  ITERATIONS,
                  REPLACE_SOURCE,
                  REPLACE_INPUT,
                  ACTION_SUBTYPE,
                  MAX_LENGTH,
                  FIRST_MATCH_ONLY,
                  ALLOW,
                  IF,
                  NAMES,
                  FUNC,
                  OPTS);
              String ruleName =
                  Objects.requireNonNull(getString(rule, RULE)).replaceAll("[^a-z0-9_-]", "");
              PreprocessorRuleMetrics ruleMetrics =
                  new PreprocessorRuleMetrics(
                      Metrics.newCounter(
                          new TaggedMetricName(
                              "preprocessor." + ruleName, "count", "port", String.valueOf(port))),
                      Metrics.newCounter(
                          new TaggedMetricName(
                              "preprocessor." + ruleName,
                              "cpu_nanos",
                              "port",
                              String.valueOf(port))),
                      Metrics.newCounter(
                          new TaggedMetricName(
                              "preprocessor." + ruleName,
                              "checked-count",
                              "port",
                              String.valueOf(port))));
              Map<String, Object> saveRule = new HashMap<>();
              saveRule.put("port", port);
              String scope = getString(rule, SCOPE);
              if ("pointLine".equals(scope) || "inputText".equals(scope)) {
                if (Predicates.getPredicate(rule) != null) {
                  throw new IllegalArgumentException(
                      "Argument [if] is not " + "allowed in [scope] = " + scope);
                }
                switch (Objects.requireNonNull(getString(rule, ACTION))) {
                  case "replaceRegex":
                    allowArguments(rule, SCOPE, SEARCH, REPLACE, MATCH, ITERATIONS);
                    portMap
                        .get(port)
                        .forPointLine()
                        .addTransformer(
                            new LineBasedReplaceRegexTransformer(
                                getString(rule, SEARCH),
                                getString(rule, REPLACE),
                                getString(rule, MATCH),
                                getInteger(rule, ITERATIONS, 1),
                                ruleMetrics));
                    saveRule.put("type", POINT_ALTER);
                    break;
                  case "blacklistRegex":
                  case "block":
                    allowArguments(rule, SCOPE, MATCH);
                    portMap
                        .get(port)
                        .forPointLine()
                        .addFilter(new LineBasedBlockFilter(getString(rule, MATCH), ruleMetrics));
                    saveRule.put("type", POINT_FILTER);
                    break;
                  case "whitelistRegex":
                  case "allow":
                    allowArguments(rule, SCOPE, MATCH);
                    portMap
                        .get(port)
                        .forPointLine()
                        .addFilter(new LineBasedAllowFilter(getString(rule, MATCH), ruleMetrics));
                    saveRule.put("type", POINT_FILTER);
                    break;
                  default:
                    throw new IllegalArgumentException(
                        "Action '"
                            + getString(rule, ACTION)
                            + "' is not valid or cannot be applied to pointLine");
                }
              } else {
                String action = Objects.requireNonNull(getString(rule, ACTION));
                switch (action) {
                  case "metricsFilter":
                    lockMetricsFilter.computeIfPresent(
                        port,
                        (s, metricsFilter) -> {
                          throw new IllegalArgumentException(
                              "Only one 'MetricsFilter' is allow per port");
                        });
                    allowArguments(rule, NAMES, FUNC, OPTS);
                    MetricsFilter mf = new MetricsFilter(rule, ruleMetrics, ruleName, port);
                    lockMetricsFilter.put(port, mf);
                    portMap.get(port).forPointLine().addFilter(mf);
                    saveRule.put("type", POINT_FILTER);
                    break;

                  case "replaceRegex":
                    allowArguments(rule, SCOPE, SEARCH, REPLACE, MATCH, ITERATIONS, IF);
                    portMap
                        .get(port)
                        .forReportPoint()
                        .addTransformer(
                            new ReportPointReplaceRegexTransformer(
                                scope,
                                getString(rule, SEARCH),
                                getString(rule, REPLACE),
                                getString(rule, MATCH),
                                getInteger(rule, ITERATIONS, 1),
                                Predicates.getPredicate(rule),
                                ruleMetrics));
                    saveRule.put("type", POINT_ALTER);
                    break;
                  case "forceLowercase":
                    allowArguments(rule, SCOPE, MATCH, IF);
                    portMap
                        .get(port)
                        .forReportPoint()
                        .addTransformer(
                            new ReportPointForceLowercaseTransformer(
                                scope,
                                getString(rule, MATCH),
                                Predicates.getPredicate(rule),
                                ruleMetrics));
                    saveRule.put("type", POINT_ALTER);
                    break;
                  case "addTag":
                    allowArguments(rule, TAG, VALUE, IF);
                    portMap
                        .get(port)
                        .forReportPoint()
                        .addTransformer(
                            new ReportPointAddTagTransformer(
                                getString(rule, TAG),
                                getString(rule, VALUE),
                                Predicates.getPredicate(rule),
                                ruleMetrics));
                    saveRule.put("type", POINT_ALTER);
                    break;
                  case "addTagIfNotExists":
                    allowArguments(rule, TAG, VALUE, IF);
                    portMap
                        .get(port)
                        .forReportPoint()
                        .addTransformer(
                            new ReportPointAddTagIfNotExistsTransformer(
                                getString(rule, TAG),
                                getString(rule, VALUE),
                                Predicates.getPredicate(rule),
                                ruleMetrics));
                    saveRule.put("type", POINT_ALTER);
                    break;
                  case "dropTag":
                    allowArguments(rule, TAG, MATCH, IF);
                    portMap
                        .get(port)
                        .forReportPoint()
                        .addTransformer(
                            new ReportPointDropTagTransformer(
                                getString(rule, TAG),
                                getString(rule, MATCH),
                                Predicates.getPredicate(rule),
                                ruleMetrics));
                    saveRule.put("type", POINT_ALTER);
                    break;
                  case "extractTag":
                    allowArguments(
                        rule,
                        TAG,
                        "source",
                        SEARCH,
                        REPLACE,
                        REPLACE_SOURCE,
                        REPLACE_INPUT,
                        MATCH,
                        IF);
                    portMap
                        .get(port)
                        .forReportPoint()
                        .addTransformer(
                            new ReportPointExtractTagTransformer(
                                getString(rule, TAG),
                                getString(rule, "source"),
                                getString(rule, SEARCH),
                                getString(rule, REPLACE),
                                (String) rule.getOrDefault(REPLACE_INPUT, rule.get(REPLACE_SOURCE)),
                                getString(rule, MATCH),
                                Predicates.getPredicate(rule),
                                ruleMetrics));
                    saveRule.put("type", POINT_ALTER);
                    break;
                  case "extractTagIfNotExists":
                    allowArguments(
                        rule,
                        TAG,
                        "source",
                        SEARCH,
                        REPLACE,
                        REPLACE_SOURCE,
                        REPLACE_INPUT,
                        MATCH,
                        IF);
                    portMap
                        .get(port)
                        .forReportPoint()
                        .addTransformer(
                            new ReportPointExtractTagIfNotExistsTransformer(
                                getString(rule, TAG),
                                getString(rule, "source"),
                                getString(rule, SEARCH),
                                getString(rule, REPLACE),
                                (String) rule.getOrDefault(REPLACE_INPUT, rule.get(REPLACE_SOURCE)),
                                getString(rule, MATCH),
                                Predicates.getPredicate(rule),
                                ruleMetrics));
                    saveRule.put("type", POINT_ALTER);
                    break;
                  case "renameTag":
                    allowArguments(rule, TAG, NEWTAG, MATCH, IF);
                    portMap
                        .get(port)
                        .forReportPoint()
                        .addTransformer(
                            new ReportPointRenameTagTransformer(
                                getString(rule, TAG),
                                getString(rule, NEWTAG),
                                getString(rule, MATCH),
                                Predicates.getPredicate(rule),
                                ruleMetrics));
                    saveRule.put("type", POINT_ALTER);
                    break;
                  case "limitLength":
                    allowArguments(rule, SCOPE, ACTION_SUBTYPE, MAX_LENGTH, MATCH, IF);
                    portMap
                        .get(port)
                        .forReportPoint()
                        .addTransformer(
                            new ReportPointLimitLengthTransformer(
                                Objects.requireNonNull(scope),
                                getInteger(rule, MAX_LENGTH, 0),
                                LengthLimitActionType.fromString(getString(rule, ACTION_SUBTYPE)),
                                getString(rule, MATCH),
                                Predicates.getPredicate(rule),
                                ruleMetrics));
                    saveRule.put("type", POINT_ALTER);
                    break;
                  case "count":
                    allowArguments(rule, SCOPE, IF);
                    portMap
                        .get(port)
                        .forReportPoint()
                        .addTransformer(
                            new CountTransformer<>(Predicates.getPredicate(rule), ruleMetrics));
                    saveRule.put("type", POINT_COUNT);
                    break;
                  case "blacklistRegex":
                    logger.warn(
                        "Preprocessor rule using deprecated syntax (action: "
                            + action
                            + "), use 'action: block' instead!");
                  case "block":
                    allowArguments(rule, SCOPE, MATCH, IF);
                    portMap
                        .get(port)
                        .forReportPoint()
                        .addFilter(
                            new ReportPointBlockFilter(
                                scope,
                                getString(rule, MATCH),
                                Predicates.getPredicate(rule),
                                ruleMetrics));
                    saveRule.put("type", POINT_FILTER);
                    break;
                  case "whitelistRegex":
                    logger.warn(
                        "Preprocessor rule using deprecated syntax (action: "
                            + action
                            + "), use 'action: allow' instead!");
                  case "allow":
                    allowArguments(rule, SCOPE, MATCH, IF);
                    portMap
                        .get(port)
                        .forReportPoint()
                        .addFilter(
                            new ReportPointAllowFilter(
                                scope,
                                getString(rule, MATCH),
                                Predicates.getPredicate(rule),
                                ruleMetrics));
                    saveRule.put("type", POINT_FILTER);
                    break;

                    // Rules for Span objects
                  case "spanReplaceRegex":
                    allowArguments(
                        rule, SCOPE, SEARCH, REPLACE, MATCH, ITERATIONS, FIRST_MATCH_ONLY, IF);
                    portMap
                        .get(port)
                        .forSpan()
                        .addTransformer(
                            new SpanReplaceRegexTransformer(
                                scope,
                                getString(rule, SEARCH),
                                getString(rule, REPLACE),
                                getString(rule, MATCH),
                                getInteger(rule, ITERATIONS, 1),
                                getBoolean(rule, FIRST_MATCH_ONLY, false),
                                Predicates.getPredicate(rule),
                                ruleMetrics));
                    saveRule.put("type", SPAN_ALTER);
                    break;
                  case "spanForceLowercase":
                    allowArguments(rule, SCOPE, MATCH, FIRST_MATCH_ONLY, IF);
                    portMap
                        .get(port)
                        .forSpan()
                        .addTransformer(
                            new SpanForceLowercaseTransformer(
                                scope,
                                getString(rule, MATCH),
                                getBoolean(rule, FIRST_MATCH_ONLY, false),
                                Predicates.getPredicate(rule),
                                ruleMetrics));
                    saveRule.put("type", SPAN_ALTER);
                    break;
                  case "spanAddAnnotation":
                  case "spanAddTag":
                    allowArguments(rule, KEY, VALUE, IF);
                    portMap
                        .get(port)
                        .forSpan()
                        .addTransformer(
                            new SpanAddAnnotationTransformer(
                                getString(rule, KEY),
                                getString(rule, VALUE),
                                Predicates.getPredicate(rule),
                                ruleMetrics));
                    saveRule.put("type", SPAN_ALTER);
                    break;
                  case "spanAddAnnotationIfNotExists":
                  case "spanAddTagIfNotExists":
                    allowArguments(rule, KEY, VALUE, IF);
                    portMap
                        .get(port)
                        .forSpan()
                        .addTransformer(
                            new SpanAddAnnotationIfNotExistsTransformer(
                                getString(rule, KEY),
                                getString(rule, VALUE),
                                Predicates.getPredicate(rule),
                                ruleMetrics));
                    saveRule.put("type", SPAN_ALTER);
                    break;
                  case "spanDropAnnotation":
                  case "spanDropTag":
                    allowArguments(rule, KEY, MATCH, FIRST_MATCH_ONLY, IF);
                    portMap
                        .get(port)
                        .forSpan()
                        .addTransformer(
                            new SpanDropAnnotationTransformer(
                                getString(rule, KEY),
                                getString(rule, MATCH),
                                getBoolean(rule, FIRST_MATCH_ONLY, false),
                                Predicates.getPredicate(rule),
                                ruleMetrics));
                    saveRule.put("type", SPAN_ALTER);
                    break;
                  case "spanWhitelistAnnotation":
                  case "spanWhitelistTag":
                    logger.warn(
                        "Preprocessor rule using deprecated syntax (action: "
                            + action
                            + "), use 'action: spanAllowAnnotation' instead!");
                  case "spanAllowAnnotation":
                  case "spanAllowTag":
                    allowArguments(rule, ALLOW, IF);
                    portMap
                        .get(port)
                        .forSpan()
                        .addTransformer(
                            SpanAllowAnnotationTransformer.create(
                                rule, Predicates.getPredicate(rule), ruleMetrics));
                    saveRule.put("type", SPAN_FILTER);
                    break;
                  case "spanExtractAnnotation":
                  case "spanExtractTag":
                    allowArguments(
                        rule,
                        KEY,
                        INPUT,
                        SEARCH,
                        REPLACE,
                        REPLACE_INPUT,
                        MATCH,
                        FIRST_MATCH_ONLY,
                        IF);
                    portMap
                        .get(port)
                        .forSpan()
                        .addTransformer(
                            new SpanExtractAnnotationTransformer(
                                getString(rule, KEY),
                                getString(rule, INPUT),
                                getString(rule, SEARCH),
                                getString(rule, REPLACE),
                                getString(rule, REPLACE_INPUT),
                                getString(rule, MATCH),
                                getBoolean(rule, FIRST_MATCH_ONLY, false),
                                Predicates.getPredicate(rule),
                                ruleMetrics));
                    saveRule.put("type", SPAN_ALTER);
                    break;
                  case "spanExtractAnnotationIfNotExists":
                  case "spanExtractTagIfNotExists":
                    allowArguments(
                        rule,
                        KEY,
                        INPUT,
                        SEARCH,
                        REPLACE,
                        REPLACE_INPUT,
                        MATCH,
                        FIRST_MATCH_ONLY,
                        IF);
                    portMap
                        .get(port)
                        .forSpan()
                        .addTransformer(
                            new SpanExtractAnnotationIfNotExistsTransformer(
                                getString(rule, KEY),
                                getString(rule, INPUT),
                                getString(rule, SEARCH),
                                getString(rule, REPLACE),
                                getString(rule, REPLACE_INPUT),
                                getString(rule, MATCH),
                                getBoolean(rule, FIRST_MATCH_ONLY, false),
                                Predicates.getPredicate(rule),
                                ruleMetrics));
                    saveRule.put("type", SPAN_ALTER);
                    break;
                  case "spanRenameAnnotation":
                  case "spanRenameTag":
                    allowArguments(rule, KEY, NEWKEY, MATCH, FIRST_MATCH_ONLY, IF);
                    portMap
                        .get(port)
                        .forSpan()
                        .addTransformer(
                            new SpanRenameAnnotationTransformer(
                                getString(rule, KEY), getString(rule, NEWKEY),
                                getString(rule, MATCH), getBoolean(rule, FIRST_MATCH_ONLY, false),
                                Predicates.getPredicate(rule), ruleMetrics));
                    saveRule.put("type", SPAN_ALTER);
                    break;
                  case "spanLimitLength":
                    allowArguments(
                        rule, SCOPE, ACTION_SUBTYPE, MAX_LENGTH, MATCH, FIRST_MATCH_ONLY, IF);
                    portMap
                        .get(port)
                        .forSpan()
                        .addTransformer(
                            new SpanLimitLengthTransformer(
                                Objects.requireNonNull(scope),
                                getInteger(rule, MAX_LENGTH, 0),
                                LengthLimitActionType.fromString(getString(rule, ACTION_SUBTYPE)),
                                getString(rule, MATCH),
                                getBoolean(rule, FIRST_MATCH_ONLY, false),
                                Predicates.getPredicate(rule),
                                ruleMetrics));
                    saveRule.put("type", SPAN_ALTER);
                    break;
                  case "spanCount":
                    allowArguments(rule, SCOPE, IF);
                    portMap
                        .get(port)
                        .forSpan()
                        .addTransformer(
                            new CountTransformer<>(Predicates.getPredicate(rule), ruleMetrics));
                    saveRule.put("type", SPAN_COUNT);
                    break;
                  case "spanBlacklistRegex":
                    logger.warn(
                        "Preprocessor rule using deprecated syntax (action: "
                            + action
                            + "), use 'action: spanBlock' instead!");
                  case "spanBlock":
                    allowArguments(rule, SCOPE, MATCH, IF);
                    portMap
                        .get(port)
                        .forSpan()
                        .addFilter(
                            new SpanBlockFilter(
                                scope,
                                getString(rule, MATCH),
                                Predicates.getPredicate(rule),
                                ruleMetrics));
                    saveRule.put("type", SPAN_FILTER);
                    break;
                  case "spanWhitelistRegex":
                    logger.warn(
                        "Preprocessor rule using deprecated syntax (action: "
                            + action
                            + "), use 'action: spanAllow' instead!");
                  case "spanAllow":
                    allowArguments(rule, SCOPE, MATCH, IF);
                    portMap
                        .get(port)
                        .forSpan()
                        .addFilter(
                            new SpanAllowFilter(
                                scope,
                                getString(rule, MATCH),
                                Predicates.getPredicate(rule),
                                ruleMetrics));
                    saveRule.put("type", SPAN_FILTER);
                    break;

                    // Rules for Log objects
                  case "logReplaceRegex":
                    allowArguments(rule, SCOPE, SEARCH, REPLACE, MATCH, ITERATIONS, IF);
                    portMap
                        .get(port)
                        .forReportLog()
                        .addTransformer(
                            new ReportLogReplaceRegexTransformer(
                                scope,
                                getString(rule, SEARCH),
                                getString(rule, REPLACE),
                                getString(rule, MATCH),
                                getInteger(rule, ITERATIONS, 1),
                                Predicates.getPredicate(rule),
                                ruleMetrics));
                    saveRule.put("type", LOG_ALTER);
                    break;
                  case "logForceLowercase":
                    allowArguments(rule, SCOPE, MATCH, IF);
                    portMap
                        .get(port)
                        .forReportLog()
                        .addTransformer(
                            new ReportLogForceLowercaseTransformer(
                                scope,
                                getString(rule, MATCH),
                                Predicates.getPredicate(rule),
                                ruleMetrics));
                    saveRule.put("type", LOG_ALTER);
                    break;
                  case "logAddAnnotation":
                  case "logAddTag":
                    allowArguments(rule, KEY, VALUE, IF);
                    portMap
                        .get(port)
                        .forReportLog()
                        .addTransformer(
                            new ReportLogAddTagTransformer(
                                getString(rule, KEY),
                                getString(rule, VALUE),
                                Predicates.getPredicate(rule),
                                ruleMetrics));
                    saveRule.put("type", LOG_ALTER);
                    break;
                  case "logAddAnnotationIfNotExists":
                  case "logAddTagIfNotExists":
                    allowArguments(rule, KEY, VALUE, IF);
                    portMap
                        .get(port)
                        .forReportLog()
                        .addTransformer(
                            new ReportLogAddTagIfNotExistsTransformer(
                                getString(rule, KEY),
                                getString(rule, VALUE),
                                Predicates.getPredicate(rule),
                                ruleMetrics));
                    saveRule.put("type", LOG_ALTER);
                    break;
                  case "logDropAnnotation":
                  case "logDropTag":
                    allowArguments(rule, KEY, MATCH, IF);
                    portMap
                        .get(port)
                        .forReportLog()
                        .addTransformer(
                            new ReportLogDropTagTransformer(
                                getString(rule, KEY),
                                getString(rule, MATCH),
                                Predicates.getPredicate(rule),
                                ruleMetrics));
                    saveRule.put("type", LOG_ALTER);
                    break;
                  case "logAllowAnnotation":
                  case "logAllowTag":
                    allowArguments(rule, ALLOW, IF);
                    portMap
                        .get(port)
                        .forReportLog()
                        .addTransformer(
                            ReportLogAllowTagTransformer.create(
                                rule, Predicates.getPredicate(rule), ruleMetrics));
                    saveRule.put("type", LOG_FILTER);
                    break;
                  case "logExtractAnnotation":
                  case "logExtractTag":
                    allowArguments(rule, KEY, INPUT, SEARCH, REPLACE, REPLACE_INPUT, MATCH, IF);
                    portMap
                        .get(port)
                        .forReportLog()
                        .addTransformer(
                            new ReportLogExtractTagTransformer(
                                getString(rule, KEY),
                                getString(rule, INPUT),
                                getString(rule, SEARCH),
                                getString(rule, REPLACE),
                                getString(rule, REPLACE_INPUT),
                                getString(rule, MATCH),
                                Predicates.getPredicate(rule),
                                ruleMetrics));
                    saveRule.put("type", LOG_ALTER);
                    break;
                  case "logExtractAnnotationIfNotExists":
                  case "logExtractTagIfNotExists":
                    allowArguments(rule, KEY, INPUT, SEARCH, REPLACE, REPLACE_INPUT, MATCH, IF);
                    portMap
                        .get(port)
                        .forReportLog()
                        .addTransformer(
                            new ReportLogExtractTagIfNotExistsTransformer(
                                getString(rule, KEY),
                                getString(rule, INPUT),
                                getString(rule, SEARCH),
                                getString(rule, REPLACE),
                                getString(rule, REPLACE_INPUT),
                                getString(rule, MATCH),
                                Predicates.getPredicate(rule),
                                ruleMetrics));
                    saveRule.put("type", LOG_ALTER);
                    break;
                  case "logRenameAnnotation":
                  case "logRenameTag":
                    allowArguments(rule, KEY, NEWKEY, MATCH, IF);
                    portMap
                        .get(port)
                        .forReportLog()
                        .addTransformer(
                            new ReportLogRenameTagTransformer(
                                getString(rule, KEY),
                                getString(rule, NEWKEY),
                                getString(rule, MATCH),
                                Predicates.getPredicate(rule),
                                ruleMetrics));
                    saveRule.put("type", LOG_ALTER);
                    break;
                  case "logLimitLength":
                    allowArguments(rule, SCOPE, ACTION_SUBTYPE, MAX_LENGTH, MATCH, IF);
                    portMap
                        .get(port)
                        .forReportLog()
                        .addTransformer(
                            new ReportLogLimitLengthTransformer(
                                Objects.requireNonNull(scope),
                                getInteger(rule, MAX_LENGTH, 0),
                                LengthLimitActionType.fromString(getString(rule, ACTION_SUBTYPE)),
                                getString(rule, MATCH),
                                Predicates.getPredicate(rule),
                                ruleMetrics));
                    saveRule.put("type", LOG_ALTER);
                    break;
                  case "logCount":
                    allowArguments(rule, SCOPE, IF);
                    portMap
                        .get(port)
                        .forReportLog()
                        .addTransformer(
                            new CountTransformer<>(Predicates.getPredicate(rule), ruleMetrics));
                    saveRule.put("type", LOG_COUNT);
                    break;

                  case "logBlacklistRegex":
                    logger.warn(
                        "Preprocessor rule using deprecated syntax (action: "
                            + action
                            + "), use 'action: logBlock' instead!");
                  case "logBlock":
                    allowArguments(rule, SCOPE, MATCH, IF);
                    portMap
                        .get(port)
                        .forReportLog()
                        .addFilter(
                            new ReportLogBlockFilter(
                                scope,
                                getString(rule, MATCH),
                                Predicates.getPredicate(rule),
                                ruleMetrics));
                    saveRule.put("type", LOG_FILTER);
                    break;
                  case "logWhitelistRegex":
                    logger.warn(
                        "Preprocessor rule using deprecated syntax (action: "
                            + action
                            + "), use 'action: spanAllow' instead!");
                  case "logAllow":
                    allowArguments(rule, SCOPE, MATCH, IF);
                    portMap
                        .get(port)
                        .forReportLog()
                        .addFilter(
                            new ReportLogAllowFilter(
                                scope,
                                getString(rule, MATCH),
                                Predicates.getPredicate(rule),
                                ruleMetrics));
                    saveRule.put("type", LOG_FILTER);
                    break;

                  default:
                    throw new IllegalArgumentException(
                        "Action '" + getString(rule, ACTION) + "' is not valid");
                }
              }
              validRules++;
              // MONIT-30818: Add rule to validRulesList for FE preprocessor rules
              saveRule.putAll(rule);
              validRulesList.add(saveRule);
            } catch (IllegalArgumentException | NullPointerException ex) {
              logger.warn(
                  "Invalid rule "
                      + (rule == null ? "" : rule.getOrDefault(RULE, ""))
                      + " (port "
                      + port
                      + "): "
                      + ex);
              totalInvalidRules++;
            }
          }
          logger.info("Loaded " + validRules + " rules for port :: " + port);
          totalValidRules += validRules;
        }
        logger.info("Loaded Preprocessor rules for port key :: \"" + strPortKey + "\"");
      }
      ruleNode.put("rules", validRulesList);
      logger.info("Total Preprocessor rules loaded :: " + totalValidRules);
      if (totalInvalidRules > 0) {
        throw new RuntimeException(
            "Total Invalid Preprocessor rules detected :: " + totalInvalidRules);
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

  public static JsonNode getJsonRules() {
    ObjectMapper mapper = new ObjectMapper();
    JsonNode node = mapper.valueToTree(ruleNode);
    return node;
  }
}
