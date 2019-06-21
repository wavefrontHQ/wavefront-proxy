package com.wavefront.agent.preprocessor;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Sets;

import com.wavefront.common.TaggedMetricName;
import com.yammer.metrics.Metrics;

import org.apache.commons.lang.StringUtils;
import org.yaml.snakeyaml.Yaml;

import java.io.InputStream;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;

import javax.annotation.Nonnull;

/**
 * Parses and stores all preprocessor rules (organized by listening port)
 *
 * Created by Vasily on 9/15/16.
 */
public class AgentPreprocessorConfiguration {

  private static final Logger logger = Logger.getLogger(AgentPreprocessorConfiguration.class.getCanonicalName());

  private final Map<String, ReportableEntityPreprocessor> portMap = new HashMap<>();

  @VisibleForTesting
  int totalInvalidRules = 0;
  @VisibleForTesting
  int totalValidRules = 0;

  public ReportableEntityPreprocessor forPort(final String strPort) {
    ReportableEntityPreprocessor preprocessor = portMap.get(strPort);
    if (preprocessor == null) {
      preprocessor = new ReportableEntityPreprocessor();
      portMap.put(strPort, preprocessor);
    }
    return preprocessor;
  }

  private void requireArguments(@Nonnull Map<String, String> rule, String... arguments) {
    if (rule == null)
      throw new IllegalArgumentException("Rule is empty");
    for (String argument : arguments) {
      if (rule.get(argument) == null || rule.get(argument).replaceAll("[^a-z0-9_-]", "").isEmpty())
        throw new IllegalArgumentException("'" + argument + "' is missing or empty");
    }
  }

  private void allowArguments(@Nonnull Map<String, String> rule, String... arguments) {
    Sets.SetView<String> invalidArguments = Sets.difference(rule.keySet(), Sets.newHashSet(arguments));
    if (invalidArguments.size() > 0) {
      throw new IllegalArgumentException("Invalid or not applicable argument(s): " +
          StringUtils.join(invalidArguments, ","));
    }
  }

  public void loadFromStream(InputStream stream) {
    totalValidRules = 0;
    totalInvalidRules = 0;
    Yaml yaml = new Yaml();
    try {
      //noinspection unchecked
      Map<String, Object> rulesByPort = (Map<String, Object>) yaml.load(stream);
      if (rulesByPort == null) {
        logger.warning("Empty preprocessor rule file detected!");
        logger.info("Total 0 rules loaded");
        return;
      }
      for (String strPort : rulesByPort.keySet()) {
        int validRules = 0;
        //noinspection unchecked
        List<Map<String, String>> rules = (List<Map<String, String>>) rulesByPort.get(strPort);
        for (Map<String, String> rule : rules) {
          try {
            requireArguments(rule, "rule", "action");
            allowArguments(rule, "rule", "action", "scope", "search", "replace", "match", "tag", "key", "newtag",
                "value", "source", "input", "iterations", "replaceSource", "replaceInput", "actionSubtype", "maxLength",
                "firstMatchOnly");
            String ruleName = rule.get("rule").replaceAll("[^a-z0-9_-]", "");
            PreprocessorRuleMetrics ruleMetrics = new PreprocessorRuleMetrics(
                Metrics.newCounter(new TaggedMetricName("preprocessor." + ruleName, "count", "port", strPort)),
                Metrics.newCounter(new TaggedMetricName("preprocessor." + ruleName, "cpu_nanos", "port", strPort)),
                Metrics.newCounter(new TaggedMetricName("preprocessor." + ruleName, "checked-count", "port", strPort)));

            if (rule.get("scope") != null && rule.get("scope").equals("pointLine")) {
              switch (rule.get("action")) {
                case "replaceRegex":
                  allowArguments(rule, "rule", "action", "scope", "search", "replace", "match", "iterations");
                  this.forPort(strPort).forPointLine().addTransformer(
                      new PointLineReplaceRegexTransformer(rule.get("search"), rule.get("replace"), rule.get("match"),
                          Integer.parseInt(rule.getOrDefault("iterations", "1")), ruleMetrics));
                  break;
                case "blacklistRegex":
                  allowArguments(rule, "rule", "action", "scope", "match");
                  this.forPort(strPort).forPointLine().addFilter(
                      new PointLineBlacklistRegexFilter(rule.get("match"), ruleMetrics));
                  break;
                case "whitelistRegex":
                  allowArguments(rule, "rule", "action", "scope", "match");
                  this.forPort(strPort).forPointLine().addFilter(
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
                  allowArguments(rule, "rule", "action", "scope", "search", "replace", "match", "iterations");
                  this.forPort(strPort).forReportPoint().addTransformer(
                      new ReportPointReplaceRegexTransformer(rule.get("scope"), rule.get("search"), rule.get("replace"),
                          rule.get("match"), Integer.parseInt(rule.getOrDefault("iterations", "1")), ruleMetrics));
                  break;
                case "forceLowercase":
                  allowArguments(rule, "rule", "action", "scope", "match");
                  this.forPort(strPort).forReportPoint().addTransformer(
                      new ReportPointForceLowercaseTransformer(rule.get("scope"), rule.get("match"), ruleMetrics));
                  break;
                case "addTag":
                  allowArguments(rule, "rule", "action", "tag", "value");
                  this.forPort(strPort).forReportPoint().addTransformer(
                      new ReportPointAddTagTransformer(rule.get("tag"), rule.get("value"), ruleMetrics));
                  break;
                case "addTagIfNotExists":
                  allowArguments(rule, "rule", "action", "tag", "value");
                  this.forPort(strPort).forReportPoint().addTransformer(
                      new ReportPointAddTagIfNotExistsTransformer(rule.get("tag"), rule.get("value"), ruleMetrics));
                  break;
                case "dropTag":
                  allowArguments(rule, "rule", "action", "tag", "match");
                  this.forPort(strPort).forReportPoint().addTransformer(
                      new ReportPointDropTagTransformer(rule.get("tag"), rule.get("match"), ruleMetrics));
                  break;
                case "extractTag":
                  allowArguments(rule, "rule", "action", "tag", "source", "search", "replace", "replaceSource",
                      "replaceInput", "match");
                  this.forPort(strPort).forReportPoint().addTransformer(
                      new ReportPointExtractTagTransformer(rule.get("tag"), rule.get("source"), rule.get("search"),
                          rule.get("replace"), rule.getOrDefault("replaceInput", rule.get("replaceSource")),
                          rule.get("match"), ruleMetrics));
                  break;
                case "extractTagIfNotExists":
                  allowArguments(rule, "rule", "action", "tag", "source", "search", "replace", "replaceSource",
                      "replaceInput", "match");
                  this.forPort(strPort).forReportPoint().addTransformer(
                      new ReportPointExtractTagIfNotExistsTransformer(rule.get("tag"), rule.get("source"),
                          rule.get("search"), rule.get("replace"), rule.getOrDefault("replaceInput",
                          rule.get("replaceSource")), rule.get("match"), ruleMetrics));
                  break;
                case "renameTag":
                  allowArguments(rule, "rule", "action", "tag", "newtag", "match");
                  this.forPort(strPort).forReportPoint().addTransformer(
                      new ReportPointRenameTagTransformer(
                          rule.get("tag"), rule.get("newtag"), rule.get("match"), ruleMetrics));
                  break;
                case "limitLength":
                  allowArguments(rule, "rule", "action", "scope", "actionSubtype", "maxLength", "match");
                  this.forPort(strPort).forReportPoint().addTransformer(
                      new ReportPointLimitLengthTransformer(rule.get("scope"), Integer.parseInt(rule.get("maxLength")),
                  LengthLimitActionType.fromString(rule.get("actionSubtype")), rule.get("match"), ruleMetrics));
                  break;
                case "blacklistRegex":
                  allowArguments(rule, "rule", "action", "scope", "match");
                  this.forPort(strPort).forReportPoint().addFilter(
                      new ReportPointBlacklistRegexFilter(rule.get("scope"), rule.get("match"), ruleMetrics));
                  break;
                case "whitelistRegex":
                  allowArguments(rule, "rule", "action", "scope", "match");
                  this.forPort(strPort).forReportPoint().addFilter(
                      new ReportPointWhitelistRegexFilter(rule.get("scope"), rule.get("match"), ruleMetrics));
                  break;

                // Rules for Span objects
                case "spanReplaceRegex":
                  allowArguments(rule, "rule", "action", "scope", "search", "replace", "match", "iterations",
                      "firstMatchOnly");
                  this.forPort(strPort).forSpan().addTransformer(
                      new SpanReplaceRegexTransformer(rule.get("scope"), rule.get("search"), rule.get("replace"),
                          rule.get("match"), Integer.parseInt(rule.getOrDefault("iterations", "1")),
                          Boolean.parseBoolean(rule.getOrDefault("firstMatchOnly", "false")), ruleMetrics));
                  break;
                case "spanForceLowercase":
                  allowArguments(rule, "rule", "action", "scope", "match", "firstMatchOnly");
                  this.forPort(strPort).forSpan().addTransformer(
                      new SpanForceLowercaseTransformer(rule.get("scope"), rule.get("match"),
                          Boolean.parseBoolean(rule.getOrDefault("firstMatchOnly", "false")), ruleMetrics));
                  break;
                case "spanAddAnnotation":
                  allowArguments(rule, "rule", "action", "key", "value");
                  this.forPort(strPort).forSpan().addTransformer(
                      new SpanAddAnnotationTransformer(rule.get("key"), rule.get("value"), ruleMetrics));
                  break;
                case "spanAddAnnotationIfNotExists":
                  allowArguments(rule, "rule", "action", "key", "value");
                  this.forPort(strPort).forSpan().addTransformer(
                      new SpanAddAnnotationIfNotExistsTransformer(rule.get("key"), rule.get("value"), ruleMetrics));
                  break;
                case "spanDropAnnotation":
                  allowArguments(rule, "rule", "action", "key", "match", "firstMatchOnly");
                  this.forPort(strPort).forSpan().addTransformer(
                      new SpanDropAnnotationTransformer(rule.get("key"), rule.get("match"),
                          Boolean.parseBoolean(rule.getOrDefault("firstMatchOnly", "false")), ruleMetrics));
                  break;
                case "spanExtractAnnotation":
                  allowArguments(rule, "rule", "action", "key", "input", "search", "replace", "replaceInput", "match",
                      "firstMatchOnly");
                  this.forPort(strPort).forSpan().addTransformer(
                      new SpanExtractAnnotationTransformer(rule.get("key"), rule.get("input"), rule.get("search"),
                          rule.get("replace"), rule.get("replaceInput"), rule.get("match"),
                          Boolean.parseBoolean(rule.getOrDefault("firstMatchOnly", "false")), ruleMetrics));
                  break;
                case "spanExtractAnnotationIfNotExists":
                  allowArguments(rule, "rule", "action", "key", "input", "search", "replace", "replaceInput", "match",
                      "firstMatchOnly");
                  this.forPort(strPort).forSpan().addTransformer(
                      new SpanExtractAnnotationIfNotExistsTransformer(rule.get("key"), rule.get("input"),
                          rule.get("search"), rule.get("replace"), rule.get("replaceInput"), rule.get("match"),
                          Boolean.parseBoolean(rule.getOrDefault("firstMatchOnly", "false")), ruleMetrics));
                  break;
                case "spanLimitLength":
                  allowArguments(rule, "rule", "action", "scope", "actionSubtype", "maxLength", "match",
                      "firstMatchOnly");
                  this.forPort(strPort).forSpan().addTransformer(
                      new SpanLimitLengthTransformer(rule.get("scope"), Integer.parseInt(rule.get("maxLength")),
                          LengthLimitActionType.fromString(rule.get("actionSubtype")), rule.get("match"),
                          Boolean.parseBoolean(rule.getOrDefault("firstMatchOnly", "false")), ruleMetrics));
                  break;
                case "spanBlacklistRegex":
                  allowArguments(rule, "rule", "action", "scope", "match");
                  this.forPort(strPort).forSpan().addFilter(
                      new SpanBlacklistRegexFilter(rule.get("scope"), rule.get("match"), ruleMetrics));
                  break;
                case "spanWhitelistRegex":
                  allowArguments(rule, "rule", "action", "scope", "match");
                  this.forPort(strPort).forSpan().addFilter(
                      new SpanWhitelistRegexFilter(rule.get("scope"), rule.get("match"), ruleMetrics));
                  break;
                default:
                  throw new IllegalArgumentException("Action '" + rule.get("action") + "' is not valid");
              }
            }
            validRules++;
          } catch (IllegalArgumentException | NullPointerException ex) {
            logger.warning("Invalid rule " + (rule == null || rule.get("rule") == null ? "" : rule.get("rule")) +
                " (port " + strPort + "): " + ex);
            totalInvalidRules++;
          }
        }
        logger.info("Loaded " + validRules + " rules for port " + strPort);
        totalValidRules += validRules;
      }
      logger.info("Total " + totalValidRules + " rules loaded");
      if (totalInvalidRules > 0) {
        throw new RuntimeException("Total " + totalInvalidRules + " invalid rules detected, aborting start-up");
      }
    } catch (ClassCastException e) {
      throw new RuntimeException("Can't parse preprocessor configuration - aborting start-up");
    }
  }
}
