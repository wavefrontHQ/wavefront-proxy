package com.wavefront.agent.preprocessor;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Sets;

import com.wavefront.common.TaggedMetricName;
import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.Counter;

import org.apache.commons.lang.StringUtils;
import org.yaml.snakeyaml.Yaml;

import java.io.InputStream;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;

import javax.validation.constraints.NotNull;

/**
 * Parses and stores all preprocessor rules (organized by listening port)
 *
 * Created by Vasily on 9/15/16.
 */
public class AgentPreprocessorConfiguration {

  private static final Logger logger = Logger.getLogger(AgentPreprocessorConfiguration.class.getCanonicalName());

  private final Map<String, PointPreprocessor> portMap = new HashMap<>();

  @VisibleForTesting
  int totalInvalidRules = 0;
  @VisibleForTesting
  int totalValidRules = 0;

  public PointPreprocessor forPort(final String strPort) {
    PointPreprocessor preprocessor = portMap.get(strPort);
    if (preprocessor == null) {
      preprocessor = new PointPreprocessor();
      portMap.put(strPort, preprocessor);
    }
    return preprocessor;
  }

  private void requireArguments(@NotNull Map<String, String> rule, String... arguments) {
    if (rule == null)
      throw new IllegalArgumentException("Rule is empty");
    for (String argument : arguments) {
      if (rule.get(argument) == null || rule.get(argument).replaceAll("[^a-z0-9_-]", "").isEmpty())
        throw new IllegalArgumentException("'" + argument + "' is missing or empty");
    }
  }

  private void allowArguments(@NotNull Map<String, String> rule, String... arguments) {
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
      for (String strPort : rulesByPort.keySet()) {
        int validRules = 0;
        //noinspection unchecked
        List<Map<String, String>> rules = (List<Map<String, String>>) rulesByPort.get(strPort);
        for (Map<String, String> rule : rules) {
          try {
            requireArguments(rule, "rule", "action");
            allowArguments(rule, "rule", "action", "scope", "search", "replace", "match",
                "tag", "newtag", "value", "source");
            Counter counter = Metrics.newCounter(new TaggedMetricName("preprocessor." +
                rule.get("rule").replaceAll("[^a-z0-9_-]", ""), "count", "port", strPort));
            if (rule.get("scope") != null && rule.get("scope").equals("pointLine")) {
              switch (rule.get("action")) {
                case "replaceRegex":
                  allowArguments(rule, "rule", "action", "scope", "search", "replace", "match");
                  this.forPort(strPort).forPointLine().addTransformer(
                      new PointLineReplaceRegexTransformer(
                          rule.get("search"), rule.get("replace"), rule.get("match"), counter));
                  break;
                case "blacklistRegex":
                  allowArguments(rule, "rule", "action", "scope", "match");
                  this.forPort(strPort).forPointLine().addFilter(
                      new PointLineBlacklistRegexFilter(rule.get("match"), counter));
                  break;
                case "whitelistRegex":
                  allowArguments(rule, "rule", "action", "scope", "match");
                  this.forPort(strPort).forPointLine().addFilter(
                      new PointLineWhitelistRegexFilter(rule.get("match"), counter));
                  break;
                default:
                  throw new IllegalArgumentException("Action '" + rule.get("action") +
                      "' is not valid or cannot be applied to pointLine");
              }
            } else {
              switch (rule.get("action")) {
                case "replaceRegex":
                  allowArguments(rule, "rule", "action", "scope", "search", "replace", "match");
                  this.forPort(strPort).forReportPoint().addTransformer(
                      new ReportPointReplaceRegexTransformer(
                          rule.get("scope"), rule.get("search"), rule.get("replace"), rule.get("match"), counter));
                  break;
                case "addTag":
                  allowArguments(rule, "rule", "action", "tag", "value");
                  this.forPort(strPort).forReportPoint().addTransformer(
                      new ReportPointAddTagTransformer(rule.get("tag"), rule.get("value"), counter));
                  break;
                case "addTagIfNotExists":
                  allowArguments(rule, "rule", "action", "tag", "value");
                  this.forPort(strPort).forReportPoint().addTransformer(
                      new ReportPointAddTagIfNotExistsTransformer(rule.get("tag"), rule.get("value"), counter));
                  break;
                case "dropTag":
                  allowArguments(rule, "rule", "action", "tag", "match");
                  this.forPort(strPort).forReportPoint().addTransformer(
                      new ReportPointDropTagTransformer(rule.get("tag"), rule.get("match"), counter));
                  break;
                case "extractTag":
                  allowArguments(rule, "rule", "action", "tag", "source", "search", "replace", "match");
                  this.forPort(strPort).forReportPoint().addTransformer(
                      new ReportPointExtractTagTransformer(rule.get("tag"), rule.get("source"), rule.get("search"),
                          rule.get("replace"), rule.get("match"), counter));
                  break;
                case "renameTag":
                  allowArguments(rule, "rule", "action", "tag", "newtag", "match");
                  this.forPort(strPort).forReportPoint().addTransformer(
                      new ReportPointRenameTagTransformer(
                          rule.get("tag"), rule.get("newtag"), rule.get("match"), counter));
                  break;
                case "blacklistRegex":
                  allowArguments(rule, "rule", "action", "scope", "match");
                  this.forPort(strPort).forReportPoint().addFilter(
                      new ReportPointBlacklistRegexFilter(rule.get("scope"), rule.get("match"), counter));
                  break;
                case "whitelistRegex":
                  allowArguments(rule, "rule", "action", "scope", "match");
                  this.forPort(strPort).forReportPoint().addFilter(
                      new ReportPointWhitelistRegexFilter(rule.get("scope"), rule.get("match"), counter));
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
