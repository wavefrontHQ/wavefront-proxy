package com.wavefront.agent.preprocessor;

import com.wavefront.common.TaggedMetricName;
import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.Counter;

import org.yaml.snakeyaml.Yaml;

import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.logging.Logger;

/**
 * Parses and stores all preprocessor rules (organized by listening port)
 *
 * Created by Vasily on 9/15/16.
 */
public class AgentPreprocessorConfiguration {

  private static final Logger logger = Logger.getLogger(AgentPreprocessorConfiguration.class.getCanonicalName());

  private final Map<String, PointPreprocessor> portMap = new HashMap<>();

  public PointPreprocessor forPort(final String strPort) {
    PointPreprocessor preprocessor = portMap.get(strPort);
    if (preprocessor == null) {
      preprocessor = new PointPreprocessor();
      portMap.put(strPort, preprocessor);
    }
    return preprocessor;
  }

  public void loadFromStream(InputStream stream) {
    Yaml yaml = new Yaml();
    Map<String, Object> rulesByPort;
    try {
      //noinspection unchecked
      rulesByPort = (Map<String, Object>) yaml.load(stream);
      int totalValidRules = 0;
      for (String strPort : rulesByPort.keySet()) {
        int validRules = 0;
        try {
          int dummy = Integer.parseInt(strPort);
        } catch (NumberFormatException ex) {
          continue; // only load rules for numeric ports
        }
        //noinspection unchecked
        Map<String, Object> rules = (Map<String, Object>) rulesByPort.get(strPort);
        for (String ruleKey : rules.keySet()) {
          //noinspection unchecked
          Map<String, String> rule = (Map<String, String>) rules.get(ruleKey);
          try {
            Counter counter = Metrics.newCounter(
                new TaggedMetricName("preprocessor." + ruleKey, "count", "port", strPort));
            if (rule.get("scope") != null && rule.get("scope").equals("pointLine")) {
              switch (rule.get("action")) {
                case "replaceRegex":
                  this.forPort(strPort).forPointLine().addTransformer(
                      new PointLineReplaceRegexTransformer(rule.get("search"), rule.get("replace"), counter));
                  validRules++;
                  break;
                case "blacklistRegex":
                  this.forPort(strPort).forPointLine().addFilter(
                      new PointLineBlacklistRegexFilter(rule.get("match"), counter));
                  validRules++;
                  break;
                case "whitelistRegex":
                  this.forPort(strPort).forPointLine().addFilter(
                      new PointLineWhitelistRegexFilter(rule.get("match"), counter));
                  validRules++;
                  break;
              }
            } else {
              switch (rule.get("action")) {
                case "replaceRegex":
                  this.forPort(strPort).forReportPoint().addTransformer(
                      new ReportPointReplaceRegexTransformer(
                          rule.get("scope"), rule.get("search"), rule.get("replace"), counter));
                  validRules++;
                  break;
                case "addTag":
                  this.forPort(strPort).forReportPoint().addTransformer(
                      new ReportPointAddTagTransformer(rule.get("tag"), rule.get("value"), counter));
                  validRules++;
                  break;
                case "addTagIfNotExists":
                  this.forPort(strPort).forReportPoint().addTransformer(
                      new ReportPointAddTagIfNotExistsTransformer(rule.get("tag"), rule.get("value"), counter));
                  validRules++;
                  break;
                case "dropTag":
                  this.forPort(strPort).forReportPoint().addTransformer(
                      new ReportPointDropTagTransformer(rule.get("tag"), rule.get("match"), counter));
                  validRules++;
                  break;
                case "renameTag":
                  this.forPort(strPort).forReportPoint().addTransformer(
                      new ReportPointRenameTagTransformer(
                          rule.get("tag"), rule.get("newtag"), rule.get("match"), counter));
                  validRules++;
                  break;
                case "blacklistRegex":
                  this.forPort(strPort).forReportPoint().addFilter(
                      new ReportPointBlacklistRegexFilter(rule.get("scope"), rule.get("match"), counter));
                  validRules++;
                  break;
                case "whitelistRegex":
                  this.forPort(strPort).forReportPoint().addFilter(
                      new ReportPointWhitelistRegexFilter(rule.get("scope"), rule.get("match"), counter));
                  validRules++;
                  break;
              }
            }
          } catch (IllegalArgumentException | NullPointerException ex) {
            logger.warning("Unable to load rule " + ruleKey + " (port " + strPort + "): " + ex);
          }
        }
        logger.info("Loaded " + validRules + " rules for port " + strPort);
        totalValidRules += validRules;
      }
      logger.info("Total " + totalValidRules + " rules loaded");
    } catch (ClassCastException e) {
      logger.warning("Can't parse preprocessor configuration");
      throw e;
    }
  }
}
