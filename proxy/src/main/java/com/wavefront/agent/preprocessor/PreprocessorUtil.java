package com.wavefront.agent.preprocessor;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.validation.constraints.NotNull;

import wavefront.report.ReportPoint;

/**
 * Utility class for methods used by preprocessors
 *
 * @author vasily@wavefront.com
 */
public abstract class PreprocessorUtil {

  /**
   * Substitute {{...}} placeholders with corresponding components of the point
   * {{metricName}} {{sourceName}} are replaced with the metric name and source respectively
   * {{anyTagK}} is replaced with the value of the anyTagK point tag
   *
   * @param input        input string with {{...}} placeholders
   * @param reportPoint  ReportPoint object to extract components from
   * @return string with substituted placeholders
   */
  public static String expandPlaceholders(String input, @NotNull ReportPoint reportPoint) {
    if (input.contains("{{")) {
      StringBuffer result = new StringBuffer();
      Matcher placeholders = Pattern.compile("\\{\\{(.*?)}}").matcher(input);
      while (placeholders.find()) {
        if (placeholders.group(1).isEmpty()) {
          placeholders.appendReplacement(result, placeholders.group(0));
        } else {
          String substitution;
          switch (placeholders.group(1)) {
            case "metricName":
              substitution = reportPoint.getMetric();
              break;
            case "sourceName":
              substitution = reportPoint.getHost();
              break;
            default:
              substitution = reportPoint.getAnnotations().get(placeholders.group(1));
          }
          if (substitution != null) {
            placeholders.appendReplacement(result, substitution);
          } else {
            placeholders.appendReplacement(result, placeholders.group(0));
          }
        }
      }
      placeholders.appendTail(result);
      return result.toString();
    }
    return input;
  }

}
