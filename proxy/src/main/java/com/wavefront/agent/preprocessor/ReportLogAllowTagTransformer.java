package com.wavefront.agent.preprocessor;

import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableSet;
import wavefront.report.Annotation;
import wavefront.report.ReportLog;

import javax.annotation.Nullable;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Predicate;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * Only allow log tags that match the allowed list.
 *
 * @author vasily@wavefront.com
 */
public class ReportLogAllowTagTransformer implements Function<ReportLog, ReportLog> {

  private final Map<String, Pattern> allowedTags;
  private final PreprocessorRuleMetrics ruleMetrics;
  private final Predicate<ReportLog> v2Predicate;


  ReportLogAllowTagTransformer(final Map<String, String> tags,
                                      @Nullable final Predicate<ReportLog> v2Predicate,
                                      final PreprocessorRuleMetrics ruleMetrics) {
    this.allowedTags = new HashMap<>(tags.size());
    tags.forEach((k, v) -> allowedTags.put(k, v == null ? null : Pattern.compile(v)));
    Preconditions.checkNotNull(ruleMetrics, "PreprocessorRuleMetrics can't be null");
    this.ruleMetrics = ruleMetrics;
    this.v2Predicate = v2Predicate != null ? v2Predicate : x -> true;
  }

  @Nullable
  @Override
  public ReportLog apply(@Nullable ReportLog reportLog) {
    if (reportLog == null) return null;
    long startNanos = ruleMetrics.ruleStart();
    try {
      if (!v2Predicate.test(reportLog)) return reportLog;

      List<Annotation> annotations = reportLog.getAnnotations().stream().
          filter(x -> allowedTags.containsKey(x.getKey())).
          filter(x -> isPatternNullOrMatches(allowedTags.get(x.getKey()), x.getValue())).
          collect(Collectors.toList());
      if (annotations.size() < reportLog.getAnnotations().size()) {
        reportLog.setAnnotations(annotations);
        ruleMetrics.incrementRuleAppliedCounter();
      }
      return reportLog;
    } finally {
      ruleMetrics.ruleEnd(startNanos);
    }
  }

  private static boolean isPatternNullOrMatches(@Nullable Pattern pattern, String string) {
    return pattern == null || pattern.matcher(string).matches();
  }

  /**
   * Create an instance based on loaded yaml fragment.
   *
   * @param ruleMap     yaml map
   * @param v2Predicate the v2 predicate
   * @param ruleMetrics metrics container
   * @return ReportLogAllowAnnotationTransformer instance
   */
  public static ReportLogAllowTagTransformer create(Map<String, Object> ruleMap,
                                                      @Nullable final Predicate<ReportLog> v2Predicate,
                                                      final PreprocessorRuleMetrics ruleMetrics) {
    Object tags = ruleMap.get("allow");
    if (tags instanceof Map) {
      //noinspection unchecked
      return new ReportLogAllowTagTransformer((Map<String, String>) tags, v2Predicate, ruleMetrics);
    } else if (tags instanceof List) {
      Map<String, String> map = new HashMap<>();
      //noinspection unchecked
      ((List<String>) tags).forEach(x -> map.put(x, null));
      return new ReportLogAllowTagTransformer(map, null, ruleMetrics);
    }
    throw new IllegalArgumentException("[allow] is not a list or a map");
  }
}
