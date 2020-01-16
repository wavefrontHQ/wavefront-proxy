package com.wavefront.agent.preprocessor;

import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableSet;
import wavefront.report.Annotation;
import wavefront.report.Span;

import javax.annotation.Nullable;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * Only allow span annotations that match the whitelist.
 *
 * @author vasily@wavefront.com
 */
public class SpanWhitelistAnnotationTransformer implements Function<Span, Span> {
  private static final Set<String> SYSTEM_TAGS = ImmutableSet.of("service", "application",
      "cluster", "shard");

  private final Map<String, Pattern> whitelistedKeys;
  private final PreprocessorRuleMetrics ruleMetrics;

  SpanWhitelistAnnotationTransformer(final Map<String, String> keys,
                                     final PreprocessorRuleMetrics ruleMetrics) {
    this.whitelistedKeys = new HashMap<>(keys.size() + SYSTEM_TAGS.size());
    SYSTEM_TAGS.forEach(x -> whitelistedKeys.put(x, null));
    keys.forEach((k, v) -> whitelistedKeys.put(k, v == null ? null : Pattern.compile(v)));
    Preconditions.checkNotNull(ruleMetrics, "PreprocessorRuleMetrics can't be null");
    this.ruleMetrics = ruleMetrics;
  }

  @Nullable
  @Override
  public Span apply(@Nullable Span span) {
    if (span == null) return null;
    long startNanos = ruleMetrics.ruleStart();
    List<Annotation> annotations = span.getAnnotations().stream().
        filter(x -> {
          if (!whitelistedKeys.containsKey(x.getKey())) return false;
          Pattern pattern = whitelistedKeys.get(x.getKey());
          return pattern == null || pattern.matcher(x.getValue()).matches();
        }).collect(Collectors.toList());
    if (annotations.size() < span.getAnnotations().size()) {
      span.setAnnotations(annotations);
      ruleMetrics.incrementRuleAppliedCounter();
    }
    ruleMetrics.ruleEnd(startNanos);
    return span;
  }

  /**
   * Create an instance based on loaded yaml fragment.
   *
   * @param ruleMap     yaml map
   * @param ruleMetrics metrics container
   * @return SpanWhitelistAnnotationTransformer instance
   */
  public static SpanWhitelistAnnotationTransformer create(
      Map<String, Object> ruleMap, final PreprocessorRuleMetrics ruleMetrics) {
    Object keys = ruleMap.get("whitelist");
    if (keys instanceof Map) {
      //noinspection unchecked
      return new SpanWhitelistAnnotationTransformer((Map<String, String>) keys, ruleMetrics);
    } else if (keys instanceof List) {
      Map<String, String> map = new HashMap<>();
      //noinspection unchecked
      ((List<String>) keys).forEach(x -> map.put(x, null));
      return new SpanWhitelistAnnotationTransformer(map, ruleMetrics);
    }
    throw new IllegalArgumentException("[whitelist] is not a list or a map");
  }
}
