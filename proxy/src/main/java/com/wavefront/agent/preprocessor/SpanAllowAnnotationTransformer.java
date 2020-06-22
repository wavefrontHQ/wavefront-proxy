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
import java.util.function.Predicate;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * Only allow span annotations that match the allowed list.
 *
 * @author vasily@wavefront.com
 */
public class SpanAllowAnnotationTransformer implements Function<Span, Span> {
  private static final Set<String> SYSTEM_TAGS = ImmutableSet.of("service", "application",
      "cluster", "shard");

  private final Map<String, Pattern> allowedKeys;
  private final PreprocessorRuleMetrics ruleMetrics;
  private final Predicate<Span> v2Predicate;


  SpanAllowAnnotationTransformer(final Map<String, String> keys,
                                 @Nullable final Predicate<Span> v2Predicate,
                                 final PreprocessorRuleMetrics ruleMetrics) {
    this.allowedKeys = new HashMap<>(keys.size() + SYSTEM_TAGS.size());
    SYSTEM_TAGS.forEach(x -> allowedKeys.put(x, null));
    keys.forEach((k, v) -> allowedKeys.put(k, v == null ? null : Pattern.compile(v)));
    Preconditions.checkNotNull(ruleMetrics, "PreprocessorRuleMetrics can't be null");
    this.ruleMetrics = ruleMetrics;
    this.v2Predicate = v2Predicate != null ? v2Predicate : x -> true;
  }

  @Nullable
  @Override
  public Span apply(@Nullable Span span) {
    if (span == null) return null;
    long startNanos = ruleMetrics.ruleStart();
    try {
      if (!v2Predicate.test(span)) return span;

      List<Annotation> annotations = span.getAnnotations().stream().
          filter(x -> allowedKeys.containsKey(x.getKey())).
          filter(x -> isPatternNullOrMatches(allowedKeys.get(x.getKey()), x.getValue())).
          collect(Collectors.toList());
      if (annotations.size() < span.getAnnotations().size()) {
        span.setAnnotations(annotations);
        ruleMetrics.incrementRuleAppliedCounter();
      }
      return span;
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
   * @return SpanAllowAnnotationTransformer instance
   */
  public static SpanAllowAnnotationTransformer create(Map<String, Object> ruleMap,
                                                      @Nullable final Predicate<Span> v2Predicate,
                                                      final PreprocessorRuleMetrics ruleMetrics) {
    Object keys = ruleMap.get("allow");
    if (keys instanceof Map) {
      //noinspection unchecked
      return new SpanAllowAnnotationTransformer((Map<String, String>) keys, v2Predicate, ruleMetrics);
    } else if (keys instanceof List) {
      Map<String, String> map = new HashMap<>();
      //noinspection unchecked
      ((List<String>) keys).forEach(x -> map.put(x, null));
      return new SpanAllowAnnotationTransformer(map, null, ruleMetrics);
    }
    throw new IllegalArgumentException("[allow] is not a list or a map");
  }
}
