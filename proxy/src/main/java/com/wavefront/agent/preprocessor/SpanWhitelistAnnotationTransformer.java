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
  @Nullable
  private final Map<String, Object> v2Predicate;


  SpanWhitelistAnnotationTransformer(final Map<String, String> keys,
                                     @Nullable final Map<String, Object> v2Predicate,
                                     final PreprocessorRuleMetrics ruleMetrics) {
    this.whitelistedKeys = new HashMap<>(keys.size() + SYSTEM_TAGS.size());
    SYSTEM_TAGS.forEach(x -> whitelistedKeys.put(x, null));
    keys.forEach((k, v) -> whitelistedKeys.put(k, v == null ? null : Pattern.compile(v)));
    Preconditions.checkNotNull(ruleMetrics, "PreprocessorRuleMetrics can't be null");
    this.ruleMetrics = ruleMetrics;
    this.v2Predicate = v2Predicate;
  }

  @Nullable
  @Override
  public Span apply(@Nullable Span span) {
    if (span == null) return null;
    long startNanos = ruleMetrics.ruleStart();
    if (!PreprocessorUtil.isRuleApplicable(v2Predicate, span)) return span;

    List<Annotation> annotations = span.getAnnotations().stream().
        filter(x -> whitelistedKeys.containsKey(x.getKey())).
        filter(x -> isPatternNullOrMatches(whitelistedKeys.get(x.getKey()), x.getValue())).
        collect(Collectors.toList());
    if (annotations.size() < span.getAnnotations().size()) {
      span.setAnnotations(annotations);
      ruleMetrics.incrementRuleAppliedCounter();
    }
    ruleMetrics.ruleEnd(startNanos);
    return span;
  }

  private static boolean isPatternNullOrMatches(@Nullable Pattern pattern, String string) {
    return pattern == null || pattern.matcher(string).matches();
  }

  /**
   * Create an instance based on loaded yaml fragment.
   *
   * @param ruleMap     yaml map
   * @param ruleMetrics metrics container
   * @return SpanWhitelistAnnotationTransformer instance
   */
  public static SpanWhitelistAnnotationTransformer create(Map<String, Object> ruleMap,
                                                          @Nullable final Map<String, Object> v2Predicate,
                                                          final PreprocessorRuleMetrics ruleMetrics) {
    Object keys = ruleMap.get("whitelist");
    if (keys instanceof Map) {
      //noinspection unchecked
      return new SpanWhitelistAnnotationTransformer((Map<String, String>) keys, v2Predicate, ruleMetrics);
    } else if (keys instanceof List) {
      Map<String, String> map = new HashMap<>();
      //noinspection unchecked
      ((List<String>) keys).forEach(x -> map.put(x, null));
      return new SpanWhitelistAnnotationTransformer(map, null, ruleMetrics);
    }
    throw new IllegalArgumentException("[whitelist] is not a list or a map");
  }
}
