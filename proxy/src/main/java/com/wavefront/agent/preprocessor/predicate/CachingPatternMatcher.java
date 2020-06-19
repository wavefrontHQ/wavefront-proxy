package com.wavefront.agent.preprocessor.predicate;

import java.util.concurrent.TimeUnit;
import java.util.function.BiFunction;
import java.util.regex.Pattern;

import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;

public class CachingPatternMatcher implements BiFunction<String, String, Boolean> {
  private final LoadingCache<String, Pattern> patternCache;

  public CachingPatternMatcher() {
    this(0);
  }

  public CachingPatternMatcher(int flags) {
     this.patternCache = Caffeine.newBuilder().
        maximumSize(10000).
        expireAfterAccess(1, TimeUnit.MINUTES).
        build(regex -> {
          return Pattern.compile(regex, flags);
        });
  }

  @Override
  public Boolean apply(String s, String s2) {
    return patternCache.get(s2).matcher(s).matches();
  }
}
