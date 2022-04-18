package com.wavefront.agent.preprocessor;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.wavefront.common.TaggedMetricName;
import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.Counter;
import com.yammer.metrics.core.Gauge;

import javax.annotation.Nullable;
import java.util.*;
import java.util.regex.Pattern;

import static com.wavefront.agent.preprocessor.PreprocessorConfigManager.*;
import static java.util.concurrent.TimeUnit.MINUTES;

public class MetricsFilter implements AnnotatedPredicate<String> {
  private final boolean allow;
  private final List<Pattern> regexList = new ArrayList<>();
  private final PreprocessorRuleMetrics ruleMetrics;
  private final Map<String, Boolean> cacheMetrics = new HashMap<>();
  private final Cache<String, Boolean> cacheRegexMatchs;
  private final Counter miss;

  public MetricsFilter(final Map<String, Object> rule, final PreprocessorRuleMetrics ruleMetrics, String ruleName, String strPort) {
    this.ruleMetrics = ruleMetrics;
    List<String> names;
    if (rule.get(NAMES) instanceof List) {
      names = (List<String>) rule.get(NAMES);
    } else {
      throw new IllegalArgumentException("'"+NAMES+"' should be a list of strings");
    }

    Map<String, Object> opts = (Map<String, Object>) rule.get(OPTS);
    int maximumSize = 1_000_000;
    if ((opts != null) && (opts.get("cacheSize") != null) && (opts.get("cacheSize") instanceof Integer)) {
      maximumSize = (Integer) opts.get("cacheSize");
    }

    cacheRegexMatchs = Caffeine.newBuilder()
            .expireAfterAccess(10, MINUTES)
            .maximumSize(maximumSize)
            .build();

    String func = rule.get(FUNC).toString();
    if (!func.equalsIgnoreCase("allow") && !func.equalsIgnoreCase("drop")) {
      throw new IllegalArgumentException("'Func' should be 'allow' or 'drop', not '" + func + "'");
    }
    allow = func.equalsIgnoreCase("allow");

    names.stream().filter(s -> s.startsWith("/") && s.endsWith("/"))
            .forEach(s -> regexList.add(Pattern.compile(s.replaceAll("/([^/]*)/", "$1"))));
    names.stream().filter(s -> !s.startsWith("/") && !s.endsWith("/"))
            .forEach(s -> cacheMetrics.put(s, allow));

    miss = Metrics.newCounter(new TaggedMetricName("preprocessor." + ruleName, "regexCache.miss", "port", strPort));
    Metrics.newGauge(new TaggedMetricName("preprocessor." + ruleName, "regexCache.size", "port", strPort),
            new Gauge<Integer>() {
              @Override
              public Integer value() {
                return regexList.size();
              }
            });
  }

  @Override
  public boolean test(String pointLine, @Nullable String[] messageHolder) {
    long startNanos = ruleMetrics.ruleStart();
    try {
      String name = pointLine.substring(0, pointLine.indexOf(" "));
      return cacheMetrics.computeIfAbsent(name, s -> testRegex(s));
    } finally {
      ruleMetrics.ruleEnd(startNanos);
    }
  }

  private boolean testRegex(String name) {
    return Boolean.TRUE.equals(cacheRegexMatchs.get(name, s -> {
      miss.inc();
      for (Pattern regex : regexList) {
        if (regex.matcher(name).find()) {
          return allow;
        }
      }
      return !allow;
    }));
  }
}
