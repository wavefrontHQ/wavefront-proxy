package com.wavefront.agent.preprocessor;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.wavefront.common.TaggedMetricName;
import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.Counter;
import com.yammer.metrics.core.Gauge;

import javax.annotation.Nullable;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Pattern;

import static com.wavefront.agent.preprocessor.PreprocessorConfigManager.*;
import static java.util.concurrent.TimeUnit.MINUTES;

public class MetricsFilter implements AnnotatedPredicate<String> {
  private final boolean allow;
  private final List<Pattern> regexList = new ArrayList<>();
  private final PreprocessorRuleMetrics ruleMetrics;
  private final Map<String, Boolean> cacheMetrics = new ConcurrentHashMap<>();
  private final Cache<String, Boolean> cacheRegexMatchs;
  private final Counter miss;
  private final Counter queries;

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

    queries = Metrics.newCounter(new TaggedMetricName("preprocessor." + ruleName, "regexCache.queries", "port", strPort));
    miss = Metrics.newCounter(new TaggedMetricName("preprocessor." + ruleName, "regexCache.miss", "port", strPort));
    TaggedMetricName sizeMetrics = new TaggedMetricName("preprocessor." + ruleName, "regexCache.size", "port", strPort);
    Metrics.defaultRegistry().removeMetric(sizeMetrics);
    Metrics.newGauge(sizeMetrics, new Gauge<Integer>() {
              @Override
              public Integer value() {
                return (int)cacheRegexMatchs.estimatedSize();
              }
            });
  }

  @Override
  public boolean test(String pointLine, @Nullable String[] messageHolder) {
    long startNanos = ruleMetrics.ruleStart();
    try {
      String name = pointLine.substring(0, pointLine.indexOf(" "));
      Boolean res = cacheMetrics.get(name);
      if (res == null) {
        res = testRegex(name);
      }
      return res;
    } finally {
      ruleMetrics.ruleEnd(startNanos);
    }
  }

  private boolean testRegex(String name) {
    queries.inc();
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
