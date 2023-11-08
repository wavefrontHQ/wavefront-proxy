package com.wavefront.agent.sampler;

import static com.wavefront.internal.SpanDerivedMetricsUtils.DEBUG_SPAN_TAG_VAL;
import static com.wavefront.sdk.common.Constants.DEBUG_TAG_KEY;

import com.github.benmanes.caffeine.cache.CacheLoader;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;
import com.wavefront.api.agent.SpanSamplingPolicy;
import com.wavefront.predicates.ExpressionSyntaxException;
import com.wavefront.predicates.Predicates;
import com.wavefront.sdk.entities.tracing.sampling.Sampler;
import com.yammer.metrics.core.Counter;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.logging.Logger;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import org.checkerframework.checker.nullness.qual.NonNull;
import wavefront.report.Annotation;
import wavefront.report.Span;

/**
 * Sampler that takes a {@link Span} as input and delegates to a {@link Sampler} when evaluating the
 * sampling decision.
 *
 * @author Han Zhang (zhanghan@vmware.com)
 */
public class SpanSampler {
  public static final String SPAN_SAMPLING_POLICY_TAG = "_sampledByPolicy";
  private static final int EXPIRE_AFTER_ACCESS_SECONDS = 3600;
  private static final int POLICY_BASED_SAMPLING_MOD_FACTOR = 100;
  private static final Logger logger = Logger.getLogger(SpanSampler.class.getCanonicalName());
  private final Sampler delegate;
  private final PreferredSampler preferredSampler;
  private final LoadingCache<String, Predicate<Span>> spanPredicateCache =
      Caffeine.newBuilder()
          .expireAfterAccess(EXPIRE_AFTER_ACCESS_SECONDS, TimeUnit.SECONDS)
          .build(
              new CacheLoader<String, Predicate<Span>>() {
                @Override
                @Nullable
                public Predicate<Span> load(@NonNull String key) {
                  try {
                    return Predicates.fromPredicateEvalExpression(key);
                  } catch (ExpressionSyntaxException ex) {
                    logger.severe("Policy expression " + key + " is invalid: " + ex.getMessage());
                    return null;
                  }
                }
              });
  private final Supplier<List<SpanSamplingPolicy>> activeSpanSamplingPoliciesSupplier;

  /**
   * Creates a new instance from a {@Sampler} delegate.
   *
   * @param delegate The delegate {@Sampler}.
   * @param activeSpanSamplingPoliciesSupplier Active span sampling policies to be applied.
   */
  public SpanSampler(
      Sampler delegate,
      @Nonnull Supplier<List<SpanSamplingPolicy>> activeSpanSamplingPoliciesSupplier,
      PreferredSampler preferredSampler) {
    this.delegate = delegate;
    this.activeSpanSamplingPoliciesSupplier = activeSpanSamplingPoliciesSupplier;
    this.preferredSampler = preferredSampler;
  }

  /**
   * Evaluates whether a span should be allowed or discarded.
   *
   * @param span The span to sample.
   * @return true if the span should be allowed, false otherwise.
   */
  public boolean sample(Span span) {
    return sample(span, null);
  }

  /**
   * Evaluates whether a span should be allowed or discarded, and increment a counter if it should
   * be discarded.
   *
   * @param span The span to sample.
   * @param discarded The counter to increment if the decision is to discard the span.
   * @return true if the span should be allowed, false otherwise.
   */
  public boolean sample(Span span, @Nullable Counter discarded) {
    if (isForceSampled(span)) {
      return true;
    }

    // Prefered sampling
    if (preferredSampler != null && preferredSampler.isApplicable(span)) {
      return preferredSampler.sample(span);
    }

    // Policy based span sampling
    List<SpanSamplingPolicy> activeSpanSamplingPolicies = activeSpanSamplingPoliciesSupplier.get();
    if (activeSpanSamplingPolicies != null) {
      int samplingPercent = 0;
      String policyId = null;
      for (SpanSamplingPolicy policy : activeSpanSamplingPolicies) {
        Predicate<Span> spanPredicate = spanPredicateCache.get(policy.getExpression());
        if (spanPredicate != null
            && spanPredicate.test(span)
            && policy.getSamplingPercent() > samplingPercent) {
          samplingPercent = policy.getSamplingPercent();
          policyId = policy.getPolicyId();
        }
      }
      if (samplingPercent > 0
          && Math.abs(UUID.fromString(span.getTraceId()).getLeastSignificantBits())
                  % POLICY_BASED_SAMPLING_MOD_FACTOR
              <= samplingPercent) {
        if (span.getAnnotations() == null) {
          span.setAnnotations(new ArrayList<>());
        }
        span.getAnnotations().add(new Annotation(SPAN_SAMPLING_POLICY_TAG, policyId));
        return true;
      }
    }
    if (delegate.sample(
        span.getName(),
        UUID.fromString(span.getTraceId()).getLeastSignificantBits(),
        span.getDuration())) {
      return true;
    }
    if (discarded != null) {
      discarded.inc();
    }
    return false;
  }

  /**
   * Util method to determine if a span is force sampled. force samples if The span annotation
   * debug=true is present
   *
   * @return true if the span should be force sampled.
   */
  private boolean isForceSampled(Span span) {
    List<Annotation> annotations = span.getAnnotations();
    for (Annotation annotation : annotations) {
      if (DEBUG_TAG_KEY.equals(annotation.getKey())) {
        if (annotation.getValue().equals(DEBUG_SPAN_TAG_VAL)) {
          return true;
        }
      }
    }
    return false;
  }
}
