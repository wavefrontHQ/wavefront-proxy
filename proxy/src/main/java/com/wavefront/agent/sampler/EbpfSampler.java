package com.wavefront.agent.sampler;

import static com.wavefront.common.TraceConstants.PARENT_KEY;
import static com.wavefront.sdk.common.Constants.*;

import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;
import com.google.common.annotations.VisibleForTesting;
import com.wavefront.sdk.entities.tracing.sampling.RateSampler;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.LongAccumulator;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import org.apache.commons.lang.StringUtils;
import wavefront.report.Annotation;
import wavefront.report.Span;

/**
 * This Sampler is for Ebpf traffic focusing on not losing a single edge due to sampling in
 * application topology. It uses a dynamic sampling rate and prioritizes less frequent edges, error
 * spans and long spans
 */
public class EbpfSampler implements PreferredSampler {
  private static final String OTEL_ORIGIN = "origin";
  private static final String OTEL_ORIGIN_VALUE_EBPF = "opapps-auto";
  static final String FROM_SERVICE_TAG_KEY = "from_service";
  static final String FROM_SOURCE_TAG_KEY = "from_source";

  private final int durationLimit;
  private final double samplingRate;
  protected static final Logger logger = Logger.getLogger("ebpfSampler");
  private double samplingFactor = 0.5;
  private long totalTypes = 0;

  public EbpfSampler(double samplingRate, int durationLimit) {
    this.samplingRate = samplingRate;
    this.durationLimit = durationLimit;
  }

  class CacheKey {
    String customer;
    String application;
    String fromService;
    String fromSource;
    String toService;
    String toSource;

    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;

      CacheKey that = (CacheKey) o;

      if (!customer.equals(that.customer)) return false;
      if (!application.equals(that.application)) return false;
      if (!fromService.equals(that.fromService)) return false;
      if (!toService.equals(that.toService)) return false;

      if (fromSource != null ? !fromSource.equals(that.fromSource) : that.fromSource != null)
        return false;
      return toSource != null ? toSource.equals(that.toSource) : that.toSource == null;
    }

    @Override
    public int hashCode() {
      int result = application.hashCode();
      result = 31 * result + fromService.hashCode();
      result = 31 * result + toService.hashCode();
      result = 31 * result + customer.hashCode();
      result = 31 * result + (fromSource != null ? fromSource.hashCode() : 0);
      result = 31 * result + (toSource != null ? toSource.hashCode() : 0);

      return result;
    }
  }

  static class EdgeStats {
    static final AtomicInteger totalEdgeCount = new AtomicInteger(0);
    static final AtomicInteger totalSampledCount = new AtomicInteger(0);
    AtomicInteger edgeCount = new AtomicInteger(0);
    AtomicInteger errorCount = new AtomicInteger(0);
    AtomicInteger longDurationCount = new AtomicInteger(0);
    LongAccumulator maxDuration = new LongAccumulator(Long::max, 0);

    public double getTypeRatio() {
      if (totalEdgeCount.get() == 0) return 0;
      return ((double) edgeCount.get()) / totalEdgeCount.get();
    }

    public double getErrorRatio() {
      if (totalEdgeCount.get() == 0) return 0;
      return ((double) errorCount.get()) / totalEdgeCount.get();
    }

    public double getLongDurationRatio() {
      if (totalEdgeCount.get() == 0) return 0;
      return ((double) longDurationCount.get()) / totalEdgeCount.get();
    }

    public double getSampledRatio() {
      if (totalEdgeCount.get() == 0) return 0.0;
      double ratio = ((double) totalSampledCount.get()) / totalEdgeCount.get();
      if (ratio > 1.0) ratio = 1.0;
      return ratio;
    }

    public boolean isSignificant() {
      return totalEdgeCount.get() >= 100;
    }

    @Override
    public String toString() {
      return String.format(
          "errorCount %d, edgeCount %d, longDurationCount %d, maxDuration %d",
          errorCount.intValue(),
          edgeCount.intValue(),
          longDurationCount.longValue(),
          maxDuration.longValue());
    }
  }

  private final LoadingCache<CacheKey, EdgeStats> edgeStats =
      Caffeine.newBuilder().build(cacheKey -> new EdgeStats());

  @Override
  public boolean sample(@Nonnull Span span) {
    Map<String, String> annotationMap = convertAnnotationToMap(span);

    boolean sampled = false;
    CacheKey key = extractCacheKey(span, annotationMap);
    EdgeStats stats = edgeStats.get(key);
    stats.totalEdgeCount.getAndIncrement();

    stats.edgeCount.getAndIncrement();
    if (!stats.isSignificant()) sampled = true;

    if (annotationMap.containsKey(ERROR_TAG_KEY)) {
      stats.errorCount.getAndIncrement();
      if (!sampled && stats.getErrorRatio() < 0.5) sampled = true;
    }
    if (span.getDuration() > stats.maxDuration.longValue()) {
      stats.maxDuration.accumulate(span.getDuration());
      sampled = true;
    }
    if (durationLimit > 0 && span.getDuration() > durationLimit) {
      stats.longDurationCount.getAndIncrement();
      if (!sampled && stats.getLongDurationRatio() < 0.5) sampled = true;
    }
    logger.log(Level.FINE, stats.toString());
    if (!sampled) {
      // adjust sampling factor every 1000 spans
      if (stats.totalEdgeCount.get() % 1000 == 0) {
        long totalTypes = edgeStats.estimatedSize();
        if (Math.abs(totalTypes - this.totalTypes) > 1) {
          samplingFactor = estimateSamplingFactor();
          this.totalTypes = totalTypes;
          logger.info(
              String.format(
                  "ebpf sampling factor adjusted to %f based on total edges of %d",
                  samplingFactor, totalTypes));
        }
      }
      double samplingRate = 1 - Math.pow(stats.getTypeRatio(), samplingFactor);
      logger.log(
          Level.FINE,
          String.format(
              "typeRatio:%f, final sampling rate %f", stats.getTypeRatio(), samplingRate));
      sampled =
          new RateSampler(samplingRate)
              .sample(null, UUID.fromString(span.getTraceId()).getLeastSignificantBits(), 0);
    }
    if (sampled) stats.totalSampledCount.getAndIncrement();
    return sampled;
  }

  @Override
  public boolean isApplicable(@Nonnull Span span) {
    List<Annotation> annotations = span.getAnnotations();
    for (Annotation annotation : annotations) {
      if (OTEL_ORIGIN.equals(annotation.getKey())) {
        if (OTEL_ORIGIN_VALUE_EBPF.equals(annotation.getValue())) return true;
      }
    }
    return false;
  }

  private static Map<String, String> convertAnnotationToMap(Span span) {
    return span.getAnnotations().stream()
        .collect(Collectors.toMap(Annotation::getKey, Annotation::getValue));
  }

  private CacheKey extractCacheKey(@Nonnull Span span, @Nonnull Map<String, String> annotationMap) {
    CacheKey key = new CacheKey();
    key.customer = span.getCustomer();
    key.application = annotationMap.getOrDefault(APPLICATION_TAG_KEY, "default_application");
    key.toService = annotationMap.getOrDefault(SERVICE_TAG_KEY, "to_service");
    key.fromService = annotationMap.getOrDefault(FROM_SERVICE_TAG_KEY, "from_service");
    key.toSource = span.getSource();
    key.fromSource = annotationMap.getOrDefault(FROM_SOURCE_TAG_KEY, "from_source");

    // pixie side seems not honoring spanTopologyDimensions in the customer setting. so not adding
    // tags for now

    return key;
  }

  @VisibleForTesting
  double estimateSamplingFactor() {
    double estimate = 0.5;
    long totalTypes = edgeStats.estimatedSize();
    double lastDelta = 1;
    int factor = 10;
    if (totalTypes > 0) {
      for (double power = 0.5; power > 0.005; ) {
        // System.out.println(power);
        double samplingRate = 1 - Math.pow(1.0 / totalTypes, power);
        double delta = Math.abs(samplingRate - this.samplingRate);
        if (delta < 0.1) break;
        if (delta >= lastDelta) break;
        else {
          estimate = power;
          lastDelta = delta;
        }
        if (Math.abs(power - 0.01) < 0.000001d) {
          factor = 1000;
          power = 0.009;
        } else if (Math.abs(power - 0.1) < 0.000001d) {
          factor = 100;
          power = 0.09;
        } else {
          power = power - (1d / factor);
        }
      }
    }
    return estimate;
  }
}
