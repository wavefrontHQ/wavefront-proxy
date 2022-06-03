package com.wavefront.agent.listeners.otlp;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Lists;

import com.wavefront.agent.handlers.ReportableEntityHandler;
import com.wavefront.agent.preprocessor.ReportableEntityPreprocessor;
import com.wavefront.common.MetricConstants;
import com.wavefront.sdk.common.Pair;
import com.wavefront.sdk.entities.histograms.HistogramGranularity;

import org.jetbrains.annotations.NotNull;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import java.util.logging.Logger;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.annotation.Nullable;

import io.opentelemetry.proto.collector.metrics.v1.ExportMetricsServiceRequest;
import io.opentelemetry.proto.common.v1.AnyValue;
import io.opentelemetry.proto.common.v1.KeyValue;
import io.opentelemetry.proto.metrics.v1.AggregationTemporality;
import io.opentelemetry.proto.metrics.v1.ExponentialHistogram;
import io.opentelemetry.proto.metrics.v1.ExponentialHistogramDataPoint;
import io.opentelemetry.proto.metrics.v1.Gauge;
import io.opentelemetry.proto.metrics.v1.Histogram;
import io.opentelemetry.proto.metrics.v1.HistogramDataPoint;
import io.opentelemetry.proto.metrics.v1.Metric;
import io.opentelemetry.proto.metrics.v1.NumberDataPoint;
import io.opentelemetry.proto.metrics.v1.ResourceMetrics;
import io.opentelemetry.proto.metrics.v1.ScopeMetrics;
import io.opentelemetry.proto.metrics.v1.Sum;
import io.opentelemetry.proto.metrics.v1.Summary;
import io.opentelemetry.proto.metrics.v1.SummaryDataPoint;
import io.opentelemetry.proto.resource.v1.Resource;
import wavefront.report.Annotation;
import wavefront.report.HistogramType;
import wavefront.report.ReportPoint;


public class OtlpMetricsUtils {
  public final static Logger OTLP_DATA_LOGGER = Logger.getLogger("OTLPDataLogger");
  public static final int MILLIS_IN_MINUTE = 60 * 1000;
  public static final int MILLIS_IN_HOUR = 60 * 60 * 1000;
  public static final int MILLIS_IN_DAY = 24 * 60 * 60 * 1000;

  public static void exportToWavefront(ExportMetricsServiceRequest request,
                                       ReportableEntityHandler<ReportPoint, String> pointHandler,
                                       ReportableEntityHandler<ReportPoint, String> histogramHandler,
                                       @Nullable Supplier<ReportableEntityPreprocessor> preprocessorSupplier,
                                       String defaultSource,
                                       boolean includeResourceAttrsForMetrics) {
    ReportableEntityPreprocessor preprocessor = null;
    if (preprocessorSupplier != null) {
      preprocessor = preprocessorSupplier.get();
    }

    for (ReportPoint point : fromOtlpRequest(request, preprocessor, defaultSource, includeResourceAttrsForMetrics)) {
      // TODO: handle sampler
      if (point.getValue() instanceof wavefront.report.Histogram) {
        if (!wasFilteredByPreprocessor(point, histogramHandler, preprocessor)) {
          histogramHandler.report(point);
        }
      } else {
        if (!wasFilteredByPreprocessor(point, pointHandler, preprocessor)) {
          pointHandler.report(point);
        }
      }
    }
  }

  private static List<ReportPoint> fromOtlpRequest(ExportMetricsServiceRequest request,
                                                   @Nullable ReportableEntityPreprocessor preprocessor,
                                                   String defaultSource, boolean includeResourceAttrsForMetrics) {
    List<ReportPoint> wfPoints = Lists.newArrayList();

    for (ResourceMetrics resourceMetrics : request.getResourceMetricsList()) {
      Resource resource = resourceMetrics.getResource();
      OTLP_DATA_LOGGER.finest(() -> "Inbound OTLP Resource: " + resource);
      Pair<String, List<KeyValue>> sourceAndResourceAttrs =
          OtlpTraceUtils.sourceFromAttributes(resource.getAttributesList(), defaultSource);
      String source = sourceAndResourceAttrs._1;
      List<KeyValue> resourceAttributes = includeResourceAttrsForMetrics ?
          sourceAndResourceAttrs._2 : Collections.EMPTY_LIST;

      for (ScopeMetrics scopeMetrics : resourceMetrics.getScopeMetricsList()) {
        OTLP_DATA_LOGGER.finest(() -> "Inbound OTLP Instrumentation Scope: " +
            scopeMetrics.getScope());
        for (Metric otlpMetric : scopeMetrics.getMetricsList()) {
          OTLP_DATA_LOGGER.finest(() -> "Inbound OTLP Metric: " + otlpMetric);
          List<ReportPoint> points = transform(otlpMetric, resourceAttributes, preprocessor, source);
          OTLP_DATA_LOGGER.finest(() -> "Converted Wavefront Metric: " + points);

          wfPoints.addAll(points);
        }
      }
    }
    return wfPoints;
  }

  @VisibleForTesting
  static boolean wasFilteredByPreprocessor(ReportPoint wfReportPoint,
                                           ReportableEntityHandler<ReportPoint, String> spanHandler,
                                           @Nullable ReportableEntityPreprocessor preprocessor) {
    if (preprocessor == null) {
      return false;
    }

    String[] messageHolder = new String[1];
    if (!preprocessor.forReportPoint().filter(wfReportPoint, messageHolder)) {
      if (messageHolder[0] != null) {
        spanHandler.reject(wfReportPoint, messageHolder[0]);
      } else {
        spanHandler.block(wfReportPoint);
      }
      return true;
    }

    return false;
  }

  @VisibleForTesting
  public static List<ReportPoint> transform(Metric otlpMetric,
                                            List<KeyValue> resourceAttrs,
                                            ReportableEntityPreprocessor preprocessor,
                                            String source) {
    List<ReportPoint> points = new ArrayList<>();
    if (otlpMetric.hasGauge()) {
      points.addAll(transformGauge(otlpMetric.getName(), otlpMetric.getGauge(), resourceAttrs));
    } else if (otlpMetric.hasSum()) {
      points.addAll(transformSum(otlpMetric.getName(), otlpMetric.getSum(), resourceAttrs));
    } else if (otlpMetric.hasSummary()) {
      points.addAll(transformSummary(otlpMetric.getName(), otlpMetric.getSummary(), resourceAttrs));
    } else if (otlpMetric.hasHistogram()) {
      points.addAll(transformHistogram(otlpMetric.getName(),
          fromOtelHistogram(otlpMetric.getHistogram()),
          otlpMetric.getHistogram().getAggregationTemporality(),
          resourceAttrs));
    } else if (otlpMetric.hasExponentialHistogram()) {
      points.addAll(transformHistogram(otlpMetric.getName(),
          fromOtelExponentialHistogram(otlpMetric.getExponentialHistogram()),
          otlpMetric.getExponentialHistogram().getAggregationTemporality(),
          resourceAttrs));
    } else {
      throw new IllegalArgumentException("Otel: unsupported metric type for " + otlpMetric.getName());
    }

    for (ReportPoint point : points) {
      point.setHost(source);
      // preprocessor rule transformations should run last
      if (preprocessor != null) {
        preprocessor.forReportPoint().transform(point);
      }
    }
    return points;
  }

  private static List<ReportPoint> transformSummary(String name, Summary summary, List<KeyValue> resourceAttrs) {
    List<ReportPoint> points = new ArrayList<>(summary.getDataPointsCount());
    for (SummaryDataPoint p : summary.getDataPointsList()) {
      points.addAll(transformSummaryDataPoint(name, p, resourceAttrs));
    }
    return points;
  }

  private static List<ReportPoint> transformSum(String name, Sum sum,
                                                List<KeyValue> resourceAttrs) {
    if (sum.getDataPointsCount() == 0) {
      throw new IllegalArgumentException("OTel: sum with no data points");
    }

    String prefix = "";
    switch (sum.getAggregationTemporality()) {
      case AGGREGATION_TEMPORALITY_CUMULATIVE:
        // no prefix
        break;
      case AGGREGATION_TEMPORALITY_DELTA:
        prefix = MetricConstants.DELTA_PREFIX;
        break;
      default:
        throw new IllegalArgumentException("OTel: sum with unsupported aggregation temporality " + sum.getAggregationTemporality().name());
    }

    List<ReportPoint> points = new ArrayList<>(sum.getDataPointsCount());
    for (NumberDataPoint p : sum.getDataPointsList()) {
      points.add(transformNumberDataPoint(prefix + name, p, resourceAttrs));
    }
    return points;
  }

  private static List<ReportPoint> transformHistogram(
      String name,
      List<BucketHistogramDataPoint> dataPoints,
      AggregationTemporality aggregationTemporality,
      List<KeyValue> resourceAttrs) {

    switch (aggregationTemporality) {
      case AGGREGATION_TEMPORALITY_CUMULATIVE:
        return transformCumulativeHistogram(name, dataPoints, resourceAttrs);
      case AGGREGATION_TEMPORALITY_DELTA:
        return transformDeltaHistogram(name, dataPoints, resourceAttrs);
      default:
        throw new IllegalArgumentException("OTel: histogram with unsupported aggregation temporality "
            + aggregationTemporality.name());
    }
  }

  private static List<ReportPoint> transformDeltaHistogram(
      String name, List<BucketHistogramDataPoint> dataPoints, List<KeyValue> resourceAttrs) {
    List<ReportPoint> reportPoints = new ArrayList<>();
    for (BucketHistogramDataPoint dataPoint : dataPoints) {
      reportPoints.addAll(transformDeltaHistogramDataPoint(name, dataPoint, resourceAttrs));
    }

    return reportPoints;
  }

  private static List<ReportPoint> transformCumulativeHistogram(
      String name, List<BucketHistogramDataPoint> dataPoints, List<KeyValue> resourceAttrs) {

    List<ReportPoint> reportPoints = new ArrayList<>();
    for (BucketHistogramDataPoint dataPoint : dataPoints) {
      reportPoints.addAll(transformCumulativeHistogramDataPoint(name, dataPoint, resourceAttrs));
    }

    return reportPoints;
  }

  private static List<ReportPoint> transformDeltaHistogramDataPoint(
      String name, BucketHistogramDataPoint point, List<KeyValue> resourceAttrs) {
    List<Double> explicitBounds = point.getExplicitBounds();
    List<Long> bucketCounts = point.getBucketCounts();
    if (explicitBounds.size() != bucketCounts.size() - 1) {
      throw new IllegalArgumentException("OTel: histogram " + name + ": Explicit bounds count " +
          "should be one less than bucket count. ExplicitBounds: " + explicitBounds.size() +
          ", BucketCounts: " + bucketCounts.size());
    }

    List<ReportPoint> reportPoints = new ArrayList<>();

    List<Double> bins = new ArrayList<>(bucketCounts.size());
    List<Integer> counts = new ArrayList<>(bucketCounts.size());

    for (int currentIndex = 0; currentIndex < bucketCounts.size(); currentIndex++) {
      bins.add(getDeltaHistogramBound(explicitBounds, currentIndex));
      counts.add(bucketCounts.get(currentIndex).intValue());
    }

    for (HistogramGranularity granularity : HistogramGranularity.values()) {
      int duration;
      switch (granularity) {
        case MINUTE:
          duration = MILLIS_IN_MINUTE;
          break;
        case HOUR:
          duration = MILLIS_IN_HOUR;
          break;
        case DAY:
          duration = MILLIS_IN_DAY;
          break;
        default:
          throw new IllegalArgumentException("Unknown granularity: " + granularity);
      }

      wavefront.report.Histogram histogram = wavefront.report.Histogram.newBuilder().
          setType(HistogramType.TDIGEST).
          setBins(bins).
          setCounts(counts).
          setDuration(duration).
          build();

      ReportPoint rp = pointWithAnnotations(name, point.getAttributesList(), resourceAttrs,
          point.getTimeUnixNano())
          .setValue(histogram)
          .build();
      reportPoints.add(rp);
    }
    return reportPoints;
  }

  private static Double getDeltaHistogramBound(List<Double> explicitBounds, int currentIndex) {
    if (explicitBounds.size() == 0) {
      // As coded in the metric exporter(OpenTelemetry Collector)
      return 0.0;
    }
    if (currentIndex == 0) {
      return explicitBounds.get(0);
    } else if (currentIndex == explicitBounds.size()) {
      return explicitBounds.get(explicitBounds.size() - 1);
    }
    return (explicitBounds.get(currentIndex - 1) + explicitBounds.get(currentIndex)) / 2.0;
  }

  private static List<ReportPoint> transformCumulativeHistogramDataPoint(
      String name, BucketHistogramDataPoint point, List<KeyValue> resourceAttrs) {
    List<Long> bucketCounts = point.getBucketCounts();
    List<Double> explicitBounds = point.getExplicitBounds();

    if (explicitBounds.size() != bucketCounts.size() - 1) {
      throw new IllegalArgumentException("OTel: histogram " + name + ": Explicit bounds count " +
          "should be one less than bucket count. ExplicitBounds: " + explicitBounds.size() +
          ", BucketCounts: " + bucketCounts.size());
    }

    List<ReportPoint> reportPoints = new ArrayList<>(bucketCounts.size());
    int currentIndex = 0;
    long cumulativeBucketCount = 0;
    for (; currentIndex < explicitBounds.size(); currentIndex++) {
      cumulativeBucketCount += bucketCounts.get(currentIndex);
      // we have to create a new builder every time as the annotations are getting appended after
      // each iteration
      ReportPoint rp = pointWithAnnotations(name, point.getAttributesList(), resourceAttrs,
          point.getTimeUnixNano())
          .setValue(cumulativeBucketCount)
          .build();
      handleDupAnnotation(rp);
      rp.getAnnotations().put("le", String.valueOf(explicitBounds.get(currentIndex)));
      reportPoints.add(rp);
    }

    ReportPoint rp = pointWithAnnotations(name, point.getAttributesList(), resourceAttrs,
        point.getTimeUnixNano())
        .setValue(cumulativeBucketCount + bucketCounts.get(currentIndex))
        .build();
    handleDupAnnotation(rp);
    rp.getAnnotations().put("le", "+Inf");
    reportPoints.add(rp);

    return reportPoints;
  }

  private static void handleDupAnnotation(ReportPoint rp) {
    if (rp.getAnnotations().containsKey("le")) {
      String val = rp.getAnnotations().get("le");
      rp.getAnnotations().remove("le");
      rp.getAnnotations().put("_le", val);
    }
  }

  private static Collection<ReportPoint> transformGauge(String name, Gauge gauge,
                                                        List<KeyValue> resourceAttrs) {
    if (gauge.getDataPointsCount() == 0) {
      throw new IllegalArgumentException("OTel: gauge with no data points");
    }

    List<ReportPoint> points = new ArrayList<>(gauge.getDataPointsCount());
    for (NumberDataPoint p : gauge.getDataPointsList()) {
      points.add(transformNumberDataPoint(name, p, resourceAttrs));
    }
    return points;
  }

  @NotNull
  private static ReportPoint transformNumberDataPoint(String name, NumberDataPoint point, List<KeyValue> resourceAttrs) {
    return pointWithAnnotations(name, point.getAttributesList(), resourceAttrs,
        point.getTimeUnixNano())
        .setValue(point.getAsDouble())
        .build();
  }

  @NotNull
  private static List<ReportPoint> transformSummaryDataPoint(String name, SummaryDataPoint point, List<KeyValue> resourceAttrs) {
    List<ReportPoint> toReturn = new ArrayList<>();
    List<KeyValue> pointAttributes = replaceQuantileTag(point.getAttributesList());
    toReturn.add(pointWithAnnotations(name + "_sum", pointAttributes, resourceAttrs, point.getTimeUnixNano())
        .setValue(point.getSum())
        .build());
    toReturn.add(pointWithAnnotations(name + "_count", pointAttributes, resourceAttrs, point.getTimeUnixNano())
        .setValue(point.getCount())
        .build());
    for (SummaryDataPoint.ValueAtQuantile q : point.getQuantileValuesList()) {
      List<KeyValue> attributes = new ArrayList<>(pointAttributes);
      KeyValue quantileTag = KeyValue.newBuilder()
          .setKey("quantile")
          .setValue(AnyValue.newBuilder().setDoubleValue(q.getQuantile()).build())
          .build();
      attributes.add(quantileTag);
      toReturn.add(pointWithAnnotations(name, attributes, resourceAttrs, point.getTimeUnixNano())
          .setValue(q.getValue())
          .build());
    }
    return toReturn;
  }

  @NotNull
  private static List<KeyValue> replaceQuantileTag(List<KeyValue> pointAttributes) {
    if (pointAttributes.isEmpty()) return pointAttributes;

    List<KeyValue> modifiableAttributes = new ArrayList<>();
    for (KeyValue pointAttribute : pointAttributes) {
      if (pointAttribute.getKey().equals("quantile")) {
        modifiableAttributes.add(KeyValue.newBuilder()
            .setKey("_quantile")
            .setValue(pointAttribute.getValue())
            .build());
      } else {
        modifiableAttributes.add(pointAttribute);
      }
    }
    return modifiableAttributes;
  }

  @NotNull
  private static ReportPoint.Builder pointWithAnnotations(String name, List<KeyValue> pointAttributes, List<KeyValue> resourceAttrs, long timeInNs) {
    ReportPoint.Builder builder = ReportPoint.newBuilder().setMetric(name);
    Map<String, String> annotations = new HashMap<>();
    List<KeyValue> otlpAttributes = Stream.of(resourceAttrs, pointAttributes)
          .flatMap(Collection::stream).collect(Collectors.toList());

    for (Annotation a : OtlpTraceUtils.annotationsFromAttributes(otlpAttributes)) {
      annotations.put(a.getKey(), a.getValue());
    }
    builder.setAnnotations(annotations);
    builder.setTimestamp(TimeUnit.NANOSECONDS.toMillis(timeInNs));
    return builder;
  }

  static List<BucketHistogramDataPoint> fromOtelHistogram(Histogram histogram) {
    List<BucketHistogramDataPoint> result = new ArrayList<>(histogram.getDataPointsCount());
    for (HistogramDataPoint dataPoint : histogram.getDataPointsList()) {
      result.add(fromOtelHistogramDataPoint(dataPoint));
    }
    return result;
  }

  static BucketHistogramDataPoint fromOtelHistogramDataPoint(HistogramDataPoint dataPoint) {
    return new BucketHistogramDataPoint(
        dataPoint.getBucketCountsList(),
        dataPoint.getExplicitBoundsList(),
        dataPoint.getAttributesList(),
        dataPoint.getTimeUnixNano());
  }

  static List<BucketHistogramDataPoint> fromOtelExponentialHistogram(
      ExponentialHistogram histogram) {
    List<BucketHistogramDataPoint> result = new ArrayList<>(histogram.getDataPointsCount());
    for (ExponentialHistogramDataPoint dataPoint : histogram.getDataPointsList()) {
      result.add(fromOtelExponentialHistogramDataPoint(dataPoint));
    }
    return result;
  }

  static BucketHistogramDataPoint fromOtelExponentialHistogramDataPoint(
      ExponentialHistogramDataPoint dataPoint) {
    // base is the factor by which explicit bounds increase from bucket to bucket. This formula
    // comes from the documentation here:
    // https://github.com/open-telemetry/opentelemetry-proto/blob/8ba33cceb4a6704af68a4022d17868a7ac1d94f4/opentelemetry/proto/metrics/v1/metrics.proto#L487
    double base = Math.pow(2.0, Math.pow(2.0, -dataPoint.getScale()));

    // ExponentialHistogramDataPoints have buckets with negative explicit bounds, buckets with
    // positive explicit bounds, and a "zero" bucket. Our job is to merge these bucket groups into
    // a single list of buckets and explicit bounds.
    List<Long> negativeBucketCounts = dataPoint.getNegative().getBucketCountsList();
    List<Long> positiveBucketCounts = dataPoint.getPositive().getBucketCountsList();

    // The total number of buckets is the number of negative buckets + the number of positive
    // buckets + 1 for the zero bucket + 1 bucket for the largest positive explicit bound up to
    // positive infinity.
    int numBucketCounts = negativeBucketCounts.size() + 1 + positiveBucketCounts.size() + 1;

    List<Long> bucketCounts = new ArrayList<>(numBucketCounts);

    // The number of explicit bounds is always 1 less than the number of buckets. This is how
    // explicit bounds work. If you have 2 explicit bounds say {2.0, 5.0} then you have 3 buckets:
    // one for values less than 2.0; one for values between 2.0 and 5.0; and one for values greater
    // than 5.0.
    List<Double> explicitBounds = new ArrayList<>(numBucketCounts - 1);

    appendNegativeBucketsAndExplicitBounds(
        dataPoint.getNegative().getOffset(), base, negativeBucketCounts, bucketCounts, explicitBounds);
    appendZeroBucketAndExplicitBound(
        dataPoint.getPositive().getOffset(), base, dataPoint.getZeroCount(), bucketCounts, explicitBounds);
    appendPositiveBucketsAndExplicitBounds(
        dataPoint.getPositive().getOffset(), base, positiveBucketCounts, bucketCounts, explicitBounds);
    return new BucketHistogramDataPoint(
        bucketCounts,
        explicitBounds,
        dataPoint.getAttributesList(),
        dataPoint.getTimeUnixNano());
  }

  // appendNegativeBucketsAndExplicitBounds appends negative buckets and explicit bounds to
  // bucketCounts and explicitBounds respectively.
  static void appendNegativeBucketsAndExplicitBounds(
      int negativeOffset,
      double base,
      List<Long> negativeBucketCounts,
      List<Long> bucketCounts,
      List<Double> explicitBounds) {
    // The smallest negative explicit bound
    double le = -Math.pow(base, ((double) negativeOffset) + ((double) negativeBucketCounts.size()));

    // The first negativeBucketCount has a negative explicit bound with the smallest magnitude;
    // the last negativeBucketCount has a negative explicit bound with the largest magnitude.
    // Therefore, to go in order from smallest to largest explicit bound, we have to start with
    // the last element in the negativeBucketCounts array.
    for (int i = negativeBucketCounts.size() - 1; i >= 0; i--) {
      bucketCounts.add(negativeBucketCounts.get(i));
      le /= base; // We divide by base because our explicit bounds are getting smaller in magnitude as we go
      explicitBounds.add(le);
    }
  }

  // appendZeroBucketAndExplicitBound appends the "zero" bucket and explicit bound to bucketCounts
  // and explicitBounds respectively. The smallest positive explicit bound is base^positiveOffset.
  static void appendZeroBucketAndExplicitBound(
      int positiveOffset,
      double base,
      long zeroBucketCount,
      List<Long> bucketCounts,
      List<Double> explicitBounds) {
    bucketCounts.add(zeroBucketCount);

    // The explicit bound of the zeroBucketCount is the smallest positive explicit bound
    explicitBounds.add(Math.pow(base, positiveOffset));
  }

  // appendPositiveBucketsAndExplicitBounds appends positive buckets and explicit bounds to
  // bucketCounts and explicitBounds respectively. The smallest positive explicit bound is
  // base^positiveOffset.
  static void appendPositiveBucketsAndExplicitBounds(
      int positiveOffset,
      double base,
      List<Long> positiveBucketCounts,
      List<Long> bucketCounts,
      List<Double> explicitBounds) {
    double le = Math.pow(base, positiveOffset);
    for (Long positiveBucketCount : positiveBucketCounts) {
      bucketCounts.add(positiveBucketCount);
      le *= base;
      explicitBounds.add(le);
    }
    // Last bucket for positive infinity is always 0.
    bucketCounts.add(0L);
  }

  private static class BucketHistogramDataPoint {
    private final List<Long> bucketCounts;
    private final List<Double> explicitBounds;
    private final List<KeyValue> attributesList;
    private final long timeUnixNano;

    private BucketHistogramDataPoint(
        List<Long> bucketCounts,
        List<Double> explicitBounds,
        List<KeyValue> attributesList,
        long timeUnixNano) {
      this.bucketCounts = bucketCounts;
      this.explicitBounds = explicitBounds;
      this.attributesList = attributesList;
      this.timeUnixNano = timeUnixNano;
    }

    List<Long> getBucketCounts() {
      return bucketCounts;
    }

    List<Double> getExplicitBounds() {
      return explicitBounds;
    }

    List<KeyValue> getAttributesList() {
      return attributesList;
    }

    long getTimeUnixNano() {
      return timeUnixNano;
    }
  }

}
