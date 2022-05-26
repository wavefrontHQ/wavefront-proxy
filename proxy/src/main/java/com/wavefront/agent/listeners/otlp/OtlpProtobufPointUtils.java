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


public class OtlpProtobufPointUtils {
  public final static Logger OTLP_DATA_LOGGER = Logger.getLogger("OTLPDataLogger");
  public static final int MILLIS_IN_MINUTE = 60 * 1000;
  public static final int MILLIS_IN_HOUR = 60 * 60 * 1000;
  public static final int MILLIS_IN_DAY = 24 * 60 * 60 * 1000;

  public static void exportToWavefront(ExportMetricsServiceRequest request,
                                       ReportableEntityHandler<ReportPoint, String> pointHandler,
                                       ReportableEntityHandler<ReportPoint, String> histogramHandler,
                                       @Nullable Supplier<ReportableEntityPreprocessor> preprocessorSupplier, String defaultSource) {
    ReportableEntityPreprocessor preprocessor = null;
    if (preprocessorSupplier != null) {
      preprocessor = preprocessorSupplier.get();
    }

    for (ReportPoint point : fromOtlpRequest(request, preprocessor, defaultSource)) {
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
                                                   String defaultSource) {
    List<ReportPoint> wfPoints = Lists.newArrayList();

    for (ResourceMetrics resourceMetrics : request.getResourceMetricsList()) {
      Resource resource = resourceMetrics.getResource();
      Pair<String, List<KeyValue>> sourceAndResourceAttrs =
          OtlpProtobufUtils.sourceFromAttributes(resource.getAttributesList(), defaultSource);
      OTLP_DATA_LOGGER.finest(() -> "Inbound OTLP Resource: " + resource);
      for (ScopeMetrics scopeMetrics :
          resourceMetrics.getScopeMetricsList()) {
        OTLP_DATA_LOGGER.finest(() -> "Inbound OTLP Instrumentation Scope: " +
            scopeMetrics.getScope());
        for (Metric otlpMetric : scopeMetrics.getMetricsList()) {
          OTLP_DATA_LOGGER.finest(() -> "Inbound OTLP Metric: " + otlpMetric);
          List<ReportPoint> points = transform(otlpMetric, sourceAndResourceAttrs._2,
              preprocessor, sourceAndResourceAttrs._1);
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
      points.addAll(transformHistogram(otlpMetric.getName(), otlpMetric.getHistogram(),
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

  private static List<ReportPoint> transformHistogram(String name, Histogram histogram, List<KeyValue> resourceAttrs) {

    switch (histogram.getAggregationTemporality()) {
      case AGGREGATION_TEMPORALITY_CUMULATIVE:
        return transformCumulativeHistogram(name, histogram, resourceAttrs);
      case AGGREGATION_TEMPORALITY_DELTA:
        return transformDeltaHistogram(name, histogram, resourceAttrs);
      default:
        throw new IllegalArgumentException("OTel: histogram with unsupported aggregation temporality "
            + histogram.getAggregationTemporality().name());
    }
  }

  private static List<ReportPoint> transformDeltaHistogram(String name, Histogram histogram, List<KeyValue> resourceAttrs) {
    List<ReportPoint> reportPoints = new ArrayList<>();
    for (HistogramDataPoint point : histogram.getDataPointsList()) {
      reportPoints.addAll(transformDeltaHistogramDataPoint(name, point, resourceAttrs));
    }

    return reportPoints;
  }

  private static List<ReportPoint> transformCumulativeHistogram(String name, Histogram histogram, List<KeyValue> resourceAttrs) {

    List<ReportPoint> reportPoints = new ArrayList<>();
    for (HistogramDataPoint point : histogram.getDataPointsList()) {
      reportPoints.addAll(transformCumulativeHistogramDataPoint(name, point, resourceAttrs));
    }

    return reportPoints;
  }

  private static List<ReportPoint> transformDeltaHistogramDataPoint(String name,
                                                                    HistogramDataPoint point, List<KeyValue> resourceAttrs) {
    if (point.getExplicitBoundsCount() != point.getBucketCountsCount() - 1) {
      throw new IllegalArgumentException("OTel: histogram " + name + ": Explicit bounds count " +
          "should be one less than bucket count. ExplicitBounds: " + point.getExplicitBoundsCount() +
          ", BucketCounts: " + point.getExplicitBoundsCount());
    }

    List<ReportPoint> reportPoints = new ArrayList<>();

    List<Double> bins = new ArrayList<>(point.getBucketCountsCount());
    List<Integer> counts = new ArrayList<>(point.getBucketCountsCount());

    for (int currentIndex = 0; currentIndex < point.getBucketCountsCount(); currentIndex++) {
      bins.add(getDeltaHistogramBound(point, currentIndex));
      counts.add((int) point.getBucketCounts(currentIndex));
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

  private static Double getDeltaHistogramBound(HistogramDataPoint point, int currentIndex) {
    if (point.getExplicitBoundsCount() == 0) {
      // As coded in the metric exporter(OpenTelemetry Collector)
      return 0.0;
    }
    if (currentIndex == 0) {
      return point.getExplicitBounds(currentIndex);
    } else if (currentIndex == point.getExplicitBoundsCount()) {
      return point.getExplicitBounds(currentIndex - 1);
    }
    return (point.getExplicitBounds(currentIndex - 1) +
        point.getExplicitBounds(currentIndex)) / 2.0;
  }

  private static List<ReportPoint> transformCumulativeHistogramDataPoint(String name,
                                                                         HistogramDataPoint point, List<KeyValue> resourceAttrs) {
    if (point.getExplicitBoundsCount() != point.getBucketCountsCount() - 1) {
      throw new IllegalArgumentException("OTel: histogram " + name + ": Explicit bounds count " +
          "should be one less than bucket count. ExplicitBounds: " + point.getExplicitBoundsCount() +
          ", BucketCounts: " + point.getExplicitBoundsCount());
    }

    List<ReportPoint> reportPoints = new ArrayList<>(point.getBucketCountsCount());
    int currentIndex = 0;
    long cumulativeBucketCount = 0;
    for (; currentIndex < point.getExplicitBoundsCount(); currentIndex++) {
      cumulativeBucketCount += point.getBucketCounts(currentIndex);
      // we have to create a new builder every time as the annotations are getting appended after
      // each iteration
      ReportPoint rp = pointWithAnnotations(name, point.getAttributesList(), resourceAttrs,
          point.getTimeUnixNano())
          .setValue(cumulativeBucketCount)
          .build();
      handleDupAnnotation(rp);
      rp.getAnnotations().put("le", String.valueOf(point.getExplicitBounds(currentIndex)));
      reportPoints.add(rp);
    }

    ReportPoint rp = pointWithAnnotations(name, point.getAttributesList(), resourceAttrs,
        point.getTimeUnixNano())
        .setValue(cumulativeBucketCount + point.getBucketCounts(currentIndex))
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
    return pointWithAnnotations(name, point.getAttributesList(), resourceAttrs, point.getTimeUnixNano())
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
    for (Annotation a : OtlpProtobufUtils.annotationsFromAttributes(otlpAttributes)) {
      annotations.put(a.getKey(), a.getValue());
    }
    builder.setAnnotations(annotations);
    builder.setTimestamp(TimeUnit.NANOSECONDS.toMillis(timeInNs));
    return builder;
  }
}
