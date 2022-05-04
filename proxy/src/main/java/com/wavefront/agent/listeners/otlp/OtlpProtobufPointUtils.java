package com.wavefront.agent.listeners.otlp;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Lists;
import com.wavefront.agent.handlers.ReportableEntityHandler;
import com.wavefront.agent.preprocessor.ReportableEntityPreprocessor;
import com.wavefront.common.MetricConstants;
import com.wavefront.sdk.common.Pair;

import io.opentelemetry.proto.collector.metrics.v1.ExportMetricsServiceRequest;
import io.opentelemetry.proto.common.v1.AnyValue;
import io.opentelemetry.proto.common.v1.KeyValue;
import io.opentelemetry.proto.metrics.v1.Gauge;
import io.opentelemetry.proto.metrics.v1.InstrumentationLibraryMetrics;
import io.opentelemetry.proto.metrics.v1.Metric;
import io.opentelemetry.proto.metrics.v1.NumberDataPoint;
import io.opentelemetry.proto.metrics.v1.ResourceMetrics;
import io.opentelemetry.proto.metrics.v1.Sum;
import io.opentelemetry.proto.metrics.v1.Summary;
import io.opentelemetry.proto.metrics.v1.SummaryDataPoint;
import io.opentelemetry.proto.resource.v1.Resource;
import org.jetbrains.annotations.NotNull;
import wavefront.report.Annotation;
import wavefront.report.ReportPoint;

import javax.annotation.Nullable;

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

public class OtlpProtobufPointUtils {
  public final static Logger OTLP_DATA_LOGGER = Logger.getLogger("OTLPDataLogger");

  public static void exportToWavefront(ExportMetricsServiceRequest request,
                                       ReportableEntityHandler<ReportPoint, String> handler,
                                       @Nullable Supplier<ReportableEntityPreprocessor> preprocessorSupplier, String defaultSource) {
    ReportableEntityPreprocessor preprocessor = null;
    if (preprocessorSupplier != null) {
      preprocessor = preprocessorSupplier.get();
    }

    for (ReportPoint point : fromOtlpRequest(request, preprocessor, defaultSource)) {
      // TODO: handle sampler
      if (!wasFilteredByPreprocessor(point, handler, preprocessor)) {
        handler.report(point);
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
      for (InstrumentationLibraryMetrics instrumentationLibraryMetrics :
          resourceMetrics.getInstrumentationLibraryMetricsList()) {
        OTLP_DATA_LOGGER.finest(() -> "Inbound OTLP Instrumentation Library: " +
            instrumentationLibraryMetrics.getInstrumentationLibrary());
        for (Metric otlpMetric : instrumentationLibraryMetrics.getMetricsList()) {
          List<ReportPoint> points = transform(otlpMetric, sourceAndResourceAttrs._2,
              preprocessor, sourceAndResourceAttrs._1);
          OTLP_DATA_LOGGER.finest(() -> "Inbound OTLP Metric: " + otlpMetric);
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
    return pointWithAnnotations(name, point.getAttributesList(), resourceAttrs)
        .setTimestamp(TimeUnit.NANOSECONDS.toMillis(point.getTimeUnixNano()))
        .setValue(point.getAsDouble())
        .build();
  }

  @NotNull
  private static List<ReportPoint> transformSummaryDataPoint(String name, SummaryDataPoint point, List<KeyValue> resourceAttrs) {
    List<ReportPoint> toReturn = new ArrayList<>();
    List<KeyValue> pointAttributes = replaceQuantileTag(point.getAttributesList());
    toReturn.add(pointWithAnnotations(name + "_sum", pointAttributes, resourceAttrs)
        .setTimestamp(TimeUnit.NANOSECONDS.toMillis(point.getTimeUnixNano()))
        .setValue(point.getSum())
        .build());
    toReturn.add(pointWithAnnotations(name + "_count", pointAttributes, resourceAttrs)
        .setTimestamp(TimeUnit.NANOSECONDS.toMillis(point.getTimeUnixNano()))
        .setValue(point.getCount())
        .build());
    for (SummaryDataPoint.ValueAtQuantile q : point.getQuantileValuesList()) {
      List<KeyValue> attributes = new ArrayList<>(pointAttributes);
      KeyValue quantileTag = KeyValue.newBuilder()
          .setKey("quantile")
          .setValue(AnyValue.newBuilder().setDoubleValue(q.getQuantile()).build())
          .build();
      attributes.add(quantileTag);
      toReturn.add(pointWithAnnotations(name, attributes, resourceAttrs)
          .setTimestamp(TimeUnit.NANOSECONDS.toMillis(point.getTimeUnixNano()))
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
  private static ReportPoint.Builder pointWithAnnotations(String name, List<KeyValue> pointAttributes, List<KeyValue> resourceAttrs) {
    ReportPoint.Builder builder = ReportPoint.newBuilder().setMetric(name);
    Map<String, String> annotations = new HashMap<>();
    List<KeyValue> otlpAttributes = Stream.of(resourceAttrs, pointAttributes)
        .flatMap(Collection::stream).collect(Collectors.toList());
    for (Annotation a : OtlpProtobufUtils.annotationsFromAttributes(otlpAttributes)) {
      annotations.put(a.getKey(), a.getValue());
    }
    builder.setAnnotations(annotations);
    return builder;
  }
}
