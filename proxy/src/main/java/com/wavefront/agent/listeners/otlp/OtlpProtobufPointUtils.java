package com.wavefront.agent.listeners.otlp;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Lists;
import com.wavefront.agent.handlers.ReportableEntityHandler;
import com.wavefront.agent.preprocessor.ReportableEntityPreprocessor;
import com.wavefront.common.MetricConstants;
import io.opentelemetry.proto.collector.metrics.v1.ExportMetricsServiceRequest;
import io.opentelemetry.proto.common.v1.KeyValue;
import io.opentelemetry.proto.metrics.v1.Gauge;
import io.opentelemetry.proto.metrics.v1.InstrumentationLibraryMetrics;
import io.opentelemetry.proto.metrics.v1.Metric;
import io.opentelemetry.proto.metrics.v1.NumberDataPoint;
import io.opentelemetry.proto.metrics.v1.ResourceMetrics;
import io.opentelemetry.proto.metrics.v1.Sum;
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
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class OtlpProtobufPointUtils {
  public final static Logger OTLP_DATA_LOGGER = Logger.getLogger("OTLPDataLogger");

  public static void exportToWavefront(ExportMetricsServiceRequest request,
                                       ReportableEntityHandler<ReportPoint, String> handler,
                                       @Nullable Supplier<ReportableEntityPreprocessor> preprocessorSupplier) {
    ReportableEntityPreprocessor preprocessor = null;
    if (preprocessorSupplier != null) {
      preprocessor = preprocessorSupplier.get();
    }

    for (ReportPoint point : fromOtlpRequest(request, preprocessor)) {
      // TODO: handle sampler
      if (!wasFilteredByPreprocessor(point, handler, preprocessor)) {
        handler.report(point);
      }
    }
  }

  private static List<ReportPoint> fromOtlpRequest(ExportMetricsServiceRequest request,
                                                   @Nullable ReportableEntityPreprocessor preprocessor) {
    List<ReportPoint> wfPoints = Lists.newArrayList();

    for (ResourceMetrics resourceMetrics : request.getResourceMetricsList()) {
      Resource resource = resourceMetrics.getResource();
      if (OTLP_DATA_LOGGER.isLoggable(Level.FINEST)) {
        OTLP_DATA_LOGGER.info("Inbound OTLP Resource: " + resource);
      }
      for (InstrumentationLibraryMetrics instrumentationLibraryMetrics :
          resourceMetrics.getInstrumentationLibraryMetricsList()) {
        if (OTLP_DATA_LOGGER.isLoggable(Level.FINEST)) {
          OTLP_DATA_LOGGER.info("Inbound OTLP Instrumentation Library: " +
              instrumentationLibraryMetrics.getInstrumentationLibrary());
        }
        for (Metric otlpMetric : instrumentationLibraryMetrics.getMetricsList()) {
          List<ReportPoint> points = transform(otlpMetric, resource.getAttributesList(),
              preprocessor);
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
                                            ReportableEntityPreprocessor preprocessor) {
    List<ReportPoint> points = new ArrayList<>();
    if (otlpMetric.hasGauge()) {
      for (ReportPoint point : transformGauge(otlpMetric.getName(), otlpMetric.getGauge(), resourceAttrs)) {
        // apply preprocessor
        if (preprocessor != null) {
          preprocessor.forReportPoint().transform(point);
        }
        points.add(point);
      }
    } else if (otlpMetric.hasSum()) {
      String prefix = "";
      switch (otlpMetric.getSum().getAggregationTemporality()) {
        case AGGREGATION_TEMPORALITY_CUMULATIVE:
          // no prefix
          break;
        case AGGREGATION_TEMPORALITY_DELTA:
          prefix = MetricConstants.DELTA_PREFIX;
          break;
        default:
          throw new IllegalArgumentException("OTel: sum with unsupported aggregation temporality " + otlpMetric.getSum().getAggregationTemporality().name());
      }
      for (ReportPoint point : transformSum(prefix + otlpMetric.getName(), otlpMetric.getSum(), resourceAttrs)) {
        // apply preprocessor
        if (preprocessor != null) {
          preprocessor.forReportPoint().transform(point);
        }
        points.add(point);
      }
    } else {
      throw new IllegalArgumentException("Otel: unsupported metric type for " + otlpMetric.getName());
    }

    return points;
  }

  private static List<ReportPoint> transformSum(String name, Sum sum, List<KeyValue> resourceAttrs) {
    if (sum.getDataPointsCount() == 0) {
      throw new IllegalArgumentException("OTel: sum with no data points");
    }

    List<ReportPoint> points = new ArrayList<>(sum.getDataPointsCount());
    for (NumberDataPoint p : sum.getDataPointsList()) {
      points.add(transformNumberDataPoint(name, p, resourceAttrs));
    }
    return points;
  }

  private static Collection<ReportPoint> transformGauge(String name, Gauge gauge, List<KeyValue> resourceAttrs) {
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
    ReportPoint.Builder toReturn = ReportPoint.newBuilder().setMetric(name);
    Map<String, String> annotations = new HashMap<>();
    List<KeyValue> otlpAttributes = Stream.of(resourceAttrs, point.getAttributesList())
        .flatMap(Collection::stream).collect(Collectors.toList());
    for (Annotation a : OtlpProtobufUtils.annotationsFromAttributes(otlpAttributes)) {
      annotations.put(a.getKey(), a.getValue());
    }

    toReturn.setTimestamp(TimeUnit.NANOSECONDS.toMillis(point.getTimeUnixNano()))
        .setValue(point.getAsDouble())
        .setAnnotations(annotations);
    return toReturn.build();
  }
}