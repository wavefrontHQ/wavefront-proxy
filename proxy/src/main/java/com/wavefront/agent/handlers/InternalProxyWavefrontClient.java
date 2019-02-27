package com.wavefront.agent.handlers;

import com.wavefront.common.Clock;
import com.wavefront.data.ReportableEntityType;
import com.wavefront.sdk.common.Pair;
import com.wavefront.sdk.common.WavefrontSender;
import com.wavefront.sdk.entities.histograms.HistogramGranularity;
import com.wavefront.sdk.entities.tracing.SpanLog;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import wavefront.report.Annotation;
import wavefront.report.Histogram;
import wavefront.report.HistogramType;
import wavefront.report.ReportPoint;
import wavefront.report.Span;

import static com.wavefront.agent.Utils.lazySupplier;

public class InternalProxyWavefrontClient implements WavefrontSender {
  private final ReportableEntityHandlerFactory handlerFactory;
  private final Supplier<ReportableEntityHandler<ReportPoint>> pointHandlerSupplier;
  private final Supplier<ReportableEntityHandler<ReportPoint>> histogramHandlerSupplier;
  private final Supplier<ReportableEntityHandler<Span>> spanHandlerSupplier;

  public InternalProxyWavefrontClient(ReportableEntityHandlerFactory handlerFactory) {
    this(handlerFactory, "internal_client");
  }

  @SuppressWarnings("unchecked")
  public InternalProxyWavefrontClient(ReportableEntityHandlerFactory handlerFactory1, String handle) {
    this.handlerFactory = handlerFactory1;
    this.pointHandlerSupplier = lazySupplier(() ->
        handlerFactory.getHandler(HandlerKey.of(ReportableEntityType.POINT, handle)));
    this.histogramHandlerSupplier = lazySupplier(() ->
        handlerFactory.getHandler(HandlerKey.of(ReportableEntityType.HISTOGRAM, handle)));
    this.spanHandlerSupplier = lazySupplier(() ->
        handlerFactory.getHandler(HandlerKey.of(ReportableEntityType.TRACE, handle)));
  }

  @Override
  public void flush() throws IOException {
    // noop
  }

  @Override
  public int getFailureCount() {
    return 0;
  }

  @Override
  public void sendDistribution(String name, List<Pair<Double, Integer>> centroids,
                               Set<HistogramGranularity> histogramGranularities, Long timestamp, String source,
                               Map<String, String> tags) throws IOException {
    final List<Double> bins = centroids.stream().map(x -> x._1).collect(Collectors.toList());
    final List<Integer> counts = centroids.stream().map(x -> x._2).collect(Collectors.toList());
    for (HistogramGranularity granularity : histogramGranularities) {
      int duration;
      switch (granularity) {
        case MINUTE:
          duration = 60000;
          break;
        case HOUR:
          duration = 3600000;
          break;
        case DAY:
          duration = 86400000;
          break;
        default:
          throw new IllegalArgumentException("Unknown granularity: " + granularity);
      }
      Histogram histogram = Histogram.newBuilder().
          setType(HistogramType.TDIGEST).
          setBins(bins).
          setCounts(counts).
          setDuration(duration).
          build();
      ReportPoint point = ReportPoint.newBuilder().
          setTable("unknown").
          setMetric(name).
          setValue(histogram).
          setTimestamp(timestamp).
          setHost(source).
          setAnnotations(tags).
          build();
      histogramHandlerSupplier.get().report(point);
    }
  }

  @Override
  public void sendMetric(String name, double value, Long timestamp, String source, Map<String, String> tags)
      throws IOException {
    // default to millis
    long timestampMillis = timestamp;
    if (timestamp < 10_000_000_000L) {
      // seconds
      timestampMillis = timestamp * 1000;
    } else if (timestamp < 10_000_000_000_000L) {
      // millis
      timestampMillis = timestamp;
    } else if (timestamp < 10_000_000_000_000_000L) {
      // micros
      timestampMillis = timestamp / 1000;
    } else if (timestamp <= 999_999_999_999_999_999L) {
      // nanos
      timestampMillis = timestamp / 1000_000;
    }

    final ReportPoint point = ReportPoint.newBuilder().
        setTable("unknown").
        setMetric(name).
        setValue(value).
        setTimestamp(timestamp == null ? Clock.now() : timestampMillis).
        setHost(source).
        setAnnotations(tags).
        build();
    pointHandlerSupplier.get().report(point);
  }

  @Override
  public void sendSpan(String name, long startMillis, long durationMillis, String source, UUID traceId, UUID spanId,
                       List<UUID> parents, List<UUID> followsFrom, List<Pair<String, String>> tags,
                       List<SpanLog> spanLogs) throws IOException {
    final List<Annotation> annotations = tags.stream().map(x -> new Annotation(x._1, x._2)).collect(Collectors.toList());
    final Span span = Span.newBuilder().
        setCustomer("unknown").
        setTraceId(traceId.toString()).
        setSpanId(spanId.toString()).
        setName(name).
        setSource(source).
        setStartMillis(startMillis).
        setDuration(durationMillis).
        setAnnotations(annotations).
        build();
    spanHandlerSupplier.get().report(span);
  }

  @Override
  public void close() throws IOException {
    // noop
  }
}
