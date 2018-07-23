package com.wavefront.metrics;

import com.fasterxml.jackson.core.JsonEncoding;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.util.TokenBuffer;
import com.tdunning.math.stats.Centroid;
import com.wavefront.common.MetricsToTimeseries;
import com.wavefront.common.MinuteBin;
import com.wavefront.common.Pair;
import com.wavefront.common.TaggedMetricName;
import com.yammer.metrics.core.Clock;
import com.yammer.metrics.core.Counter;
import com.yammer.metrics.core.Gauge;
import com.yammer.metrics.core.Histogram;
import com.yammer.metrics.core.Metered;
import com.yammer.metrics.core.Metric;
import com.yammer.metrics.core.MetricName;
import com.yammer.metrics.core.MetricProcessor;
import com.yammer.metrics.core.MetricsRegistry;
import com.yammer.metrics.core.SafeVirtualMachineMetrics;
import com.yammer.metrics.core.Sampling;
import com.yammer.metrics.core.Summarizable;
import com.yammer.metrics.core.Timer;
import com.yammer.metrics.core.VirtualMachineMetrics;
import com.yammer.metrics.core.WavefrontHistogram;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.Validate;

import java.io.IOException;
import java.io.OutputStream;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.MissingResourceException;
import java.util.ResourceBundle;
import java.util.SortedMap;

import javax.annotation.Nullable;

import static com.wavefront.common.MetricsToTimeseries.sanitize;

/**
 * Generator of metrics as a JSON node and outputting it to an output stream or returning a json node.
 *
 * @author Clement Pang (clement@wavefront.com)
 */
public abstract class JsonMetricsGenerator {

  private static final JsonFactory factory = new JsonFactory();

  private static final Clock clock = Clock.defaultClock();
  private static final VirtualMachineMetrics vm = SafeVirtualMachineMetrics.getInstance();

  public static void generateJsonMetrics(OutputStream outputStream, MetricsRegistry registry, boolean includeVMMetrics,
                                         boolean includeBuildMetrics, boolean clearMetrics, MetricTranslator metricTranslator) throws IOException {
    JsonGenerator json = factory.createGenerator(outputStream, JsonEncoding.UTF8);
    writeJson(json, registry, includeVMMetrics, includeBuildMetrics, clearMetrics, null, metricTranslator);
  }

  public static JsonNode generateJsonMetrics(MetricsRegistry registry, boolean includeVMMetrics,
                                             boolean includeBuildMetrics, boolean clearMetrics) throws IOException {
    return generateJsonMetrics(registry, includeVMMetrics, includeBuildMetrics, clearMetrics, null, null);
  }

  public static JsonNode generateJsonMetrics(MetricsRegistry registry, boolean includeVMMetrics,
                                             boolean includeBuildMetrics, boolean clearMetrics,
                                             @Nullable Map<String, String> pointTags,
                                             @Nullable MetricTranslator metricTranslator) throws IOException {
    TokenBuffer t = new TokenBuffer(new ObjectMapper(), false);
    writeJson(t, registry, includeVMMetrics, includeBuildMetrics, clearMetrics, pointTags, metricTranslator);
    JsonParser parser = t.asParser();
    return parser.readValueAsTree();
  }

  static final class Context {
    final boolean showFullSamples;
    final JsonGenerator json;

    Context(JsonGenerator json, boolean showFullSamples) {
      this.json = json;
      this.showFullSamples = showFullSamples;
    }
  }

  public static void writeJson(JsonGenerator json, MetricsRegistry registry, boolean includeVMMetrics,
                               boolean includeBuildMetrics, boolean clearMetrics) throws IOException {
    writeJson(json, registry, includeVMMetrics, includeBuildMetrics, clearMetrics, null, null);
  }

  public static void writeJson(JsonGenerator json, MetricsRegistry registry, boolean includeVMMetrics,
                               boolean includeBuildMetrics, boolean clearMetrics,
                               @Nullable Map<String, String> pointTags,
                               @Nullable MetricTranslator metricTranslator) throws IOException {
    json.writeStartObject();
    if (includeVMMetrics) {
      writeVmMetrics(json, pointTags);
    }
    if (includeBuildMetrics) {
      try {
        writeBuildMetrics(ResourceBundle.getBundle("build"), json, pointTags);
      } catch (MissingResourceException ignored) {
      }
    }
    writeRegularMetrics(new Processor(clearMetrics), json, registry, false, pointTags, metricTranslator);
    json.writeEndObject();
    json.close();
  }

  private static void writeBuildMetrics(ResourceBundle props, JsonGenerator json,
                                        Map<String, String> pointTags) throws IOException {
    json.writeFieldName("build");
    if (pointTags != null) {
      json.writeStartObject();
      writeTags(json, pointTags);
      json.writeFieldName("value");
    }
    json.writeStartObject();
    if (props.containsKey("build.version")) {
      // attempt to make a version string
      int version = extractVersion(props.getString("build.version"));
      if (version != 0) {
        json.writeNumberField("version", version);
      }
      json.writeStringField("version_raw", props.getString("build.version"));
    }
    if (props.containsKey("build.commit")) {
      json.writeStringField("commit", props.getString("build.commit"));
    }
    if (props.containsKey("build.hostname")) {
      json.writeStringField("build_host", props.getString("build.hostname"));
    }
    if (props.containsKey("maven.build.timestamp")) {
      if (StringUtils.isNumeric(props.getString("maven.build.timestamp"))) {
        json.writeNumberField("timestamp", Long.valueOf(props.getString("maven.build.timestamp")));
      }
      json.writeStringField("timestamp_raw", props.getString("maven.build.timestamp"));
    }
    json.writeEndObject();
    if (pointTags != null) {
      json.writeEndObject();
    }
  }

  static int extractVersion(String versionStr) {
    int version = 0;
    String[] components = versionStr.split("\\.");
    for (int i = 0; i < Math.min(3, components.length); i++) {
      String component = components[i];
      if (StringUtils.isNotBlank(component) && StringUtils.isNumeric(component)) {
        version *= 1000; // we'll assume this will fit. 3.123.0 will become 3123000.
        version += Integer.valueOf(component);
      } else {
        version = 0; // not actually a convertable name (probably something with SNAPSHOT).
        break;
      }
    }
    if (components.length == 2) {
      version *= 1000;
    } else if (components.length == 1) {
      version *= 1000000; // make sure 3   outputs 3000000
    }
    return version;
  }

  private static void mergeMapIntoJson(JsonGenerator jsonGenerator, Map<String, Double> metrics) throws IOException {
    for (Map.Entry<String, Double> entry : metrics.entrySet()) {
      jsonGenerator.writeNumberField(entry.getKey(), entry.getValue());
    }
  }

  private static void writeVmMetrics(JsonGenerator json, @Nullable Map<String, String> pointTags) throws IOException {
    json.writeFieldName("jvm");  // jvm
    if (pointTags != null) {
      json.writeStartObject();
      writeTags(json, pointTags);
      json.writeFieldName("value");
    }
    json.writeStartObject();
    {
      json.writeFieldName("vm");  // jvm.vm
      json.writeStartObject();
      {
        json.writeStringField("name", vm.name());
        json.writeStringField("version", vm.version());
      }
      json.writeEndObject();

      json.writeFieldName("memory");  // jvm.memory
      json.writeStartObject();
      {
        mergeMapIntoJson(json, MetricsToTimeseries.memoryMetrics(vm));
        json.writeFieldName("memory_pool_usages");  // jvm.memory.memory_pool_usages
        json.writeStartObject();
        {
          mergeMapIntoJson(json, MetricsToTimeseries.memoryPoolsMetrics(vm));
        }
        json.writeEndObject();
      }
      json.writeEndObject();

      final Map<String, VirtualMachineMetrics.BufferPoolStats> bufferPoolStats = vm.getBufferPoolStats();
      if (!bufferPoolStats.isEmpty()) {
        json.writeFieldName("buffers");  // jvm.buffers
        json.writeStartObject();
        {
          json.writeFieldName("direct");  // jvm.buffers.direct
          json.writeStartObject();
          {
            mergeMapIntoJson(json, MetricsToTimeseries.buffersMetrics(bufferPoolStats.get("direct")));
          }
          json.writeEndObject();

          json.writeFieldName("mapped");  // jvm.buffers.mapped
          json.writeStartObject();
          {
            mergeMapIntoJson(json, MetricsToTimeseries.buffersMetrics(bufferPoolStats.get("mapped")));
          }
          json.writeEndObject();
        }
        json.writeEndObject();
      }

      mergeMapIntoJson(json, MetricsToTimeseries.vmMetrics(vm));  // jvm.<vm_metric>
      json.writeNumberField("current_time", clock.time());

      json.writeFieldName("thread-states");  // jvm.thread-states
      json.writeStartObject();
      {
        mergeMapIntoJson(json, MetricsToTimeseries.threadStateMetrics(vm));
      }
      json.writeEndObject();

      json.writeFieldName("garbage-collectors");  // jvm.garbage-collectors
      json.writeStartObject();
      {
        for (Map.Entry<String, VirtualMachineMetrics.GarbageCollectorStats> entry : vm.garbageCollectors()
            .entrySet()) {
          json.writeFieldName(entry.getKey());  // jvm.garbage-collectors.<gc_id>
          json.writeStartObject();
          {
            mergeMapIntoJson(json, MetricsToTimeseries.gcMetrics(entry.getValue()));
          }
          json.writeEndObject();
        }
      }
      json.writeEndObject();
    }
    json.writeEndObject();
    if (pointTags != null) {
      json.writeEndObject();
    }
  }

  private static void writeTags(JsonGenerator json, Map<String, String> pointTags) throws IOException {
    Validate.notNull(pointTags, "pointTags argument can't be null!");
    json.writeFieldName("tags");
    json.writeStartObject();
    for (Map.Entry<String, String> tagEntry : pointTags.entrySet()) {
      json.writeStringField(tagEntry.getKey(), tagEntry.getValue());
    }
    json.writeEndObject();
  }

  public static void writeRegularMetrics(Processor processor, JsonGenerator json, MetricsRegistry registry,
                                         boolean showFullSamples) throws IOException {
    writeRegularMetrics(processor, json, registry, showFullSamples, null);
  }

  public static void writeRegularMetrics(Processor processor, JsonGenerator json,
                                         MetricsRegistry registry, boolean showFullSamples,
                                         @Nullable Map<String, String> pointTags) throws IOException {
    writeRegularMetrics(processor, json, registry, showFullSamples, pointTags, null);
  }

  public static void writeRegularMetrics(Processor processor, JsonGenerator json,
                                         MetricsRegistry registry, boolean showFullSamples,
                                         @Nullable Map<String, String> pointTags,
                                         @Nullable MetricTranslator metricTranslator) throws IOException {
    for (Map.Entry<String, SortedMap<MetricName, Metric>> entry : registry.groupedMetrics().entrySet()) {
      for (Map.Entry<MetricName, Metric> subEntry : entry.getValue().entrySet()) {
        MetricName key = subEntry.getKey();
        Metric value = subEntry.getValue();
        if (metricTranslator != null) {
          Pair<MetricName, Metric> pair = metricTranslator.apply(Pair.of(key, value));
          if (pair == null) continue;
          key = pair._1;
          value = pair._2;
        }
        boolean closeObjectRequired = false;
        if (key instanceof TaggedMetricName || pointTags != null) {
          closeObjectRequired = true;
          // write the hashcode since we need to support metrics with the same name but with different tags.
          // the server will remove the suffix.
          json.writeFieldName(sanitize(key) + "$" + subEntry.hashCode());
          // write out the tags separately
          // instead of metricName: {...}
          // we write
          // metricName_hashCode: {
          //   tags: {
          //     tagK: tagV,...
          //   },
          //   value: {...}
          // }
          //
          json.writeStartObject();
          json.writeFieldName("tags");
          json.writeStartObject();
          Map<String, String> tags = new HashMap<>();
          if (pointTags != null) {
            tags.putAll(pointTags);
          }
          if (key instanceof TaggedMetricName) {
            tags.putAll(((TaggedMetricName) key).getTags());
          }
          for (Map.Entry<String, String> tagEntry : tags.entrySet()) {
            json.writeStringField(tagEntry.getKey(), tagEntry.getValue());
          }
          json.writeEndObject();
          json.writeFieldName("value");
        } else {
          json.writeFieldName(sanitize(key));
        }
        try {
          value.processWith(processor, key, new Context(json, showFullSamples));
        } catch (Exception e) {
          e.printStackTrace();
        }
        // need to close the object as well.
        if (closeObjectRequired) {
          json.writeEndObject();
        }
      }
    }
  }

  static final class Processor implements MetricProcessor<Context> {

    private final boolean clear;

    public Processor(boolean clear) {
      this.clear = clear;
    }

    private void internalProcessYammerHistogram(Histogram histogram, Context context) throws Exception {
      final JsonGenerator json = context.json;
      json.writeStartObject();
      {
        json.writeNumberField("count", histogram.count());
        writeSummarizable(histogram, json);
        writeSampling(histogram, json);
        if (context.showFullSamples) {
          json.writeObjectField("values", histogram.getSnapshot().getValues());
        }
        if (clear) histogram.clear();
      }
      json.writeEndObject();
    }

    private void internalProcessWavefrontHistogram(WavefrontHistogram hist, Context context) throws Exception {
      final JsonGenerator json = context.json;
      json.writeStartObject();
      json.writeArrayFieldStart("bins");
      for (MinuteBin bin : hist.bins(clear)) {

        final Collection<Centroid> centroids = bin.getDist().centroids();

        json.writeStartObject();
        // Count
        json.writeNumberField("count", bin.getDist().size());
        // Start
        json.writeNumberField("startMillis", bin.getMinuteMillis());
        // Duration
        json.writeNumberField("durationMillis", 60 * 1000);
        // Means
        json.writeArrayFieldStart("means");
        for (Centroid c : centroids) {
          json.writeNumber(c.mean());
        }
        json.writeEndArray();
        // Counts
        json.writeArrayFieldStart("counts");
        for (Centroid c : centroids) {
          json.writeNumber(c.count());
        }
        json.writeEndArray();

        json.writeEndObject();
      }
      json.writeEndArray();
      json.writeEndObject();
    }

    @Override
    public void processHistogram(MetricName name, Histogram histogram, Context context) throws Exception {
      if (histogram instanceof WavefrontHistogram) {
        internalProcessWavefrontHistogram((WavefrontHistogram) histogram, context);
      } else /*Treat as standard yammer histogram */ {
        internalProcessYammerHistogram(histogram, context);
      }
    }

    @Override
    public void processCounter(MetricName name, Counter counter, Context context) throws Exception {
      final JsonGenerator json = context.json;
      json.writeNumber(counter.count());
    }

    @Override
    public void processGauge(MetricName name, Gauge<?> gauge, Context context) throws Exception {
      final JsonGenerator json = context.json;
      Object gaugeValue = evaluateGauge(gauge);
      if (gaugeValue != null) {
        json.writeObject(gaugeValue);
      } else {
        json.writeNull();
      }
    }

    @Override
    public void processMeter(MetricName name, Metered meter, Context context) throws Exception {
      final JsonGenerator json = context.json;
      json.writeStartObject();
      {
        writeMeteredFields(meter, json);
      }
      json.writeEndObject();
    }

    @Override
    public void processTimer(MetricName name, Timer timer, Context context) throws Exception {
      final JsonGenerator json = context.json;
      json.writeStartObject();
      {
        json.writeFieldName("duration");
        json.writeStartObject();
        {
          json.writeStringField("unit", timer.durationUnit().toString().toLowerCase());
          writeSummarizable(timer, json);
          writeSampling(timer, json);
          if (context.showFullSamples) {
            json.writeObjectField("values", timer.getSnapshot().getValues());
          }
        }
        json.writeEndObject();

        json.writeFieldName("rate");
        json.writeStartObject();
        {
          writeMeteredFields(timer, json);
        }
        json.writeEndObject();
      }
      json.writeEndObject();
      if (clear) timer.clear();
    }
  }

  private static Object evaluateGauge(Gauge<?> gauge) {
    try {
      return gauge.value();
    } catch (RuntimeException e) {
      return "error reading gauge: " + e.getMessage();
    }
  }

  private static void writeSummarizable(Summarizable metric, JsonGenerator json) throws IOException {
    for (Map.Entry<String, Double> entry : MetricsToTimeseries.explodeSummarizable(metric).entrySet()) {
      json.writeNumberField(entry.getKey(), entry.getValue());
    }
  }

  private static void writeSampling(Sampling metric, JsonGenerator json) throws IOException {
    for (Map.Entry<String, Double> entry : MetricsToTimeseries.explodeSampling(metric).entrySet()) {
      json.writeNumberField(entry.getKey(), entry.getValue());
    }
  }

  private static void writeMeteredFields(Metered metered, JsonGenerator json) throws IOException {
    for (Map.Entry<String, Double> entry : MetricsToTimeseries.explodeMetered(metered).entrySet()) {
      json.writeNumberField(entry.getKey(), entry.getValue());
    }
  }
}
