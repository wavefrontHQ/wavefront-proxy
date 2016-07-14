package com.wavefront.metrics;

import com.fasterxml.jackson.core.JsonEncoding;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.util.TokenBuffer;
import com.wavefront.common.TaggedMetricName;
import com.yammer.metrics.core.*;
import com.yammer.metrics.stats.Snapshot;
import org.apache.commons.lang.StringUtils;

import java.io.IOException;
import java.io.OutputStream;
import java.util.Map;
import java.util.MissingResourceException;
import java.util.ResourceBundle;
import java.util.SortedMap;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;

/**
 * Generator of metrics as a JSON node and outputting it to an output stream or returning a json node.
 *
 * @author Clement Pang (clement@wavefront.com)
 */
public abstract class JsonMetricsGenerator {

  private static final JsonFactory factory = new JsonFactory();

  private static final Clock clock = Clock.defaultClock();
  private static final VirtualMachineMetrics vm = VirtualMachineMetrics.getInstance();

  public static void generateJsonMetrics(OutputStream outputStream, MetricsRegistry registry, boolean includeVMMetrics,
                                         boolean includeBuildMetrics, boolean clearMetrics)
      throws IOException {
    JsonGenerator json = factory.createGenerator(outputStream, JsonEncoding.UTF8);
    writeJson(json, registry, includeVMMetrics, includeBuildMetrics, clearMetrics);
  }

  public static JsonNode generateJsonMetrics(MetricsRegistry registry, boolean includeVMMetrics,
                                             boolean includeBuildMetrics, boolean clearMetrics) throws IOException {
    TokenBuffer t = new TokenBuffer(new ObjectMapper());
    writeJson(t, registry, includeVMMetrics, includeBuildMetrics, clearMetrics);
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
    json.writeStartObject();
    if (includeVMMetrics) {
      writeVmMetrics(json);
    }
    if (includeBuildMetrics) {
      try {
        writeBuildMetrics(ResourceBundle.getBundle("build"), json);
      } catch (MissingResourceException ignored) {
      }
    }
    writeRegularMetrics(new Processor(clearMetrics), json, registry, false);
    json.writeEndObject();
    json.close();
  }

  private static void writeBuildMetrics(ResourceBundle props, JsonGenerator json) throws IOException {
    json.writeFieldName("build");
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

  private static void writeVmMetrics(JsonGenerator json) throws IOException {
    json.writeFieldName("jvm");
    json.writeStartObject();
    {
      json.writeFieldName("vm");
      json.writeStartObject();
      {
        json.writeStringField("name", vm.name());
        json.writeStringField("version", vm.version());
      }
      json.writeEndObject();

      json.writeFieldName("memory");
      json.writeStartObject();
      {
        json.writeNumberField("totalInit", vm.totalInit());
        json.writeNumberField("totalUsed", vm.totalUsed());
        json.writeNumberField("totalMax", vm.totalMax());
        json.writeNumberField("totalCommitted", vm.totalCommitted());

        json.writeNumberField("heapInit", vm.heapInit());
        json.writeNumberField("heapUsed", vm.heapUsed());
        json.writeNumberField("heapMax", vm.heapMax());
        json.writeNumberField("heapCommitted", vm.heapCommitted());

        json.writeNumberField("heap_usage", vm.heapUsage());
        json.writeNumberField("non_heap_usage", vm.nonHeapUsage());
        json.writeFieldName("memory_pool_usages");
        json.writeStartObject();
        {
          for (Map.Entry<String, Double> pool : vm.memoryPoolUsage().entrySet()) {
            json.writeNumberField(pool.getKey(), pool.getValue());
          }
        }
        json.writeEndObject();
      }
      json.writeEndObject();

      final Map<String, VirtualMachineMetrics.BufferPoolStats> bufferPoolStats = vm.getBufferPoolStats();
      if (!bufferPoolStats.isEmpty()) {
        json.writeFieldName("buffers");
        json.writeStartObject();
        {
          json.writeFieldName("direct");
          json.writeStartObject();
          {
            json.writeNumberField("count", bufferPoolStats.get("direct").getCount());
            json.writeNumberField("memoryUsed", bufferPoolStats.get("direct").getMemoryUsed());
            json.writeNumberField("totalCapacity", bufferPoolStats.get("direct").getTotalCapacity());
          }
          json.writeEndObject();

          json.writeFieldName("mapped");
          json.writeStartObject();
          {
            json.writeNumberField("count", bufferPoolStats.get("mapped").getCount());
            json.writeNumberField("memoryUsed", bufferPoolStats.get("mapped").getMemoryUsed());
            json.writeNumberField("totalCapacity", bufferPoolStats.get("mapped").getTotalCapacity());
          }
          json.writeEndObject();
        }
        json.writeEndObject();
      }


      json.writeNumberField("daemon_thread_count", vm.daemonThreadCount());
      json.writeNumberField("thread_count", vm.threadCount());
      json.writeNumberField("current_time", clock.time());
      json.writeNumberField("uptime", vm.uptime());
      json.writeNumberField("fd_usage", vm.fileDescriptorUsage());

      json.writeFieldName("thread-states");
      json.writeStartObject();
      {
        for (Map.Entry<Thread.State, Double> entry : vm.threadStatePercentages()
            .entrySet()) {
          json.writeNumberField(entry.getKey().toString().toLowerCase(),
              entry.getValue());
        }
      }
      json.writeEndObject();

      json.writeFieldName("garbage-collectors");
      json.writeStartObject();
      {
        for (Map.Entry<String, VirtualMachineMetrics.GarbageCollectorStats> entry : vm.garbageCollectors()
            .entrySet()) {
          json.writeFieldName(entry.getKey());
          json.writeStartObject();
          {
            final VirtualMachineMetrics.GarbageCollectorStats gc = entry.getValue();
            json.writeNumberField("runs", gc.getRuns());
            json.writeNumberField("time", gc.getTime(TimeUnit.MILLISECONDS));
          }
          json.writeEndObject();
        }
      }
      json.writeEndObject();
    }
    json.writeEndObject();
  }

  public static void writeRegularMetrics(Processor processor, JsonGenerator json, MetricsRegistry registry,
                                         boolean showFullSamples) throws IOException {
    for (Map.Entry<String, SortedMap<MetricName, Metric>> entry : registry.groupedMetrics().entrySet()) {
      for (Map.Entry<MetricName, Metric> subEntry : entry.getValue().entrySet()) {
        if (subEntry.getKey() instanceof TaggedMetricName) {
          // write the hashcode since we need to support metrics with the same name but with different tags.
          // the server will remove the suffix.
          json.writeFieldName(sanitize(subEntry.getKey()) + "$" + subEntry.hashCode());
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
          for (Map.Entry<String, String> tagEntry : ((TaggedMetricName) subEntry.getKey()).getTags().entrySet()) {
            json.writeStringField(tagEntry.getKey(), tagEntry.getValue());
          }
          json.writeEndObject();
          json.writeFieldName("value");
        } else {
          json.writeFieldName(sanitize(subEntry.getKey()));
        }
        try {
          subEntry.getValue().processWith(processor, subEntry.getKey(), new Context(json, showFullSamples));
        } catch (Exception e) {
          e.printStackTrace();
        }
        // need to close the object as well.
        if (subEntry.getKey() instanceof TaggedMetricName) {
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

    @Override
    public void processHistogram(MetricName name, Histogram histogram, Context context) throws Exception {
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
    json.writeNumberField("min", metric.min());
    json.writeNumberField("max", metric.max());
    json.writeNumberField("mean", metric.mean());
  }

  private static void writeSampling(Sampling metric, JsonGenerator json) throws IOException {
    final Snapshot snapshot = metric.getSnapshot();
    json.writeNumberField("median", snapshot.getMedian());
    json.writeNumberField("p75", snapshot.get75thPercentile());
    json.writeNumberField("p95", snapshot.get95thPercentile());
    json.writeNumberField("p99", snapshot.get99thPercentile());
    json.writeNumberField("p999", snapshot.get999thPercentile());
  }

  private static void writeMeteredFields(Metered metered, JsonGenerator json) throws IOException {
    json.writeNumberField("count", metered.count());
    json.writeNumberField("mean", metered.meanRate());
    json.writeNumberField("m1", metered.oneMinuteRate());
  }

  private static final Pattern SIMPLE_NAMES = Pattern.compile("[^a-zA-Z0-9_.\\-~]");

  private static String sanitize(MetricName metricName) {
    return SIMPLE_NAMES.matcher(metricName.getGroup() + "." + metricName.getName()).replaceAll("_");
  }
}
