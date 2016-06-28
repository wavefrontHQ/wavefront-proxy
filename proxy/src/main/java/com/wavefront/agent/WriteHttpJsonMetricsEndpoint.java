package com.wavefront.agent;

import com.fasterxml.jackson.databind.JsonNode;

import java.util.logging.Level;
import java.util.logging.Logger;

import javax.annotation.Nullable;
import javax.ws.rs.Consumes;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriInfo;

import sunnylabs.report.ReportPoint;

/**
 * Agent-side JSON metrics endpoint for parsing JSON from write_http collectd plugin.
 *
 * @see <a href="https://collectd.org/wiki/index.php/Plugin:Write_HTTP">https://collectd.org/wiki/index.php/Plugin:Write_HTTP</a>
 */
@Path("/")
public class WriteHttpJsonMetricsEndpoint extends PointHandlerImpl {

  protected static final Logger logger = Logger.getLogger("agent");

  @Nullable
  private final String prefix;
  private final String defaultHost;

  public WriteHttpJsonMetricsEndpoint(final int port, final String host,
                                      @Nullable
                                      final String prefix, final String validationLevel,
                                      final int blockedPointsPerBatch, PostPushDataTimedTask[] postPushDataTimedTasks) {
    super(port, validationLevel, blockedPointsPerBatch, postPushDataTimedTasks);
    this.prefix = prefix;
    this.defaultHost = host;
  }

  @POST
  @Consumes(MediaType.APPLICATION_JSON)
  public Response reportMetrics(@Context UriInfo uriInfo,
                                JsonNode metrics) {
    final PostPushDataTimedTask randomPostTask = this.getRandomPostTask();
    if (!metrics.isArray()) {
      logger.warning("metrics is not an array!");
      randomPostTask.incrementBlockedPoints();
      throw new IllegalArgumentException("Metrics must be an array");
    }

    for (final JsonNode metric : metrics) {
      try {
        JsonNode host = metric.get("host");
        String hostName;
        if (host != null) {
          hostName = host.textValue();
          if (hostName == null || hostName.isEmpty()) {
            hostName = defaultHost;
          }
        } else {
          hostName = defaultHost;
        }

        JsonNode time = metric.get("time");
        long ts = 0;
        if (time != null) {
          ts = time.asLong() * 1000;
        }
        JsonNode values = metric.get("values");
        if (values == null) {
          randomPostTask.incrementBlockedPoints();
          logger.warning("Skipping.  Missing values.");
          continue;
        }
        int index = 0;
        for (final JsonNode value : values) {
          String metricName = getMetricName(metric, index);
          ReportPoint.Builder builder = ReportPoint.newBuilder()
              .setMetric(metricName)
              .setTable("dummy")
              .setTimestamp(ts)
              .setHost(hostName);
          if (value.isDouble()) {
            builder.setValue(value.asDouble());
          } else {
            builder.setValue(value.asLong());
          }
          ReportPoint point = builder.build();
          reportPoint(point, "write_http json: " + pointToString(point));
          index++;
        }
      } catch (final Exception e) {
        randomPostTask.incrementBlockedPoints();
        logger.log(Level.WARNING, "Failed adding metric", e);
      }
    }
    return Response.accepted().build();
  }

  /**
   * Generates a metric name from json format:
     {
     "values": [197141504, 175136768],
     "dstypes": ["counter", "counter"],
     "dsnames": ["read", "write"],
     "time": 1251533299,
     "interval": 10,
     "host": "leeloo.lan.home.verplant.org",
     "plugin": "disk",
     "plugin_instance": "sda",
     "type": "disk_octets",
     "type_instance": ""
    }

    host "/" plugin ["-" plugin instance] "/" type ["-" type instance] =>
    {plugin}[.{plugin_instance}].{type}[.{type_instance}]
  */
  private String getMetricName(final JsonNode metric, int index) {
    JsonNode plugin = metric.get("plugin");
    JsonNode plugin_instance = metric.get("plugin_instance");
    JsonNode type = metric.get("type");
    JsonNode type_instance = metric.get("type_instance");

    if (plugin == null || type == null) {
      throw new IllegalArgumentException("plugin or type is missing");
    }

    StringBuilder sb = new StringBuilder();
    sb.append(plugin.textValue());
    sb.append('.');
    if (plugin_instance != null) {
      String value = plugin_instance.textValue();
      if (value != null && !value.isEmpty()) {
        sb.append(value);
        sb.append('.');
      }
    }
    sb.append(type.textValue());
    sb.append('.');
    if (type_instance != null) {
      String value = type_instance.textValue();
      if (value != null && !value.isEmpty()) {
        sb.append(value);
        sb.append('.');
      }
    }

    JsonNode dsnames = metric.get("dsnames");
    if (dsnames == null || !dsnames.isArray() || dsnames.size() <= index) {
      throw new IllegalArgumentException("dsnames is not set");
    }
    sb.append(dsnames.get(index).textValue());
    return sb.toString();
  }
}
