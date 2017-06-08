package com.wavefront.agent;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.wavefront.agent.preprocessor.PointPreprocessor;

import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.server.handler.AbstractHandler;

import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.annotation.Nullable;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import sunnylabs.report.ReportPoint;

/**
 * Agent-side JSON metrics endpoint for parsing JSON from write_http collectd plugin.
 *
 * @see <a href="https://collectd.org/wiki/index.php/Plugin:Write_HTTP">https://collectd.org/wiki/index.php/Plugin:Write_HTTP</a>
 */
public class WriteHttpJsonMetricsEndpoint extends AbstractHandler {

  protected static final Logger logger = Logger.getLogger("agent");
  private static final Logger blockedPointsLogger = Logger.getLogger("RawBlockedPoints");

  @Nullable
  private final String prefix;
  private final String defaultHost;
  @Nullable
  private final PointPreprocessor preprocessor;
  private final PointHandler handler;

  public WriteHttpJsonMetricsEndpoint(final String port, final String host,
                                      @Nullable
                                      final String prefix, final String validationLevel,
                                      final int blockedPointsPerBatch, PostPushDataTimedTask[] postPushDataTimedTasks,
                                      @Nullable final PointPreprocessor preprocessor) {
    this.handler = new PointHandlerImpl(port, validationLevel, blockedPointsPerBatch, postPushDataTimedTasks);
    this.prefix = prefix;
    this.defaultHost = host;
    this.preprocessor = preprocessor;
  }

  @Override
  public void handle(String target, Request baseRequest, HttpServletRequest request, HttpServletResponse response)
      throws IOException, ServletException {
    response.setContentType("text/html;charset=utf-8");

    JsonNode metrics = new ObjectMapper().readTree(request.getReader());

    if (!metrics.isArray()) {
      logger.warning("metrics is not an array!");
      handler.handleBlockedPoint("[metrics] is not an array!");
      response.setStatus(HttpServletResponse.SC_BAD_REQUEST); // return HTTP 400
      baseRequest.setHandled(true);
      return;
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
          handler.handleBlockedPoint("[values] missing in JSON object");
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
          if (preprocessor != null) {
            preprocessor.forReportPoint().transform(point);
            if (!preprocessor.forReportPoint().filter(point)) {
              if (preprocessor.forReportPoint().getLastFilterResult() != null) {
                blockedPointsLogger.warning(PointHandlerImpl.pointToString(point));
              } else {
                blockedPointsLogger.info(PointHandlerImpl.pointToString(point));
              }
              handler.handleBlockedPoint(preprocessor.forReportPoint().getLastFilterResult());
              continue;
            }
          }
          handler.reportPoint(point, "write_http json: " + PointHandlerImpl.pointToString(point));
          index++;
        }
      } catch (final Exception e) {
        handler.handleBlockedPoint("Failed adding metric: " + e);
        logger.log(Level.WARNING, "Failed adding metric", e);
        response.setStatus(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
        baseRequest.setHandled(true);
        return;
      }
    }
    response.setStatus(HttpServletResponse.SC_OK);
    baseRequest.setHandled(true);
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
