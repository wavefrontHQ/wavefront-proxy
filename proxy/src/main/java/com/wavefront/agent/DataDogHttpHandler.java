package com.wavefront.agent;

import com.fasterxml.jackson.databind.JsonNode;

import org.apache.commons.lang.StringUtils;

import java.util.logging.Logger;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import javax.annotation.Nullable;
import javax.ws.rs.Consumes;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriInfo;

import com.wavefront.common.MetricWhiteBlackList;
import sunnylabs.report.ReportPoint;

/**
 * This class handles DataDog requests that come from the agent and are POST'd to their endpoint.
 * This handler will parse requests from the following URLs:
 *     /intake
 *     /api/v1/series
 *     / (redirected to /api/v1/series handler)
 */
@Path("/")
public class DataDogHttpHandler extends PointHandler {
  private static final Logger LOG = Logger.getLogger(
      DataDogHttpHandler.class.getCanonicalName());

  private final String defaultHost;
  private MetricWhiteBlackList whiteBlackList;

  DataDogHttpHandler(final int port, final String host,
                            @Nullable
                            final String prefix, final String validationLevel,
                            final int blockedPointsPerBatch, PostPushDataTimedTask[] postPushDataTimedTasks,
                     @Nullable final String pointLineWhiteListRegex,
                     @Nullable final String pointLineBlackListRegex) {
    super(port, validationLevel, blockedPointsPerBatch, prefix, postPushDataTimedTasks);
    this.defaultHost = host;
    this.whiteBlackList = new MetricWhiteBlackList(pointLineWhiteListRegex,
        pointLineBlackListRegex,
        String.valueOf(port));
  }

  /**
   * Handles the HTTP request from the datadog agent with the URI: /intake/?api_key=<datadog api
   * key> ASSUMPTION: the content body is zip'd using ZLib and the value is a JSON object. Given a
   * json message, this will parse and find the metrics and post those to the WF server. The JSON is
   * expected to look like this: { "metrics": [ [ "system.disk.total", 1451409097, 497448.0, {
   * "device_name": "udev", "hostname": "mike-ubuntu14", "type": "gauge" } ], ... } Each metric in
   * the metrics array is consider a report point and is sent to the WF server using the
   * PointHandler object.  The metric array element is made up of: (0):  metric name (1):  timestamp
   * (epoch seconds) (2):  value (assuming float for all values) (3):  tags (including host); all
   * tags are converted to tags except hostname which is sent on its own as the source for the
   * point.
   *
   * In addition to the metric array elements, all top level elements that begin with : cpu* mem*
   * are captured and the value is sent.  These items are in the form of: { ...
   * "collection_timestamp": 1451409092.995346, "cpuGuest": 0.0, "cpuIdle": 99.33, "cpuStolen": 0.0,
   * ... "internalHostname": "mike-ubuntu14", ... } The names are retrieved from the JSON key name
   * splitting the key on upper case letters and adding a dot between to form a metric name like
   * this example: "cpuGuest" => "cpu.guest" The value comes from the JSON key's value.
   *
   * @param root root node of the parsed HttpContent body
   */
  @POST
  @Consumes(MediaType.APPLICATION_JSON)
  @Path("intake")
  public Response handleIntakeRequest(@Context UriInfo uriInfo, JsonNode root) {
    // get the hostname used by all of the top level metrics
    String hostName = root.findPath("internalHostname").asText();
    if (StringUtils.isBlank(hostName)) {
      hostName = this.defaultHost;
    }
    // get the collection timestamp for all the top level metrics
    final double ts = root.findPath("collection_timestamp").asDouble();

    // iterator over all the top level fields and pull out the name/value
    // pairs of all items we care about
    // {
    //     "cpuIdle": 0.00,
    //     "cpuUser": 0.00,
    //     ....
    // }
    Iterator<Map.Entry<String, JsonNode>> fields = root.fields();
    while (fields.hasNext()) {
      Map.Entry<String, JsonNode> field = fields.next();
      if (field.getKey().startsWith("cpu") ||
          field.getKey().startsWith("mem")) {
        final ReportPoint point = ReportPoint.newBuilder()
            .setMetric(convertKeyToDottedName(field.getKey()))
            .setTimestamp((long) ts * 1000) // convert to ms
            .setHost(hostName)
            .setValue(field.getValue().asDouble())
            .setTable("datadog")
            .build();
        // apply white/black lists after formatting
        if (!whiteBlackList.passes(point.getMetric())) {
          handleBlockedPoint(point.getMetric());
        } else {
          reportPoint(point, root.toString());
        }
      }
    }

    // metrics array items
    final JsonNode metrics = root.findPath("metrics");
    for (final JsonNode metric : metrics) {
      // pull out the tags and then search for the hostname
      // we won't send the hostname as a tag, we'll send that as "source"
      // to WF point handler
      final JsonNode tags = metric.get(3);
      final Map<String, String> annotations = new HashMap<>();
      JsonNode host = null;
      fields = tags.fields();
      while (fields.hasNext()) {
        Map.Entry<String, JsonNode> field = fields.next();
        if (field.getKey().equals("hostname")) {
          host = field.getValue();
        } else {
          annotations.put(field.getKey(), field.getValue().asText());
        }
      }

      // assuming we found a host, then send the details to WF
      if (host != null) {
        final ReportPoint point = ReportPoint.newBuilder()
            .setAnnotations(annotations)
            .setMetric(metric.get(0).asText())
            .setTimestamp(metric.get(1).asLong() * 1000) // convert to ms
            .setHost(host.asText())
            .setValue(metric.get(2).asDouble())
            .setTable("datadog")
            .build();
        // apply white/black lists after formatting
        if (!whiteBlackList.passes(point.getMetric())) {
          handleBlockedPoint(point.getMetric());
        } else {
          reportPoint(point, root.toString());
        }
      }
    }
    return Response.accepted().build();
  }

  /**
   * Handles the HTTP request from the datadog agent with the URI: /api/v1/series/?api_key=<datadog
   * api key> JSON is expected to look like : { "series": [ { "device_name" : null, "host": "<host
   * name>", "interval": 10.0, "metric": "<name>", "points": [ [ 1451950930.0, 0 ] ], "tags": null,
   * "type": "gauge" }, ... ] } The point element is made up of: (0):   timestamp (epoch seconds)
   * (1):   value (numeric)
   *
   * @param root root node of the parsed HttpContent JSON body
   */
  @POST
  @Consumes(MediaType.APPLICATION_JSON)
  @Path("api/v1/series")
  public Response handleApiSeries(@Context UriInfo uriInfo, JsonNode root) {
    // ignore everything else and get the "series" array
    final JsonNode metrics = root.findPath("series");
    for (final JsonNode metric : metrics) {
      String metricName = metric.findPath("metric").asText();

      // grab the tags
      final Map<String, String> annotations = new HashMap<>();
      final JsonNode tags = metric.findPath("tags");
      if (tags.isArray()) {
        // assumption: must be an array, values are strings; format:
        // name:value
        for (final JsonNode tag : tags) {
          final String namevalue = tag.asText();
          final String[] parts = namevalue.split(":");
          if (parts.length != 2) {
            LOG.warning(String.format("Expected tag to be in format <name>:<value> but got '%s'.  Ignoring this tag.", namevalue));
            continue;
          }
          annotations.put(parts[0], parts[1]);
        }
      }

      String host = metric.findPath("host").asText();
      if (StringUtils.isBlank(host)) {
        host = this.defaultHost;
      }
      final JsonNode points = metric.findPath("points");
      for (final JsonNode pt : points) {
        final ReportPoint point = ReportPoint.newBuilder()
            .setAnnotations(annotations)
            .setMetric(metricName)
            .setTimestamp(pt.get(0).asLong() * 1000) // convert to ms
            .setHost(host)
            .setValue(pt.get(1).asDouble())
            .setTable("datadog")
            .build();
        // apply white/black lists after formatting
        if (!whiteBlackList.passes(point.getMetric())) {
          handleBlockedPoint(point.getMetric());
        } else {
          reportPoint(point, root.toString());
        }
      }
    }
    return Response.accepted().build();
  }

  @POST
  @Consumes(MediaType.APPLICATION_JSON)
  public Response handleDefault(@Context UriInfo uriInfo, JsonNode root) {
    return handleApiSeries(uriInfo, root);
  }


  /**
   * Convert a key that is camel-case notation to a dotted equivalent. This is best described with
   * an example: key = "memPhysFree" returns "mem.phys.free"
   *
   * @param key a camel-case string value
   * @return dotted notation with each uppercase containing a dot before
   */
  private String convertKeyToDottedName(final String key) {
    final StringBuilder sb = new StringBuilder();
    for (int i = 0; i < key.length(); i++) {
      final char c = key.charAt(i);
      if (Character.isUpperCase(c)) {
        sb.append(".");
        sb.append(Character.toLowerCase(c));
      } else {
        sb.append(c);
      }
    }
    return sb.toString();
  }
}
