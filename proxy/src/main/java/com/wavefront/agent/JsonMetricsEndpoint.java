package com.wavefront.agent;

import com.google.common.collect.Maps;

import com.fasterxml.jackson.databind.JsonNode;
import com.wavefront.agent.preprocessor.PointPreprocessor;
import com.wavefront.common.Clock;
import com.wavefront.metrics.JsonMetricsParser;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import javax.annotation.Nullable;
import javax.ws.rs.Consumes;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriInfo;

import sunnylabs.report.ReportPoint;

/**
 * Agent-side JSON metrics endpoint.
 *
 * @author Clement Pang (clement@wavefront.com).
 */
@Path("/")
public class JsonMetricsEndpoint extends PointHandlerImpl {

  @Nullable
  private final String prefix;
  private final String defaultHost;
  @Nullable
  private final PointPreprocessor preprocessor;

  public JsonMetricsEndpoint(final int port, final String host,
                             @Nullable
                             final String prefix, final String validationLevel, final int blockedPointsPerBatch,
                             PostPushDataTimedTask[] postPushDataTimedTasks,
                             @Nullable final PointPreprocessor preprocessor) {
    super(port, validationLevel, blockedPointsPerBatch, postPushDataTimedTasks);
    this.prefix = prefix;
    this.defaultHost = host;
    this.preprocessor = preprocessor;
  }

  @POST
  @Consumes(MediaType.APPLICATION_JSON)
  public Response reportMetrics(@QueryParam("h") String host,
                                @QueryParam("p") String prefix,
                                @QueryParam("d") Long timestamp,
                                @Context UriInfo uriInfo,
                                JsonNode metrics) {
    MultivaluedMap<String, String> queryParams = uriInfo.getQueryParameters(true);
    Map<String, String> tags = Maps.newHashMap();
    for (Map.Entry<String, List<String>> entry : queryParams.entrySet()) {
      String tagk = entry.getKey().trim().toLowerCase();
      if (tagk.equals("h") || tagk.equals("p") || tagk.equals("d") || tagk.equals("t")) {
        continue;
      }
      tags.put(tagk, entry.getValue().get(0));
    }
    List<ReportPoint> points = new ArrayList<>();
    long when = timestamp == null ? Clock.now() : timestamp;
    if (this.prefix != null) {
      if (prefix == null) prefix = this.prefix;
      else prefix = this.prefix + "." + prefix;
    }
    if (host == null) host = defaultHost;
    JsonMetricsParser.report("dummy", prefix, metrics, points, host, when);
    for (ReportPoint point : points) {
      if (point.getAnnotations() == null) {
        point.setAnnotations(tags);
      } else {
        Map<String, String> newAnnotations = Maps.newHashMap(tags);
        newAnnotations.putAll(point.getAnnotations());
        point.setAnnotations(newAnnotations);
      }
      if (preprocessor != null) {
        if (!preprocessor.forReportPoint().filter(point)) {
          handleBlockedPoint(preprocessor.forReportPoint().getLastFilterResult());
          continue;
        }
        preprocessor.forReportPoint().transform(point);
      }
      reportPoint(point, "json: " + pointToString(point));
    }
    return Response.accepted().build();
  }
}
