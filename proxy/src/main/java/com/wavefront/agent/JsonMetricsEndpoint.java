package com.wavefront.agent;

import com.google.common.collect.Maps;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.wavefront.agent.preprocessor.PointPreprocessor;
import com.wavefront.common.Clock;
import com.wavefront.metrics.JsonMetricsParser;

import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.server.handler.AbstractHandler;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;

import javax.annotation.Nullable;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import wavefront.report.ReportPoint;

/**
 * Agent-side JSON metrics endpoint.
 *
 * @author Clement Pang (clement@wavefront.com).
 */
public class JsonMetricsEndpoint extends AbstractHandler {

  private static final Logger blockedPointsLogger = Logger.getLogger("RawBlockedPoints");

  @Nullable
  private final String prefix;
  private final String defaultHost;
  @Nullable
  private final PointPreprocessor preprocessor;
  private final PointHandler handler;

  public JsonMetricsEndpoint(final String port, final String host,
                             @Nullable
                             final String prefix, final String validationLevel, final int blockedPointsPerBatch,
                             PostPushDataTimedTask[] postPushDataTimedTasks,
                             @Nullable final PointPreprocessor preprocessor) {
    this.handler = new PointHandlerImpl(port, validationLevel, blockedPointsPerBatch, postPushDataTimedTasks);
    this.prefix = prefix;
    this.defaultHost = host;
    this.preprocessor = preprocessor;
  }

  @Override
  public void handle(String target, Request baseRequest, HttpServletRequest request, HttpServletResponse response)
      throws IOException, ServletException {
    Map<String, String> tags = Maps.newHashMap();
    for (Enumeration<String> parameters = request.getParameterNames(); parameters.hasMoreElements();) {
      String tagk = parameters.nextElement().trim().toLowerCase();
      if (tagk.equals("h") || tagk.equals("p") || tagk.equals("d") || tagk.equals("t")) {
        continue;
      }
      if (request.getParameter(tagk) != null && request.getParameter(tagk).length() > 0) {
        tags.put(tagk, request.getParameter(tagk));
      }
    }
    List<ReportPoint> points = new ArrayList<>();
    Long timestamp;
    if (request.getParameter("d") == null) {
      timestamp = Clock.now();
    } else {
      try {
        timestamp = Long.parseLong(request.getParameter("d"));
      } catch (NumberFormatException e) {
        timestamp = Clock.now();
      }
    }
    String prefix;
    if (this.prefix != null) {
      prefix = request.getParameter("p") == null ? this.prefix : this.prefix + "." + request.getParameter("p");
    } else {
      prefix = request.getParameter("p");
    }
    String host = request.getParameter("h") == null ? defaultHost : request.getParameter("h");

    JsonNode metrics = new ObjectMapper().readTree(request.getReader());

    JsonMetricsParser.report("dummy", prefix, metrics, points, host, timestamp);
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
          if (preprocessor.forReportPoint().getLastFilterResult() != null) {
            blockedPointsLogger.warning(PointHandlerImpl.pointToString(point));
          } else {
            blockedPointsLogger.info(PointHandlerImpl.pointToString(point));
          }
          handler.handleBlockedPoint(preprocessor.forReportPoint().getLastFilterResult());
          continue;
        }
        preprocessor.forReportPoint().transform(point);
      }
      handler.reportPoint(point, "json: " + PointHandlerImpl.pointToString(point));
    }
    response.setContentType("text/html;charset=utf-8");
    response.setStatus(HttpServletResponse.SC_OK);
    baseRequest.setHandled(true);
  }
}
