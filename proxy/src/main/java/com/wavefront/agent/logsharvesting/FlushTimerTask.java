package com.wavefront.agent.logsharvesting;

import com.beust.jcommander.internal.Lists;
import com.fasterxml.jackson.databind.JsonNode;
import com.wavefront.agent.PointHandler;
import com.wavefront.metrics.JsonMetricsGenerator;
import com.wavefront.metrics.JsonMetricsParser;
import com.yammer.metrics.core.MetricsRegistry;

import java.io.IOException;
import java.util.List;
import java.util.TimerTask;
import java.util.logging.Logger;

import sunnylabs.report.ReportPoint;

/**
 * @author Mori Bellamy (mori@wavefront.com)
 */
public class FlushTimerTask extends TimerTask {
  private static final Logger logger = Logger.getLogger(FlushTimerTask.class.getCanonicalName());
  private final MetricsRegistry metricsRegistry;
  private final PointHandler pointHandler;

  public FlushTimerTask(MetricsRegistry metricsRegistry, PointHandler pointHandler) {
    this.metricsRegistry = metricsRegistry;
    this.pointHandler = pointHandler;
  }


  @Override
  public void run() {
    try {
      JsonNode jsonNode = JsonMetricsGenerator.generateJsonMetrics(metricsRegistry, false, false, false);
      List<ReportPoint> reportPoints = Lists.newLinkedList();
      //JsonMetricsParser.report();
    } catch (IOException e) {
      logger.severe("Could not process points: " + e.getMessage());
      e.printStackTrace();
    }
  }
}
