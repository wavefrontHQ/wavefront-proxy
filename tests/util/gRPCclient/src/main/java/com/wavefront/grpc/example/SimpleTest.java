package com.wavefront.grpc.example;

import com.wavefront.grpc.*;
import io.grpc.Channel;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.StatusRuntimeException;

import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

public class SimpleTest {
  private static final Logger logger = Logger.getLogger(SimpleTest.class.getName());

  private final WFProxyGrpc.WFProxyBlockingStub blockingStub;

  public SimpleTest(Channel channel) {
    blockingStub = WFProxyGrpc.newBlockingStub(channel);
  }

  public static void main(String[] args) throws Exception {
    String wfProxy = "localhost:50051";
    if (args.length > 0) {
      if ("--help".equals(args[0])) {
        System.err.println("Usage: [wfProxy]");
        System.err.println();
        System.err.println("  wfProxy  The server to connect to. Defaults to " + wfProxy);
        System.exit(1);
      }
    }
    if (args.length > 1) {
      wfProxy = args[1];
    }

    ManagedChannel channel = ManagedChannelBuilder.forTarget(wfProxy).usePlaintext().build();
    try {
      SimpleTest client = new SimpleTest(channel);
      client.sendMetrics();
    } finally {
      channel.shutdownNow().awaitTermination(5, TimeUnit.SECONDS);
    }
  }

  public void sendMetrics() {
    Point point = Point.newBuilder()
            .setName("test.metric.a")
            .setTimestamp(System.currentTimeMillis())
            .setSource("sampleApp")
            .setValue(Math.random() * 1000d)
            .putTags("tag1", "tagv1")
            .putTags("tag2", "tagv2")
            .putTags("tag3", "tagv3")
            .build();

    Point point2 = Point.newBuilder()
            .setName("test.metric.b")
            .setValue(Math.random() * 1000d)
            .build();

    Histogram.Builder histogramBuilder = Histogram.newBuilder()
            .setName("test.histogram.a");
    for (int i = 0; i < 10; i++) {
      histogramBuilder.addBinsBuilder().setCount(i).setMean(Math.random() * 1000d).build();
    }

    Batch request = Batch.newBuilder()
            .addPoints(point)
            .addPoints(point2)
            .addHistograms(histogramBuilder.build())
            .build();
    Response response;
    try {
      response = blockingStub.reportPoints(request);
    } catch (StatusRuntimeException e) {
      logger.log(Level.WARNING, "RPC failed: {0}", e.getStatus());
      return;
    }
    logger.info("result: " + response.getRes());


    Batch.Builder requestWithDefaultsBuilder = Batch.newBuilder()
            .setDefaultSource("default_source")
            .setDefaultTimestamp(System.currentTimeMillis())
            .putCommonTags("app", this.getClass().getSimpleName());
    requestWithDefaultsBuilder.addPointsBuilder()
            .setName("test.metric.a")
            .setValue(Math.random() * 1000d)
            .build();
    requestWithDefaultsBuilder.addPointsBuilder()
            .setName("test.metric.b")
            .setValue(Math.random() * 1000d)
            .setType(POINT_TYPE.DELTA)
            .putTags("service", "main")
            .build();

    try {
      response = blockingStub.reportPoints(requestWithDefaultsBuilder.build());
    } catch (StatusRuntimeException e) {
      logger.log(Level.WARNING, "RPC failed: {0}", e.getStatus());
      return;
    }
    logger.info("result: " + response.getRes());
  }
}
