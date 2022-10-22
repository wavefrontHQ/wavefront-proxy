package com.wavefront.grpc.example;

import com.wavefront.grpc.Batch;
import com.wavefront.grpc.Point;
import com.wavefront.grpc.Response;
import com.wavefront.grpc.WFProxyGrpc;
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
    String metricName = "test.metric.grpc";
    String wfProxy = "localhost:50051";
    if (args.length > 0) {
      if ("--help".equals(args[0])) {
        System.err.println("Usage: [name [wfProxy]]");
        System.err.println();
        System.err.println("  name    The name you wish to be greeted by. Defaults to " + metricName);
        System.err.println("  wfProxy  The server to connect to. Defaults to " + wfProxy);
        System.exit(1);
      }
      metricName = args[0];
    }
    if (args.length > 1) {
      wfProxy = args[1];
    }

    ManagedChannel channel = ManagedChannelBuilder.forTarget(wfProxy).usePlaintext().build();
    try {
      SimpleTest client = new SimpleTest(channel);
      client.sendMetric(metricName);
    } finally {
      channel.shutdownNow().awaitTermination(5, TimeUnit.SECONDS);
    }
  }

  public void sendMetric(String name) {
    logger.info("Will try to send '" + name + "' metrics to WF ...");
    Point point = Point.newBuilder()
            .setName(name)
            .setTs(System.currentTimeMillis())
            .setSource("sampleApp")
            .setValue((int) (Math.random() * 1000))
            .putTags("tag1", "tagv1")
            .putTags("tag2", "tagv2")
            .putTags("tag3", "tagv3")
            .build();

    Point point2 = Point.newBuilder()
            .setName(name).setValue((int) (Math.random() * 1000))
            .build();

    Response response;
    Batch request = Batch.newBuilder().addPoints(point).addPoints(point2).build();
    try {
      response = blockingStub.reportPoints(request);
    } catch (StatusRuntimeException e) {
      logger.log(Level.WARNING, "RPC failed: {0}", e.getStatus());
      return;
    }
    logger.info("Greeting: " + response.getRes());
  }
}
