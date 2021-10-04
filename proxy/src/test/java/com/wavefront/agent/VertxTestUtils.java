package com.wavefront.agent;


import com.wavefront.agent.logforwarder.ingestion.client.gateway.GatewayClient;
import com.wavefront.agent.logforwarder.ingestion.client.gateway.GatewayClientManager;
import com.wavefront.agent.logforwarder.ingestion.client.gateway.model.GatewayResponse;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.ServerSocket;
import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.zip.GZIPOutputStream;

import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * @author Manoj Ramakrishnan (rmanoj@vmware.com).
 * @since 9/14/21 4:57 PM
 */
public class VertxTestUtils {
  public static int getUnusedRandomPort() throws Exception {
    ServerSocket socket = new ServerSocket(0);
    socket.close();
    return socket.getLocalPort();
  }

  public static GatewayClientManager createMockLemansClient(CompletableFuture<GatewayResponse> completableFuture) {
    GatewayClientManager gatewayClientManager = mock(GatewayClientManager.class);
    GatewayClient gatewayClient = mock(GatewayClient.class);
    when(gatewayClientManager.getGatewayClient()).thenReturn(gatewayClient);
    when(gatewayClient.sendRequest(any())).thenReturn(completableFuture);
    return gatewayClientManager;
  }

  public static void waitForCondition(int sleepMillis, int retryCount, Callable<Boolean> c) throws Exception {
    while (!c.call() && retryCount-- > 0) {
      Thread.sleep(sleepMillis);
    }
  }

  public static byte[] compressToGzip(String text) {
    byte[] textAsBytes = text.getBytes(StandardCharsets.UTF_8);
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    try (GZIPOutputStream os = new GZIPOutputStream(baos)) {
      os.write(textAsBytes);
    } catch (IOException ioe) {
      return null;
    }
    return baos.toByteArray();
  }

  public static void undeployVerticle(Vertx vertx) throws Exception {
    CountDownLatch latch = new CountDownLatch(vertx.deploymentIDs().size());
    System.out.println(String.format("Started Undeploying %d deplyments", vertx.deploymentIDs().size()));
    Handler<AsyncResult<Void>> handler = event -> {
      latch.countDown();
      System.out.println("Undeployed verticle");
    };
    vertx.deploymentIDs().forEach(id -> vertx.undeploy(id, handler));
    latch.await(5, TimeUnit.SECONDS);
    System.out.println(String.format("Finished Undeploying %d deplyments", vertx.deploymentIDs().size()));
  }

  public static void undeployVerticles(Collection<Vertx> verticles) throws Exception {
    for (Vertx verticle : verticles) {
      undeployVerticle(verticle);
    }
  }
}
