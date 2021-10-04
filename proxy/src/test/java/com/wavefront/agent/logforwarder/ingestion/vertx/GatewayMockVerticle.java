package com.wavefront.agent.logforwarder.ingestion.vertx;

import com.wavefront.agent.logforwarder.ingestion.client.gateway.verticle.HttpServerVerticle;
import org.apache.http.HttpStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.lang.invoke.MethodHandles;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.DeploymentOptions;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.core.VertxOptions;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;
import io.vertx.ext.web.handler.BodyHandler;

/**
 * A mock implementation of {@link AbstractVerticle} for simulating test cases with a vertx server.
 * @author Manoj Ramakrishnan (rmanoj@vmware.com).
 * @since 10/4/21 10:12 AM
 */
public class GatewayMockVerticle extends AbstractVerticle {

  private static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  private int port;
  private Router router;
  private Consumer<RoutingContext> consumer;

  public GatewayMockVerticle(int port, Consumer<RoutingContext> consumer) {
    this.port = port;
    this.consumer = consumer;
  }

  @Override
  public void start(Promise<Void> startPromise) {
    router = Router.router(vertx);
    this.router.route()
        .handler(BodyHandler.create(false))
        .handler(this::handleIngress);

    Promise<String> httpServerDeployment = Promise.promise();
    vertx.deployVerticle(() -> new HttpServerVerticle(router, port, "/log-forwarder/health"),
        new DeploymentOptions().setInstances(1), httpServerDeployment);

    httpServerDeployment.future().setHandler(ar -> {
      if (ar.succeeded()) {
        logger.info("Successfully started Gateway mock verticle {}", deploymentID());
        startPromise.complete();
      } else {
        logger.error("Failed starting Gateway mock verticle", ar.cause());
        startPromise.fail(ar.cause());
      }
    });
  }

  private void handleIngress(RoutingContext routingContext) {
    consumer.accept(routingContext);
  }

  public static Vertx deploy(int port) throws Exception {
    return deploy(port, ACCEPT_ALL_LEMANS_CONSUMER);
  }

  public static Vertx deploy(int port, Consumer<RoutingContext> consumer) throws Exception {
    Vertx vertx = Vertx.vertx(new VertxOptions());
    Promise<String> promise = Promise.promise();
    vertx.deployVerticle(() -> new GatewayMockVerticle(port, consumer), new DeploymentOptions().setInstances(1),
        promise);
    CountDownLatch latch = new CountDownLatch(1);
    promise.future().setHandler(ar -> latch.countDown());
    latch.await(5, TimeUnit.SECONDS);
    return vertx;
  }

  public static Consumer<RoutingContext> ACCEPT_ALL_LEMANS_CONSUMER = routingContext -> {
    String path = routingContext.normalisedPath();
    if (path.startsWith("/le-mans/v1/streams/ingestion-pipeline-stream") ||
        path.startsWith("/le-mans/v1/streams/non-billable-ingestion-pipeline-stream") ||
        path.startsWith("/le-mans/v1/streams/events-pipeline-stream")) {
      // Accept all the data.
      routingContext.response().setStatusCode(HttpStatus.SC_OK).end();
    } else {
      routingContext.response().setStatusCode(HttpStatus.SC_INTERNAL_SERVER_ERROR).end();
    }
  };

  public static Consumer<RoutingContext> UNAUTHORIZE_ALL_LEMANS_CONSUMER = routingContext -> {
    String path = routingContext.normalisedPath();
    if (path.startsWith("/le-mans/v1/streams/ingestion-pipeline-stream") ||
        path.startsWith("/le-mans/v1/streams/non-billable-ingestion-pipeline-stream") ||
        path.startsWith("/le-mans/v1/streams/events-pipeline-stream")) {
      // Accept all the data.
      routingContext.response().setStatusCode(HttpStatus.SC_UNAUTHORIZED).end("{}");
    } else {
      routingContext.response().setStatusCode(HttpStatus.SC_INTERNAL_SERVER_ERROR).end();
    }
  };

}