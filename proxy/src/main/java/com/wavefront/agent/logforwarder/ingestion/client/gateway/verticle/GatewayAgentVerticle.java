package com.wavefront.agent.logforwarder.ingestion.client.gateway.verticle;

import com.wavefront.agent.logforwarder.ingestion.restapi.BaseHttpEndpoint;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.invoke.MethodHandles;
import java.util.Arrays;
import java.util.Map;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.CompositeFuture;
import io.vertx.core.DeploymentOptions;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.core.VertxOptions;
import io.vertx.ext.web.Router;

/**
 * A lifecycle manager of all {@link BaseHttpEndpoint}  needed for the log gateway ingestion on
 * the proxy.
 * @author Manoj Ramakrishnan (rmanoj@vmware.com).
 * @since 9/20/21 10:39 AM
 */
public class GatewayAgentVerticle extends AbstractVerticle {
  private static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private Map<String, BaseHttpEndpoint> BaseHttpEndpointMap;

  private int port;

  public GatewayAgentVerticle(Map<String, BaseHttpEndpoint> BaseHttpEndpointMap, int port) {
    this.BaseHttpEndpointMap = BaseHttpEndpointMap;
    this.port = port;
  }

  @Override
  public void start(Promise<Void> startPromise) {

    Router router = Router.router(vertx);
    Promise<String> ingressVerticleDeployment = Promise.promise();

    // Pass on the configs
    DeploymentOptions deploymentOptions = new DeploymentOptions();
    deploymentOptions.setConfig(config());
    deploymentOptions.setInstances(context.getInstanceCount());

    vertx.deployVerticle(() -> new IngressVerticle(router, this.BaseHttpEndpointMap), deploymentOptions,
        ingressVerticleDeployment);

    CompositeFuture.join(Arrays.asList(ingressVerticleDeployment.future())).compose(ar -> {
      if (ar.failed()) {
        return Future.failedFuture(ar.cause());
      } else {
        logger.info("All required gateway agent verticles started, starting " +
            "GatewayAgentHttpServerVerticle");
        Promise<String> httpServerDeployment = Promise.promise();
        vertx.deployVerticle(() -> new HttpServerVerticle(router, port, "/lemans-agent/health"),
            deploymentOptions, httpServerDeployment);
        return httpServerDeployment.future();
      }
    }).setHandler(ar -> {
      if (ar.succeeded()) {
        logger.info("Successfully started Lemans Agent verticle " + deploymentID());
        startPromise.complete();
      } else {
        logger.error("Failed starting some Lemans agent verticles", ar.cause());
        startPromise.fail(ar.cause());
      }
    });
  }

  public static Vertx deploy(Map<String, BaseHttpEndpoint> BaseHttpEndpointMap, int instanceCount, int port) {
    Vertx vertx = Vertx.vertx(new VertxOptions());
    vertx.deployVerticle(() -> new GatewayAgentVerticle(BaseHttpEndpointMap, port), new DeploymentOptions()
        .setInstances(instanceCount));
    return vertx;
  }

}
