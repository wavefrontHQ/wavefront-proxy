/*
 * Copyright (c) 2019 VMware, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License.  You may obtain a copy of
 * the License at http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, without warranties or
 * conditions of any kind, EITHER EXPRESS OR IMPLIED.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

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
 * This verticle is responsible for starting all the verticles as per their dependency
 */
public class MainVerticle extends AbstractVerticle {

    private static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    private Map<String, BaseHttpEndpoint> BaseHttpEndpointMap;

    private int port;

    public MainVerticle(Map<String, BaseHttpEndpoint> BaseHttpEndpointMap, int port) {
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
                logger.info("All required verticles started, starting HttpServerVerticle");
                Promise<String> httpServerDeployment = Promise.promise();
                vertx.deployVerticle(() -> new HttpServerVerticle(router, port, "/log-forwarder/health"),
                        deploymentOptions, httpServerDeployment);
                return httpServerDeployment.future();
            }
        }).setHandler(ar -> {
            if (ar.succeeded()) {
                logger.info("Successfully started main verticle " + deploymentID());
                startPromise.complete();
            } else {
                logger.error("Failed starting some verticle", ar.cause());
                startPromise.fail(ar.cause());
            }
        });
    }

    public static Vertx deploy(Map<String, BaseHttpEndpoint> BaseHttpEndpointMap, int instanceCount, int port) {
        Vertx vertx = Vertx.vertx(new VertxOptions());
        vertx.deployVerticle(() -> new MainVerticle(BaseHttpEndpointMap, port), new DeploymentOptions()
                .setInstances(instanceCount));
        return vertx;
    }
}
