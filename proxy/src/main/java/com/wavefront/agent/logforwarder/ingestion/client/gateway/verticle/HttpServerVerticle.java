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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.invoke.MethodHandles;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.Promise;
import io.vertx.core.http.HttpMethod;
import io.vertx.core.http.HttpServer;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;

public class HttpServerVerticle extends AbstractVerticle {

    private static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    private Router router;
    private int port;
    private String healthUrl;

    public HttpServerVerticle(Router router, int port, String healthUrl) {
        this.router = router;
        this.port = port;
        this.healthUrl = healthUrl;
    }

    @Override
    public void start(Promise<Void> promise) {
        HttpServer server = vertx.createHttpServer();
        router.route(HttpMethod.GET, healthUrl).order(0).handler(this::healthHandler);

        server.requestHandler(router).listen(port, ar -> {
            if (ar.succeeded()) {
                logger.info("HTTP server running on port " + port);
                promise.complete();
            } else {
                logger.error("Could not start a HTTP server", ar.cause());
                promise.fail(ar.cause());
            }
        });
    }

    private void healthHandler(RoutingContext context) {
        context.response().end();
    }

    public static String getName() {
        return MethodHandles.lookup().lookupClass().getSimpleName();
    }
}
