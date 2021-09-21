package com.wavefront.agent.logforwarder.ingestion.restapi;

import com.wavefront.agent.logforwarder.ingestion.util.RequestUtil;

import java.util.concurrent.CompletableFuture;

import io.vertx.ext.web.RoutingContext;
import java.util.concurrent.CompletableFuture;
import io.vertx.ext.web.RoutingContext;
import static com.wavefront.agent.logforwarder.ingestion.util.RequestUtil.methodNotSupported;

/**
 * @author Manoj Ramakrishnan (rmanoj@vmware.com).
 * @since 9/17/21 2:57 PM
 */
public interface BaseHttpEndpoint {

    default void handleGet(CompletableFuture future, RoutingContext routingContext) {
      RequestUtil.methodNotSupported(future, routingContext);
    }

    default void handlePost(CompletableFuture future, RoutingContext routingContext) {
      methodNotSupported(future, routingContext);
    }

    default void handlePatch(CompletableFuture future, RoutingContext routingContext) {
      methodNotSupported(future, routingContext);
    }

    default void handleDelete(CompletableFuture future, RoutingContext routingContext) {
      methodNotSupported(future, routingContext);
    }

    default void handlePut(CompletableFuture future, RoutingContext routingContext) {
      methodNotSupported(future, routingContext);
    }
}

