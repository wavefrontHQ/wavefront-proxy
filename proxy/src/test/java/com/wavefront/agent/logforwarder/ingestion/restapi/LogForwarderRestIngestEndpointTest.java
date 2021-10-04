package com.wavefront.agent.logforwarder.ingestion.restapi;

import com.wavefront.agent.VertxTestUtils;
import com.wavefront.agent.logforwarder.config.LogForwarderArgs;
import com.wavefront.agent.logforwarder.config.LogForwarderConfigProperties;
import com.wavefront.agent.logforwarder.ingestion.client.gateway.utils.Utils;
import com.wavefront.agent.logforwarder.ingestion.processors.config.ComponentConfig;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import io.vertx.core.DeploymentOptions;
import io.vertx.core.Vertx;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import org.apache.http.HttpStatus;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;


/**
 *  Unit test for testing basic json format Rest endpoint log forwarder exposes
 *  {@link LogForwarderRestIngestEndpoint}
 *
 * @author rmanoj@vmware.com
 */
@RunWith(VertxUnitRunner.class)
public class LogForwarderRestIngestEndpointTest {
    private Vertx vertx;
    private int port;
    private static long REQ_TIME_OUT_MILLIS = 5000;

    @Before
    public void setUp(TestContext context) throws Exception {
        String componentName = "test-config";
        LogForwarderConfigProperties.logForwarderArgs = new LogForwarderArgs();
        // No worker threads to consume buffers.
        LogForwarderConfigProperties.logForwarderArgs.noOfWorkerThreads = 0;
        // Queue size of 1.
        LogForwarderConfigProperties.inMemoryBufferSize = 1;
        ComponentConfig componentConfig = new ComponentConfig();
        componentConfig.component = componentName;
        componentConfig.processors = new ArrayList<>();
        LogForwarderConfigProperties.componentConfigMap.put(componentName, componentConfig);

        vertx = Vertx.vertx();
        port = VertxTestUtils.getUnusedRandomPort();

        LogForwarderRestIngestEndpoint ingestService = new LogForwarderRestIngestEndpoint(componentName);
        Map<String, BaseHttpEndpoint> BaseHttpEndpointMap = new HashMap<>();
        BaseHttpEndpointMap.put(LogForwarderRestIngestEndpoint.SELF_LINK, ingestService);

        vertx.deployVerticle(() -> new RestApiVerticle(BaseHttpEndpointMap, port), new DeploymentOptions().setInstances(1),
                context.asyncAssertSuccess());
    }

    @After
    public void tearDown(TestContext context) {
        LogForwarderConfigProperties.logForwarderArgs = null;
        LogForwarderConfigProperties.componentConfigMap.clear();
        vertx.close(context.asyncAssertSuccess());
    }

    @Test
    public void testIngestion(TestContext context) {
        // First message will fill the buffers.
        final Async async1 = context.async();
        vertx.createHttpClient().post(port, "localhost",
            LogForwarderRestIngestEndpoint.SELF_LINK + "/", response -> {
                    response.bodyHandler(body -> {
                        context.assertEquals(HttpStatus.SC_OK, response.statusCode());
                        async1.complete();
                    });
                })
                .putHeader("Content-Length",  String.valueOf(Utils.toJson("{}").length()))
                .write(Utils.toJson("{}")).end();
        async1.awaitSuccess(REQ_TIME_OUT_MILLIS);

        // No buffers to consume this message.
        final Async async2 = context.async();
        vertx.createHttpClient().post(port, "localhost",
            LogForwarderRestIngestEndpoint.SELF_LINK + "/", response -> {
                    response.bodyHandler(body -> {
                        context.assertEquals(HttpStatus.SC_SERVICE_UNAVAILABLE, response.statusCode());
                        async2.complete();
                    });
                })
                .putHeader("Content-Length",  String.valueOf(Utils.toJson("{}").length()))
                .write(Utils.toJson("{}")).end();
        async2.awaitSuccess(REQ_TIME_OUT_MILLIS);
    }
}
