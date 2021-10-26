package com.wavefront.agent.logforwarder.ingestion.processors;


import com.wavefront.agent.VertxTestUtils;
import com.wavefront.agent.logforwarder.config.LogForwarderArgs;
import com.wavefront.agent.logforwarder.config.LogForwarderConfigProperties;
import com.wavefront.agent.logforwarder.constants.LogForwarderConstants;
import com.wavefront.agent.logforwarder.ingestion.LogForwarderTestBase;
import com.wavefront.agent.logforwarder.ingestion.client.gateway.model.GatewayRequest;
import com.wavefront.agent.logforwarder.ingestion.client.gateway.utils.Utils;
import com.wavefront.agent.logforwarder.ingestion.processors.model.event.EventBatch;
import com.wavefront.agent.logforwarder.ingestion.processors.model.event.EventPayload;
import com.wavefront.agent.logforwarder.ingestion.vertx.GatewayMockVerticle;
import org.apache.http.HttpStatus;
import org.json.simple.JSONAware;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.function.Consumer;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.ext.web.RoutingContext;

/**
 * Tests log ingestion into rest api end point exposed for log ingestion via
 * {@link com.wavefront.agent.logforwarder.ingestion.restapi.LogForwarderRestIngestEndpoint}
 * and processor logic {@link PostToLogIqProcessor} for backend Ingestion gateway
 *
 * @author rmanoj@vmware.com
 */
public class TestPostToLogIqProcessor extends LogForwarderTestBase {
    private int httpPort;
    private int syslogPort;
    private String configFile;

    private static final String SRE_ORG_ID = UUID.randomUUID().toString();
    private static final String ORG_ID = "test-org";
    private static final String SDDC_ID = "testLogForwarder";
    private String ingestionUri;

    @Before
    public void setup() throws Exception {
        super.cleanup();
        this.testPort = VertxTestUtils.getUnusedRandomPort();
        this.httpPort = VertxTestUtils.getUnusedRandomPort();
        this.syslogPort = VertxTestUtils.getUnusedRandomPort();
        this.ingestionUri = String.format(LF_INGESTION_URI_FORMAT, httpPort);
        this.logGatewayUrl = String.format("http://localhost:%d", testPort);
        initializeArgs(ORG_ID, SDDC_ID, "false");
        LogForwarderConfigProperties.logForwarderArgs.sreOrgId = SRE_ORG_ID;
//        LogForwarderConfigProperties.logForwarderArgs.skipLeMansClient = true;
        this.testVerticle = GatewayMockVerticle.deploy(testPort, getVerifyingConsumer());
    }

    @Test
    public void testPortToLogIqProcessorTest() throws Throwable {
        configFile = "test-ingestion-to-org-in-log-gateway.json";
        deployRestApiServices(configFile, httpPort, syslogPort);

        //send simple json data
        sendSimpleJsonData(ingestionUri);

        Callable<Long> succ200 = getMeterCallable("POST-vmc_sre-messages-success-200");
        Callable<Long> succ202 = getMeterCallable("POST-vmc_sre-messages-success-202");
        Callable<Boolean> callable = () -> {
            long successCount200 = succ200.call();
            long successCount202 = succ202.call();
            long successCount = successCount200 + successCount202;
            logger.info("Http - 200 count = {}, Http - 202 count = {}, Total expected = {}", successCount200,
                    successCount202, 100);
            return successCount == 100;
        };
        VertxTestUtils.waitForCondition(200, 20, callable);
//        Assert.assertTrue("Not all the messages are posted to LINT.", (succ200.call() + succ202.call()) == 100L);
    }

    private Consumer<RoutingContext> getVerifyingConsumer() {
        return routingContext -> {
            // Assert the headers in the request
            HttpServerRequest request = routingContext.request();
            int statusCode = HttpStatus.SC_OK;
            boolean allMatching = SRE_ORG_ID.equals(request.getHeader(LogForwarderConstants.HEADER_ARCTIC_SRE_ORG_ID))
                    && ORG_ID.equals(request.getHeader("vmc-org-id"))
                    && request.headers().contains("timestamp")
                    && "default".equals(request.getHeader("structure"))
                    && SDDC_ID.equals(request.getHeader("agent"))
                    && Utils.computeHash(LogForwarderConfigProperties.orgId)
                            .equals(request.getHeader("lm-tenant-id"));

            if (!allMatching) {
                statusCode = HttpStatus.SC_INTERNAL_SERVER_ERROR;
            }
            routingContext.response().setStatusCode(statusCode).end();
        };
    }
}
