package com.wavefront.agent.logforwarder.ingestion;


import com.wavefront.agent.VertxTestUtils;
import com.wavefront.agent.logforwarder.config.LogForwarderArgs;
import com.wavefront.agent.logforwarder.config.LogForwarderConfigProperties;
import com.wavefront.agent.logforwarder.ingestion.client.gateway.GatewayClientState;
import com.wavefront.agent.logforwarder.ingestion.client.gateway.metrics.MetricsService;
import com.wavefront.agent.logforwarder.ingestion.client.gateway.utils.Utils;
import com.wavefront.agent.logforwarder.ingestion.processors.LogIngestionTestProcessor;
import com.wavefront.agent.logforwarder.ingestion.util.RestApiSourcesMetricsUtil;
import com.wavefront.agent.logforwarder.ingestion.utils.TestUtils;
import com.wavefront.agent.logforwarder.services.LogForwarderConfigService;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.io.IOUtils;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.HttpClientBuilder;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.junit.Before;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.lang.invoke.MethodHandles;
import java.util.concurrent.Callable;
import io.dropwizard.metrics5.Meter;
import io.dropwizard.metrics5.MetricRegistry;
import io.vertx.core.Vertx;

/**
 * A test base class which initializes log forwarder arguments and has helpers to read sample log
 * for testing ingestion related functionality.
 * @author rmanoj@vmware.com
 */
public class LogForwarderTestBase {

    protected static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
    /**
     * NOTE : Before running this test, ensure the following :
     * 1. Get a new access key (API key) from logGatewayUrl and use that as logAccessKey
     * 2. Fill out lemansDataVolume with a local accessible path
     */
    protected String logAccessKey = "Edaxozr9hv5NmKcMjwPBh4CEcJ9X268y";
    protected String logGatewayUrl = "https://data.staging.symphony-dev.com/";
    protected int testPort;
    protected Vertx testVerticle;

    protected String DATA_NOT_IN_JSON_FORMAT = "data-not-in-json-format";
    protected String cfapiText;
    protected String cfapiTextLiAgent;
    protected String simpleText;
    public static String LF_INGESTION_URI_FORMAT = "http://localhost:%d/log-forwarder/ingest";

    protected void initializeArgs(String orgId, String sddcId, String addFwderIdInEvent) {
        LogForwarderConfigProperties.logForwarderArgs = new LogForwarderArgs();
        LogForwarderConfigProperties.logForwarderArgs.logIqUrl = logGatewayUrl;
        LogForwarderConfigProperties.logForwarderArgs.lemansServerUrl = logGatewayUrl;
        LogForwarderConfigProperties.logForwarderArgs.logIqAccessKey = logAccessKey;
        LogForwarderConfigProperties.logForwarderArgs.lemansAccessKey = logAccessKey;
        LogForwarderConfigProperties.logForwarderArgs.lemansClientDiskBackedQueueLocation = "./lemans-data";
        LogForwarderConfigProperties.logForwarderArgs.disableBidirectionalCommunication = true;
        LogForwarderConfigProperties.logForwarderArgs.orgId = orgId;
        LogForwarderConfigProperties.logForwarderArgs.addFwderIdInEvent = addFwderIdInEvent;
        LogForwarderConfigProperties.orgId = orgId;
        LogForwarderConfigProperties.sddcId = sddcId;
    }

    protected void cleanup() throws Exception {
        // Undeploy Rest API verticles
        if (MapUtils.isNotEmpty(LogForwarderConfigProperties.respApiVerticles)) {
            logger.info("Undeploying {} Rest API verticles", LogForwarderConfigProperties.respApiVerticles.size());
            VertxTestUtils.undeployVerticles(LogForwarderConfigProperties.respApiVerticles.values());
        }
        LogForwarderConfigProperties.respApiVerticles.clear();

        // Undeploy test verticle
        if (testVerticle != null) {
            logger.info("Undeploying Test verticle");
            VertxTestUtils.undeployVerticle(testVerticle);
        }

        // Undeploy agent verticle
        if (LogForwarderConfigProperties.logForwarderAgentVertx != null) {
            logger.info("Undeploying Agent verticle");
            VertxTestUtils.undeployVerticle(LogForwarderConfigProperties.logForwarderAgentVertx);
            LogForwarderConfigProperties.logForwarderAgentVertx = null;
        }

        LogForwarderConfigProperties.logForwarderArgs = null;
        LogForwarderConfigProperties.orgId = null;
        LogForwarderConfigProperties.sddcId = null;
        LogForwarderConfigProperties.forwarderType = null;

        // Clear metrics
        TestUtils.clearMetrics();

        GatewayClientState.accessKeyVsLemansClient.clear();
        if (MapUtils.isNotEmpty(LogForwarderConfigProperties.logForwardingIdVsConfig)) {
            LogForwarderConfigProperties.logForwardingIdVsConfig.clear();
        }
    }

    @Before
    public void readData() throws Exception {
        cfapiText = readFile(new java.io.File(".")
                .getCanonicalPath() + "/src/test/resources/ingestion-data/cfapi.json");
        cfapiTextLiAgent = readFile(new java.io.File(".")
                .getCanonicalPath() + "/src/test/resources/ingestion-data/cfapi-li-agent.json");
        simpleText = readFile(new java.io.File(".")
                .getCanonicalPath() + "/src/test/resources/ingestion-data/simple.json");
        LogIngestionTestProcessor.eventPayload = null;
    }

    /**
     * send 100 cfapi messages to the url passed as parameter
     *
     * @param cfapiUri http url, not null
     * @throws Exception
     */
    protected void sendNonJsonData(String cfapiUri) throws Exception {
        logger.info("going to send http data");
        for (int i = 0; i < 100; i++) {
            HttpPost post = new HttpPost(cfapiUri);
            post.setEntity(new StringEntity(DATA_NOT_IN_JSON_FORMAT));
            HttpClient client = HttpClientBuilder.create().build();
            HttpResponse response = client.execute(post);
        }
        logger.info("done sending http data");
    }

    /**
     * send 100 cfapi messages to the url passed as parameter
     *
     * @param cfapiUri http url, not null
     * @throws Exception
     */
    protected void sendCfapiData(String cfapiUri) throws Exception {
        logger.info("going to send http data");
        for (int i = 0; i < 100; i++) {
            HttpPost post = new HttpPost(cfapiUri);
            post.setEntity(new StringEntity(cfapiText));
            HttpClient client = HttpClientBuilder.create().build();
            HttpResponse response = client.execute(post);
            //logger.info("post cfapi message, response code = {}", response.getStatusLine().getStatusCode());
        }
        logger.info("done sending http data");

    }

    /**
     * send 100 simple json messages to the url passed as parameter
     *
     * @param httpUri http url, not null
     * @throws Exception
     */
    protected void sendSimpleJsonData(String httpUri) throws Exception {
        logger.info("going to send http simple data on http uri: " + httpUri);
        for (int i = 0; i < 100; i++) {
            HttpPost post = new HttpPost(httpUri);
            post.setEntity(new StringEntity(simpleText));
            post.setHeader("structure", "default");
            HttpClient client = HttpClientBuilder.create().build();
            HttpResponse response = client.execute(post);
            logger.info("post simple json message, response code = {}", response.getStatusLine().getStatusCode());
        }
        logger.info("done sending http simpledata");
    }

    protected String readFile(String fileName) throws Exception {
        try (InputStream testFileStream = new FileInputStream(fileName)) {
            return IOUtils.toString(testFileStream, "UTF-8");
        }
    }

    protected String getJsonFromFile(String fileName) {
        String jsonTxt = null;
        try {
            InputStream inputStream
                    = LogForwarderTestBase.class.getClassLoader().getResourceAsStream(fileName);
            jsonTxt = IOUtils.toString(inputStream);
            inputStream.close();
        } catch (IOException ioe) {
            throw new RuntimeException("Unable to read Json", ioe);
        }
        return jsonTxt;
    }

    protected Meter getRestApiSourceMeterStartingWith(String metricName) {
        return getMeterStartingWith(RestApiSourcesMetricsUtil.restApiMetricsRegistry, metricName);
    }

    private Meter getMeterStartingWith(MetricRegistry registry, String metricName) {
        Meter[] meters = new Meter[1];
        registry.getMeters().forEach((meterName, meter) -> {
            if (meterName.getKey().startsWith(metricName)) {
                meters[0] = meter;
            }
        });
        return meters[0];
    }

    protected Callable<Long> getMeterCallable(String name) {
        return () -> MetricsService.getInstance().getMeter(name).getCount();
    }

    protected long getMeterCount(String name) {
        return MetricsService.getInstance().getMeter(name).getCount();
    }

    protected void deployRestApiServices(String configFile, int httpPort, int syslogPort)
            throws Exception {
        String config = TestUtils.readFile(getClass().getClassLoader(), configFile);
        config = updatePorts(config, httpPort, syslogPort);
        LogForwarderConfigService.startRestApiServices(config);
        Thread.sleep(2000L);
    }

    // Config contains static ports, replace them with dynamic ports.
    private String updatePorts(String config, int httpPort, int syslogPort) throws Exception {
        System.out.println("Before Json" + config);
        JSONArray jsonArray = (JSONArray) new JSONParser().parse(config);
        jsonArray.forEach((componentConfig) -> {
            JSONObject componentConfigJSON = (JSONObject) componentConfig;
            if (componentConfigJSON.get("syslogPort") != null) {
                componentConfigJSON.put("syslogPort", String.valueOf(syslogPort));
            }
            if (componentConfigJSON.get("httpPort") != null) {
                componentConfigJSON.put("httpPort", String.valueOf(vertxHttpPort(httpPort, true)));
            }
        });
        System.out.println("After Json" + Utils.toJson(jsonArray));
        return Utils.toJson(jsonArray);
    }

    protected int vertxHttpPort(int port, boolean enableVertx) {
        // If vert.x is enabled we add '101' to port, so subtracting it.
        return enableVertx ? port - 101 : port;
    }
}
