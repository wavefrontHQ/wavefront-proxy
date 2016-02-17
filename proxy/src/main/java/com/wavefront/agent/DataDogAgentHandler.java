package com.wavefront.agent;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.http.DefaultFullHttpResponse;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpContent;
import io.netty.handler.codec.http.HttpHeaders;
import io.netty.handler.codec.http.HttpObject;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpVersion;
import io.netty.handler.codec.http.LastHttpContent;
import io.netty.util.CharsetUtil;

import static io.netty.handler.codec.http.HttpHeaders.Names.*;

import java.nio.ByteBuffer;
import java.util.zip.Inflater;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.logging.Logger;
import java.util.logging.Level;
import java.util.Map;
import java.util.Map.Entry;
import java.util.UUID;

import com.wavefront.agent.api.ForceQueueEnabledAgentAPI;
import sunnylabs.report.ReportPoint;

import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.JsonNode;

/**
 * This class is a netty channel handler for metrics arriving from
 * a DataDog agent.  This handler operates as a mini HTTP server and returns
 * a 200 status code for all requests (unless an exception occurs).
 * To use this, change datadog agent configuration(s) to point the dd_url
 * value to the Wavefront proxy where this handler is running.
 * To use this to send data to both datadog and WF, be sure to put a proxy
 * in front of this handler (teeproxy, or similar) and duplicate requests to
 * this handler.
 * This is based off the netty example HttpSnoopServer found here:
 * https://github.com/netty/netty/tree/4.1/example/src/main/java/io/netty/example/http/snoop
 * This class was created from the example provided in HttpSnoopServerHandler
 * class in the above directory.
 */
@ChannelHandler.Sharable
public class DataDogAgentHandler extends SimpleChannelInboundHandler<Object> {
    private static final Logger LOG = Logger.getLogger(
        DataDogAgentHandler.class.getCanonicalName());

    /**
     * The HTTP request object passed to channelRead0()
     */
    private HttpRequest request;

    /**
     * The point handler that takes report metrics one data point at a time
     * and handles batching and retries, etc
     */
    private final PointHandler pointHandler;

    public DataDogAgentHandler(final ForceQueueEnabledAgentAPI agentAPI,
                               final UUID daemonId,
                               final int port,
                               final String prefix,
                               final String logLevel,
                               final String validationLevel,
                               final long millisecondsPerBatch,
                               final int blockedPointsPerBatch) {
        this.pointHandler = new PointHandler(agentAPI, daemonId, port, logLevel, validationLevel, millisecondsPerBatch, blockedPointsPerBatch);
    }

    @Override
    public void channelReadComplete(ChannelHandlerContext ctx) throws Exception {
        ctx.flush();
    }

    @Override
    protected void channelRead0(io.netty.channel.ChannelHandlerContext ctx,
                                Object msg) {
        if (msg instanceof HttpRequest) {
            this.request = (HttpRequest) msg;
            LOG.info(String.format("%s %s", request.getMethod(), request.getUri()));
            // TODO: anything to do here?
        }

        // NOTE: this requires the use of the HttpObjectAggregator in the netty
        // pipeline
        if (msg instanceof LastHttpContent) {
            final HttpContent httpContent = (HttpContent) msg;
            LOG.fine(String.format("[%s] CONTENT\n%s", request.getUri(), request));
            final ByteBuf content = httpContent.content();
            final String header = request.headers().get("Content-Encoding");
            final boolean compressed = (header != null && header.equalsIgnoreCase("deflate"));
            if (request.getUri().startsWith("/intake")) {
                final JsonNode root = parseJson(content, compressed);
                handleIntakeRequest(root);
            } else if (request.getUri().startsWith("/api/v1/series")) {
                final JsonNode root = parseJson(content, compressed);
                handleApiSeries(root);
            } else if (request.getUri().equals("/")) { // assume point series
                final JsonNode root = parseJson(content, compressed);
                handleApiSeries(root);
            } else {
                LOG.warning(String.format("Ignoring %s %s:\n%s", request.getMethod(), request.getUri(), content.toString(CharsetUtil.UTF_8)));
            }

            writeResponse(httpContent, ctx);
        }
    }

    /**
     * Decompress the provided string and parse the resulting JSON string
     * @param content the HttpContent (compressed JSON)
     * @param isCompressed is the contents compressed?
     * @return the parsed root node
     */
    private JsonNode parseJson(final ByteBuf content,
                               final boolean isCompressed) {
        if (!content.isReadable()) {
            LOG.warning(String.format("[%s] Unable to read content. Ignoring",
                                      request.getUri()));
            throw new IllegalArgumentException("Unable to read content");
        }

        // get the contents of the HTTP message body as a byte array
        final ByteBuffer data = ByteBuffer.allocate(content.readableBytes());
        content.getBytes(0, data);
        byte[] jsonBytes = null;
        if (isCompressed) {
            final byte[] compressed = data.array();
            jsonBytes = new byte[compressed.length * 100];
            if (isCompressed) {
                try {
                    // decompress the message
                    final Inflater decompressor = new Inflater();
                    decompressor.setInput(compressed);
                    decompressor.inflate(jsonBytes);
                } catch (final java.util.zip.DataFormatException e) {
                    LOG.log(Level.WARNING, "Failed to decompress message", e);
                    throw new IllegalArgumentException("Unable to decompress message", e);
                }
            }
        } else {
            jsonBytes = data.array();
        }

        // decompressed - now parse JSON
        final ObjectMapper jsonTree = new ObjectMapper();
        try {
            return jsonTree.readTree(jsonBytes);
        } catch (final java.io.IOException e) {
            LOG.log(Level.WARNING,
                    String.format("Unable to parse JSON\n%s", jsonBytes),
                    e);
            throw new IllegalArgumentException("Unable to parse JSON", e);
        }
    }
    
    /**
     * Handles the HTTP request from the datadog agent with the URI:
     *     /intake/?api_key=<datadog api key>
     * ASSUMPTION: the content body is zip'd using ZLib and the value is
     *      a JSON object.
     * Given a json message, this will parse and find the metrics and post those
     * to the WF server. The JSON is expected to look like this:
     * {
     *    "metrics": [
     *      [
     *          "system.disk.total",
     *          1451409097,
     *          497448.0,
     *          {
     *              "device_name": "udev",
     *              "hostname": "mike-ubuntu14",
     *              "type": "gauge"
     *          }
     *      ],
     *      ...
     *   }
     * Each metric in the metrics array is consider a report point and is
     * sent to the WF server using the PointHandler object.  The metric array
     * element is made up of:
     *     (0):  metric name
     *     (1):  timestamp (epoch seconds)
     *     (2):  value (assuming float for all values)
     *     (3):  tags (including host); all tags are converted to tags except
     *           hostname which is sent on its own as the source for the point.
     *
     * In addition to the metric array elements, all top level elements that
     * begin with :
     *    cpu*
     *    mem*
     * are captured and the value is sent.  These items are in the form of:
     * {
     *  ...
     *    "collection_timestamp": 1451409092.995346,
     *    "cpuGuest": 0.0,
     *    "cpuIdle": 99.33,
     *    "cpuStolen": 0.0,
     *  ...
     *    "internalHostname": "mike-ubuntu14",
     *  ...
     * }
     * The names are retrieved from the JSON key name splitting the key
     * on upper case letters and adding a dot between to form a metric name
     * like this example:
     *    "cpuGuest" => "cpu.guest"
     * The value comes from the JSON key's value.
     *
     * @param root root node of the parsed HttpContent body
     */
    private void handleIntakeRequest(final JsonNode root) {
        // get the hostname used by all of the top level metrics
        final String hostName = root.findPath("internalHostname").asText();
        // get the collection timestamp for all the top level metrics
        final double ts = root.findPath("collection_timestamp").asDouble();

        // iterator over all the top level fields and pull out the name/value
        // pairs of all items we care about
        // {
        //     "cpuIdle": 0.00,
        //     "cpuUser": 0.00,
        //     ....
        // }
        Iterator<Map.Entry<String, JsonNode>> fields = root.getFields();
        while (fields.hasNext()) {
            Map.Entry<String, JsonNode> field = fields.next();
            if (field.getKey().startsWith("cpu") ||
                field.getKey().startsWith("mem"))
            {
                final ReportPoint point = ReportPoint.newBuilder()
                    .setMetric(covertKeyToDottedName(field.getKey()))
                    .setTimestamp((long)ts * 1000) // convert to ms
                    .setHost(hostName)
                    .setValue(field.getValue().asDouble())
                    .setTable("datadog") // TODO: what is table?
                    .build();
                LOG.finer("reporting point: " + point);
                pointHandler.reportPoint(point, root.toString());
            }
        }

        // metrics array items
        final JsonNode metrics = root.findPath("metrics");
        for (final JsonNode metric : metrics) {
            // pull out the tags and then search for the hostname
            // we won't send the hostname as a tag, we'll send that as "source"
            // to WF point handler
            final JsonNode tags = metric.get(3);
            final Map<String, String> annotations = new HashMap<>();
            JsonNode host = null;
            fields = tags.getFields();
            while (fields.hasNext()) {
                Map.Entry<String, JsonNode> field = fields.next();
                if (field.getKey().equals("hostname")) {
                    host = field.getValue();
                } else {
                    annotations.put(field.getKey(), field.getValue().asText());
                }
            }

            // assuming we found a host, then send the details to WF
            if (host != null) {
                final ReportPoint point = ReportPoint.newBuilder()
                    .setAnnotations(annotations)
                    .setMetric(metric.get(0).asText())
                    .setTimestamp(metric.get(1).asLong() * 1000) // convert to ms
                    .setHost(host.asText())
                    .setValue(metric.get(2).asDouble())
                    .setTable("datadog") // TODO: what is table?
                    .build();
                LOG.finer("reporting point: " + point);
                pointHandler.reportPoint(point, root.toString());
            }
        }
    }

    /**
     * Handles the HTTP request from the datadog agent with the URI:
     *      /api/v1/series/?api_key=<datadog api key>
     * JSON is expected to look like :
     * {
     *     "series": [
     *        {
     *             "device_name" : null,
     *             "host": "<host name>",
     *             "interval": 10.0,
     *             "metric": "<name>",
     *             "points": [
     *                [
     *                   1451950930.0,
     *                   0
     *                ]
     *             ],
     *             "tags": null,
     *             "type": "gauge"
     *        },
     *        ...
     *     ]
     * }
     * The point element is made up of:
     *    (0):   timestamp (epoch seconds)
     *    (1):   value (numeric)
     * @param root root node of the parsed HttpContent JSON body
     */
    private void handleApiSeries(final JsonNode root) {
        // ignore everything else and get the "series" array
        final JsonNode metrics = root.findPath("series");
        for (final JsonNode metric : metrics) {
            String metricName = metric.findPath("metric").asText();

            // grab the tags
            final Map<String, String> annotations = new HashMap<>();
            final JsonNode tags = metric.findPath("tags");
            if (tags.isArray()) {
                // assumption: must be an array, values are strings; format:
                // name:value
                for (final JsonNode tag : tags) {
                    final String namevalue = tag.asText();
                    final String[] parts = namevalue.split(":");
                    if (parts.length != 2) {
                        LOG.warning(String.format("Expected tag to be in format <name>:<value> but got '%s'.  Ignoring this tag.", namevalue));
                        continue;
                    }
                    annotations.put(parts[0], parts[1]);
                }
            }

            final JsonNode points = metric.findPath("points");
            for (final JsonNode pt : points) {
                final ReportPoint point = ReportPoint.newBuilder()
                    .setAnnotations(annotations)
                    .setMetric(metricName)
                    .setTimestamp(pt.get(0).asLong() * 1000) // convert to ms
                    .setHost(metric.findPath("host").asText())
                    .setValue(pt.get(1).asDouble())
                    .setTable("datadog") // TODO: what is table?
                    .build();
                LOG.finer("reporting point: " + point);
                pointHandler.reportPoint(point, root.toString());
            }
        }
    }

    /**
     * Writes the response - 200 if everything went ok.
     * This is mostly the same as what was found in the snoop example referenced
     * above in the class details.
     */
    private void writeResponse(final HttpObject current,
                               final ChannelHandlerContext ctx) {
        // Decide whether to close the connection or not.
        final boolean keepAlive = HttpHeaders.isKeepAlive(request);
        // Build the response object.
        final HttpResponseStatus status = current.getDecoderResult().isSuccess() ? HttpResponseStatus.OK : HttpResponseStatus.BAD_REQUEST;
        final FullHttpResponse response = new DefaultFullHttpResponse(
            HttpVersion.HTTP_1_1, status, Unpooled.copiedBuffer("", CharsetUtil.UTF_8));

        response.headers().set(CONTENT_TYPE, "text/plain; charset=UTF-8");

        if (keepAlive) {
            // Add 'Content-Length' header only for a keep-alive connection.
            response.headers().set(CONTENT_LENGTH, response.content().readableBytes());
            // Add keep alive header as per:
            // - http://www.w3.org/Protocols/HTTP/1.1/draft-ietf-http-v11-spec-01.html#Connection
            response.headers().set(CONNECTION, HttpHeaders.Values.KEEP_ALIVE);
        }

        // Write the response.
        LOG.fine("response: " + response.toString());
        ctx.write(response);
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
        LOG.log(Level.WARNING, "Failed", cause);
        // TODO: write 500 response
        ctx.close();
    }
    
    /**
     * Convert a key that is camel-case notation to a dotted equivalent. This
     * is best described with an example:
     *    key = "memPhysFree"
     *    returns "mem.phys.free"
     * @param key a camel-case string value
     * @return dotted notation with each uppercase containing a dot before
     */
    private String covertKeyToDottedName(final String key) {
        final StringBuilder sb = new StringBuilder();
        for (int i = 0; i < key.length(); i++) {
            final char c = key.charAt(i);
            if (Character.isUpperCase(c)) {
                sb.append(".");
                sb.append(Character.toLowerCase(c));
            } else {
                sb.append(c);
            }
        }
        return sb.toString();
    }
}
