package com.wavefront.agent;

import java.util.logging.Logger;
import java.util.logging.Level;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.socket.DatagramPacket;
import io.netty.util.CharsetUtil;
import sunnylabs.report.ReportPoint;
import com.wavefront.agent.api.ForceQueueEnabledAgentAPI;

/**
 * DogStatsD handler that takes a string in this format:
 *    metric.name:value|type|@sample_rate|#tag1:value,tag2
 * parses and then sends the metric to the wavefront server.
 * Currently only 'g' and 'c' metric types are supported (others are ignored)
 */
public class DogStatsDUDPHandler extends SimpleChannelInboundHandler<DatagramPacket> {
    private static final Logger LOG = Logger.getLogger(
        DogStatsDUDPHandler.class.getCanonicalName());

    /**
     * The point handler that takes report metrics one data point at a time
     * and handles batching and retries, etc
     */
    private final PointHandler pointHandler;

    /**
     * Constructor (matches the other constructors).
     */
    public DogStatsDUDPHandler(final ForceQueueEnabledAgentAPI agentAPI,
                               final UUID daemonId,
                               final int port,
                               final String prefix,
                               final String logLevel,
                               final String validationLevel,
                               final long millisecondsPerBatch,
                               final int blockedPointsPerBatch) {
        this.pointHandler = new PointHandler(agentAPI, daemonId, port, logLevel, validationLevel, millisecondsPerBatch, blockedPointsPerBatch);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void channelRead0(ChannelHandlerContext ctx, DatagramPacket packet) throws Exception {
        final String msg = packet.content().toString(CharsetUtil.UTF_8);
        LOG.info("Received message '" + msg + "'");
        final ReportPoint point =
            decodeMessage(msg, packet.sender().getHostName());
        if (point != null) {
            LOG.fine("Sending point : " + point.toString());
            pointHandler.reportPoint(point, msg);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void channelReadComplete(ChannelHandlerContext ctx) {
        ctx.flush();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        LOG.log(Level.WARNING, "Caught exception in dogstatsd handler", cause);
        // Don't close the channel because we can keep serving requests.
    }

    /**
     * Decodes a message received.  The expected format is:
     *    metric.name:value|type|@sample_rate|#tag1:value,tag2
     * @param msg the incoming message
     * @param source the source/host
     * @return the report point generated or null if the message could not be
     *         parsed or is invalid.
     */
    public ReportPoint decodeMessage(final String msg, final String source) {
        final Map<String, String> annotations = new HashMap<>();

        // split into name and value + metadata and check the message format
        final String[] name_metadata = msg.split(":", 2);
        if (name_metadata.length != 2) {
            // not a valid message
            LOG.warning("Unsupported DogStatsD format: '" + msg + "'");
            return null;
        }
        final String[] parts = name_metadata[1].split("\\|");
        if (parts.length <= 1) {
            LOG.warning("Unsupported DogStatsD message: '" + msg + "'");
            return null;
        }

        // check the metric type is supported
        if (parts[1].charAt(0) != 'g' && parts[1].charAt(0) != 'c') {
            LOG.warning("Skipping DogStatsD metric type: '" + parts[1] + "' (" + msg + ")");
            return null;
        }

        // skip over the sample rate and find tags
        int loc = 1;
        if (parts.length > loc+1) {
            if (parts[loc].charAt(0) == '@') {
                loc++;
            }
            if (parts.length > loc+1) {
                if (parts[2].charAt(0) == '#') {
                    for (int i = 3; i < parts.length; i++) {
                        final String[] tag = parts[i].split(":");
                        if (tag.length == 2) {
                            annotations.put(tag[0], tag[1]);
                        }
                    }
                }
            }
        }

        return ReportPoint.newBuilder()
            .setHost(source)
            .setAnnotations(annotations)
            .setMetric(name_metadata[0])
            .setValue(Double.parseDouble(parts[0]))
            .setTimestamp(System.currentTimeMillis())
            .setTable("datadog") // TODO: what is table?
            .build();
    }
}
