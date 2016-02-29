package com.wavefront.ingester;

import java.util.logging.Logger;
import java.util.logging.Level;
import java.util.Iterator;
import java.util.List;
import java.util.HashMap;
import java.util.Map;

import sunnylabs.report.ReportPoint;

/**
 * DogStatsD decoder that takes a string in this format:
 *    metric.name:value|type|@sample_rate|#tag1:value,tag2
 */
public class DogStatsDDecoder implements Decoder {
    private static final Logger LOG = Logger.getLogger(
        DogStatsDDecoder.class.getCanonicalName());

    /**
     * {@inheritDoc}
     */
    @Override
    public void decodeReportPoints(String msg, List<ReportPoint> out, String customerId) {
        final Map<String, String> annotations = new HashMap<>();
        final String[] name_metadata = msg.split(":", 2);
        if (name_metadata.length != 2) {
            // not a valid message
            LOG.warning("Unsupported DogStatsD format: '" + msg + "'");
            return;
        }
        final String[] parts = name_metadata[1].split("|");
        if (parts.length <= 1) {
            LOG.warning("Unsupported DogStatsD message: '" + msg + "'");
            return;
        }
        if (parts[1].charAt(0) != 'g' && parts[1].charAt(0) != 'c') {
            LOG.warning("Skipping DogStatsD metric type: '" + parts[1] + "' (" + msg + ")");
            return;
        }

        if (parts.length > 2 && parts[3].charAt(0) == '#') {
            for (int i = 3; i < parts.length; i++) {
                final String[] tag = parts[i].split(":");
                if (tag.length == 2) {
                    annotations.put(tag[0], tag[1]);
                }
            }
        }

        out.add(ReportPoint.newBuilder()
                .setAnnotations(annotations)
                .setMetric(name_metadata[0])
                .setValue(parts[0])
                .setTable("datadog") // TODO: what is table?
                .setHost(getHostName()).build());
        LOG.warning(out.get(0).toString());
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void decodeReportPoints(String msg, List<ReportPoint> out) {
        throw new IllegalStateException("No customer ID set for dogstatsd format");
    }

    /**
     * Gets the hostname (assumes windows or unix).  This code was lifted from
     * this SO question:
     * http://stackoverflow.com/a/17958246
     */
    private String getHostName() {
        if (System.getProperty("os.name").startsWith("Windows")) {
            // Windows will always set the 'COMPUTERNAME' variable
            return System.getenv("COMPUTERNAME");
        } else {
            return System.getenv("HOSTNAME");                
        }
    }
}
