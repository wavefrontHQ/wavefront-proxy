package com.wavefront.ingester;

import com.google.common.base.Preconditions;

import com.wavefront.common.MetricMangler;

import io.netty.channel.ChannelHandlerContext;
import net.razorvine.pickle.PickleUtils;
import net.razorvine.pickle.Unpickler;
import sunnylabs.report.ReportPoint;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Pickle protocol format decoder.
 * https://docs.python.org/2/library/pickle.html
 */
public class PickleProtocolDecoder implements Decoder<byte[]> {

  protected static final Logger logger = Logger.getLogger("agent");

  private final String defaultHostName;
  private final List<String> customSourceTags;
  private final MetricMangler metricMangler;

  /**
   * Constructor.
   * @param customSourceTags list of tags that should be considered the host.
   * @param mangler the metric mangler object.
   */
  public PickleProtocolDecoder(List<String> customSourceTags,
                               MetricMangler mangler) {
    this.defaultHostName = "unknown";
    Preconditions.checkNotNull(customSourceTags);
    this.customSourceTags = customSourceTags;
    this.metricMangler = mangler;
  }

  /**
   * Constructor.
   * @param hostName the default host name.
   * @param customSourceTags list of source tags for this host.
   * @param mangler the metric mangler object.
   */
  public PickleProtocolDecoder(String hostName, List<String> customSourceTags,
                               MetricMangler mangler) {
    Preconditions.checkNotNull(hostName);
    this.defaultHostName = hostName;
    Preconditions.checkNotNull(customSourceTags);
    this.customSourceTags = customSourceTags;
    this.metricMangler = mangler;
  }

  @Override
  public void decodeReportPoints(ChannelHandlerContext ctx, byte[] msg, List<ReportPoint> out, String customerId) {
    InputStream is = new ByteArrayInputStream(msg);
    Unpickler unpickler = new Unpickler();
    Object dataRaw = null;
    try {
      dataRaw = unpickler.load(is);
      if (!(dataRaw instanceof List)) {
        throw new IllegalArgumentException("unable to unpickle data");
      }      
    } catch (final java.io.IOException ioe) {
      throw new IllegalArgumentException("unable to unpickle data", ioe);
    }

    // [(path, (timestamp, value)), ...]
    List<Object[]> data = (List<Object[]>) dataRaw;
    for (Object[] o : data) {
      Object[] details = (Object[])o[1];
      if (details == null || details.length != 2) {
        logger.warning("Unexpected pickle protocol input");
        continue;
      }
      long ts;
      if (details[0] == null) {
        logger.warning("Unexpected pickle protocol input (timestamp is null)");
        continue;
      } else if (details[0] instanceof Double) {
        ts = ((Double)details[0]).longValue() * 1000;
      } else if (details[0] instanceof Long) {
        ts = ((Long)details[0]).longValue() * 1000;
      } else if (details[0] instanceof Integer) {
        ts = ((Integer)details[0]).longValue() * 1000;
      } else {
        logger.warning("Unexpected pickle protocol input (details[0]: "
                       + details[0].getClass().getName() + ")");
        continue;
      }

      if (details[1] == null) {
        continue;
      }
      
      double value;
      if (details[1] instanceof Double) {
        value = ((Double)details[1]).doubleValue();
      } else if (details[1] instanceof Long) {
        value = ((Long)details[1]).longValue();
      } else if (details[1] instanceof Integer) {
        value = ((Integer)details[1]).intValue();
      } else {
        logger.warning("Unexpected pickle protocol input (value is null)");
        continue;
      }

      ReportPoint point = new ReportPoint();
      MetricMangler.MetricComponents components =
          this.metricMangler.extractComponents(o[0].toString());
      point.setMetric(components.metric);
      String host = components.source;
      if (host == null) {
        final Map<String, String> annotations = point.getAnnotations();
        // iterate over the set of custom tags, breaking when one is found
        for (String tag : customSourceTags) {
          host = annotations.remove(tag);
          if (host != null) {
            break;
          }
        }
        if (host == null) {
          host = this.defaultHostName;
        }
      }
      point.setHost(host);
      point.setTable(customerId);
      point.setTimestamp(ts);
      point.setValue(value);
      point.setAnnotations(new HashMap<String, String>());
      out.add(point);
    }
  }

  @Override
  public void decodeReportPoints(ChannelHandlerContext ctx, byte[] msg, List<ReportPoint> out) {
    decodeReportPoints(ctx, msg, out, "dummy");
  }
}
