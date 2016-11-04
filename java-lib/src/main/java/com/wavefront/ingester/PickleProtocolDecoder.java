package com.wavefront.ingester;

import com.google.common.base.Preconditions;
import com.wavefront.common.MetricMangler;

import net.razorvine.pickle.Unpickler;
import sunnylabs.report.ReportPoint;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;

/**
 * Pickle protocol format decoder.
 * https://docs.python.org/2/library/pickle.html
 * @author Mike McLaughlin (mike@wavefront.com)
 */
public class PickleProtocolDecoder implements Decoder<byte[]> {

  protected static final Logger logger = Logger.getLogger(PickleProtocolDecoder.class.getCanonicalName());

  private final int port;
  private final String defaultHostName;
  private final List<String> customSourceTags;
  private final MetricMangler metricMangler;
  private final ThreadLocal<Unpickler> unpicklerThreadLocal = new ThreadLocal<Unpickler>() {
    @Override
    protected Unpickler initialValue() {
      return new Unpickler();
    }
  };

  /**
   * Constructor.
   * @param hostName the default host name.
   * @param customSourceTags list of source tags for this host.
   * @param mangler the metric mangler object.
   * @param port the listening port (for debug logging)
   */
  public PickleProtocolDecoder(String hostName, List<String> customSourceTags,
                               MetricMangler mangler, int port) {
    Preconditions.checkNotNull(hostName);
    this.defaultHostName = hostName;
    Preconditions.checkNotNull(customSourceTags);
    this.customSourceTags = customSourceTags;
    this.metricMangler = mangler;
    this.port = port;
  }

  @Override
  public void decodeReportPoints(byte[] msg, List<ReportPoint> out, String customerId) {
    InputStream is = new ByteArrayInputStream(msg);
    Object dataRaw;
    try {
      dataRaw = unpicklerThreadLocal.get().load(is);
      if (!(dataRaw instanceof List)) {
        throw new IllegalArgumentException(
            String.format("[%d] unable to unpickle data (unpickle did not return list)", port));
      }      
    } catch (final IOException ioe) {
      throw new IllegalArgumentException(String.format("[%d] unable to unpickle data", port), ioe);
    }

    // [(path, (timestamp, value)), ...]
    List<Object[]> data = (List<Object[]>) dataRaw;
    for (Object[] o : data) {
      Object[] details = (Object[])o[1];
      if (details == null || details.length != 2) {
        logger.warning(String.format("[%d] Unexpected pickle protocol input", port));
        continue;
      }
      long ts;
      if (details[0] == null) {
        logger.warning(String.format("[%d] Unexpected pickle protocol input (timestamp is null)", port));
        continue;
      } else if (details[0] instanceof Double) {
        ts = ((Double)details[0]).longValue() * 1000;
      } else if (details[0] instanceof Long) {
        ts = ((Long)details[0]).longValue() * 1000;
      } else if (details[0] instanceof Integer) {
        ts = ((Integer)details[0]).longValue() * 1000;
      } else {
        logger.warning(String.format("[%d] Unexpected pickle protocol input (details[0]: %s)",
            port, details[0].getClass().getName()));
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
        logger.warning(String.format("[%d] Unexpected pickle protocol input (value is null)", port));
        continue;
      }

      ReportPoint point = new ReportPoint();
      MetricMangler.MetricComponents components =
          this.metricMangler.extractComponents(o[0].toString());
      point.setMetric(components.metric);
      String host = components.source;
      final Map<String, String> annotations = point.getAnnotations();
      if (host == null && annotations != null) {
        // iterate over the set of custom tags, breaking when one is found
        for (final String tag : customSourceTags) {
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
      point.setAnnotations(Collections.<String, String>emptyMap());
      out.add(point);
    }
  }

  @Override
  public void decodeReportPoints(byte[] msg, List<ReportPoint> out) {
    decodeReportPoints(msg, out, "dummy");
  }
}
