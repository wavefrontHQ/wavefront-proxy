package com.wavefront.agent.histogram;

import com.google.common.base.Preconditions;
import com.tdunning.math.stats.AgentDigest;
import com.tdunning.math.stats.TDigest;
import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.Histogram;
import com.yammer.metrics.core.MetricName;
import net.openhft.chronicle.bytes.Bytes;
import net.openhft.chronicle.core.io.IORuntimeException;
import net.openhft.chronicle.core.util.ReadResolvable;
import net.openhft.chronicle.hash.serialization.BytesReader;
import net.openhft.chronicle.hash.serialization.BytesWriter;
import net.openhft.chronicle.wire.WireIn;
import net.openhft.chronicle.wire.WireOut;
import wavefront.report.ReportPoint;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Helpers around histograms
 *
 * @author Tim Schmidt (tim@wavefront.com).
 */
public final class HistogramUtils {
  private HistogramUtils() {
    // Not instantiable
  }

  /**
   * Generates a {@link HistogramKey} according a prototype {@link ReportPoint} and
   * {@link Granularity}.
   */
  public static HistogramKey makeKey(ReportPoint point, Granularity granularity) {
    Preconditions.checkNotNull(point);
    Preconditions.checkNotNull(granularity);

    String[] annotations = null;
    if (point.getAnnotations() != null) {
      List<Map.Entry<String, String>> keyOrderedTags = point.getAnnotations().entrySet()
          .stream().sorted(Map.Entry.comparingByKey()).collect(Collectors.toList());
      annotations = new String[keyOrderedTags.size() * 2];
      for (int i = 0; i < keyOrderedTags.size(); ++i) {
        annotations[2 * i] = keyOrderedTags.get(i).getKey();
        annotations[(2 * i) + 1] = keyOrderedTags.get(i).getValue();
      }
    }

    return new HistogramKey(
        (byte) granularity.ordinal(),
        granularity.getBinId(point.getTimestamp()),
        point.getMetric(),
        point.getHost(),
        annotations
    );
  }

  /**
   * Creates a {@link ReportPoint} from a {@link HistogramKey} - {@link AgentDigest} pair
   *
   * @param histogramKey the key, defining metric, source, annotations, duration and start-time
   * @param agentDigest  the digest defining the centroids
   * @return the corresponding point
   */
  public static ReportPoint pointFromKeyAndDigest(HistogramKey histogramKey,
                                                  AgentDigest agentDigest) {
    return ReportPoint.newBuilder()
        .setTimestamp(histogramKey.getBinTimeMillis())
        .setMetric(histogramKey.getMetric())
        .setHost(histogramKey.getSource())
        .setAnnotations(histogramKey.getTagsAsMap())
        .setTable("dummy")
        .setValue(agentDigest.toHistogram((int) histogramKey.getBinDurationInMillis()))
        .build();
  }

  /**
   * Convert granularity to string. If null, we assume we are dealing with
   * "distribution" port.
   *
   * @param granularity granularity
   * @return string representation
   */
  public static String granularityToString(@Nullable Granularity granularity) {
    return granularity == null ? "distribution" : granularity.toString();
  }

  /**
   * Merges a histogram into a TDigest
   *
   * @param target target TDigest
   * @param source histogram to merge
   */
  public static void mergeHistogram(final TDigest target, final wavefront.report.Histogram source) {
    List<Double> means = source.getBins();
    List<Integer> counts = source.getCounts();

    if (means != null && counts != null) {
      int len = Math.min(means.size(), counts.size());

      for (int i = 0; i < len; ++i) {
        Integer count = counts.get(i);
        Double mean = means.get(i);

        if (count != null && count > 0 && mean != null && Double.isFinite(mean)) {
          target.add(mean, count);
        }
      }
    }
  }

  /**
   * (For now, a rather trivial) encoding of {@link HistogramKey} the form short length and bytes
   *
   * Consider using chronicle-values or making this stateful with a local
   * byte[] / Stringbuffers to be a little more efficient about encodings.
   */
  public static class HistogramKeyMarshaller implements BytesReader<HistogramKey>,
      BytesWriter<HistogramKey>, ReadResolvable<HistogramKeyMarshaller> {
    private static final HistogramKeyMarshaller INSTANCE = new HistogramKeyMarshaller();

    private static final Histogram accumulatorKeySizes =
        Metrics.newHistogram(new MetricName("histogram", "", "accumulatorKeySize"));

    private HistogramKeyMarshaller() {
      // Private Singleton
    }

    public static HistogramKeyMarshaller get() {
      return INSTANCE;
    }

    @Nonnull
    @Override
    public HistogramKeyMarshaller readResolve() {
      return INSTANCE;
    }

    private static void writeString(Bytes out, String s) {
      Preconditions.checkArgument(s == null || s.length() <= Short.MAX_VALUE,
          "String too long (more than 32K)");
      byte[] bytes = s == null ? new byte[0] : s.getBytes(StandardCharsets.UTF_8);
      out.writeShort((short) bytes.length);
      out.write(bytes);
    }

    private static String readString(Bytes in) {
      byte[] bytes = new byte[in.readShort()];
      in.read(bytes);
      return new String(bytes);
    }

    @Override
    public void readMarshallable(@Nonnull WireIn wire) throws IORuntimeException {
      // ignore, stateless
    }

    @Override
    public void writeMarshallable(@Nonnull WireOut wire) {
      // ignore, stateless
    }

    @Nonnull
    @Override
    public HistogramKey read(Bytes in, @Nullable HistogramKey using) {
      if (using == null) {
        using = new HistogramKey();
      }
      using.setGranularityOrdinal(in.readByte());
      using.setBinId(in.readInt());
      using.setMetric(readString(in));
      using.setSource(readString(in));
      int numTags = in.readShort();
      if (numTags > 0) {
        final String[] tags = new String[numTags];
        for (int i = 0; i < numTags; ++i) {
          tags[i] = readString(in);
        }
        using.setTags(tags);
      }
      return using;
    }

    @Override
    public void write(Bytes out, @Nonnull HistogramKey toWrite) {
      int accumulatorKeySize = 5;
      out.writeByte(toWrite.getGranularityOrdinal());
      out.writeInt(toWrite.getBinId());
      accumulatorKeySize += 2 + toWrite.getMetric().length();
      writeString(out, toWrite.getMetric());
      accumulatorKeySize += 2 + (toWrite.getSource() == null ? 0 : toWrite.getSource().length());
      writeString(out, toWrite.getSource());
      short numTags = toWrite.getTags() == null ? 0 : (short) toWrite.getTags().length;
      accumulatorKeySize += 2;
      out.writeShort(numTags);
      for (short i = 0; i < numTags; ++i) {
        final String tag = toWrite.getTags()[i];
        accumulatorKeySize += 2 + (tag == null ? 0 : tag.length());
        writeString(out, tag);
      }
      accumulatorKeySizes.update(accumulatorKeySize);
    }
  }
}
