package com.wavefront.agent.histogram;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;

import com.tdunning.math.stats.AgentDigest;
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

import org.apache.commons.lang.time.DateUtils;

import java.io.UnsupportedEncodingException;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;

import javax.annotation.Nullable;
import javax.validation.constraints.NotNull;

import wavefront.report.ReportPoint;

/**
 * Helpers around histograms
 *
 * @author Tim Schmidt (tim@wavefront.com).
 */
public final class Utils {
  private static final Logger logger = Logger.getLogger(Utils.class.getCanonicalName());

  private Utils() {
    // Not instantiable
  }

  /**
   * Standard supported aggregation Granularities.
   */
  public enum Granularity {
    MINUTE((int) DateUtils.MILLIS_PER_MINUTE),
    HOUR((int) DateUtils.MILLIS_PER_HOUR),
    DAY((int) DateUtils.MILLIS_PER_DAY);

    private final int inMillis;

    Granularity(int inMillis) {
      this.inMillis = inMillis;
    }

    /**
     * Duration of a corresponding bin in milliseconds.
     *
     * @return bin length in milliseconds
     */
    public int getInMillis() {
      return inMillis;
    }

    /**
     * Bin id for an epoch time is the epoch time in the corresponding granularity.
     *
     * @param timeMillis epoch time in milliseconds
     * @return the bin id
     */
    public int getBinId(long timeMillis) {
      return (int) (timeMillis / inMillis);
    }

    public static Granularity fromMillis(long millis) {
      if (millis <= 60 * 1000) {
        return MINUTE;
      } else if (millis <= 60 * 60 * 1000) {
        return HOUR;
      } else {
        return DAY;
      }
    }

    public static Granularity fromString(String granularityName) {
      if (granularityName.equals("minute")) {
        return MINUTE;
      }
      if (granularityName.equals("hour")) {
        return HOUR;
      }
      if (granularityName.equals("day")) {
        return DAY;
      }
      return null;
    }

    public static String granularityToString(@Nullable Granularity granularity) {
      if (granularity == null) {
        return "distribution";
      }
      switch (granularity) {
        case DAY:
          return "day";
        case HOUR:
          return "hour";
        case MINUTE:
          return "minute";
      }
      return "unknown";
    }
  }

  /**
   * Generates a {@link HistogramKey} according a prototype {@link ReportPoint} and {@link Granularity}.
   */
  public static HistogramKey makeKey(ReportPoint point, Granularity granularity) {
    Preconditions.checkNotNull(point);
    Preconditions.checkNotNull(granularity);

    String[] annotations = null;
    if (point.getAnnotations() != null) {
      List<Map.Entry<String, String>> keyOrderedTags = point.getAnnotations().entrySet()
          .stream().sorted(Comparator.comparing(Map.Entry::getKey)).collect(Collectors.toList());
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
  public static ReportPoint pointFromKeyAndDigest(HistogramKey histogramKey, AgentDigest agentDigest) {
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
   * Uniquely identifies a time-series - time-interval pair. These are the base sample aggregation scopes on the agent.
   */
  public static class HistogramKey {
    // NOTE: fields are not final to allow object reuse
    private byte granularityOrdinal;
    private int binId;
    private String metric;
    @Nullable
    private String source;
    @Nullable
    private String[] tags;


    private HistogramKey(byte granularityOrdinal, int binId, @NotNull String metric, @Nullable String source, @Nullable String[] tags) {
      this.granularityOrdinal = granularityOrdinal;
      this.binId = binId;
      this.metric = metric;
      this.source = source;
      this.tags = ((tags == null || tags.length == 0) ? null : tags);
    }

    private HistogramKey() {
      // For decoding
    }

    public byte getGranularityOrdinal() {
      return granularityOrdinal;
    }

    public int getBinId() {
      return binId;
    }

    public String getMetric() {
      return metric;
    }

    @Nullable
    public String getSource() {
      return source;
    }

    @Nullable
    public String[] getTags() {
      return tags;
    }

    @Override
    public String toString() {
      return "HistogramKey{" +
          "granularityOrdinal=" + granularityOrdinal +
          ", binId=" + binId +
          ", metric='" + metric + '\'' +
          ", source='" + source + '\'' +
          ", tags=" + Arrays.toString(tags) +
          '}';
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;

      HistogramKey histogramKey = (HistogramKey) o;

      if (granularityOrdinal != histogramKey.granularityOrdinal) return false;
      if (binId != histogramKey.binId) return false;
      if (!metric.equals(histogramKey.metric)) return false;
      if (source != null ? !source.equals(histogramKey.source) : histogramKey.source != null) return false;
      return Arrays.equals(tags, histogramKey.tags);

    }

    @Override
    public int hashCode() {
      int result = 1;
      result = 31 * result + (int) granularityOrdinal;
      result = 31 * result + binId;
      result = 31 * result + metric.hashCode();
      result = 31 * result + (source != null ? source.hashCode() : 0);
      result = 31 * result + Arrays.hashCode(tags);
      return result;
    }

    /**
     * Unpacks tags into a map.
     */
    public Map<String, String> getTagsAsMap() {
      if (tags == null || tags.length == 0) {
        return ImmutableMap.of();
      }

      Map<String, String> annotations = new HashMap<>(tags.length / 2);
      for (int i = 0; i < tags.length - 1; i += 2) {
        annotations.put(tags[i], tags[i + 1]);
      }

      return annotations;
    }

    public long getBinTimeMillis() {
      return getBinDurationInMillis() * binId;
    }

    public long getBinDurationInMillis() {
      return Granularity.values()[granularityOrdinal].getInMillis();
    }
  }

  /**
   * (For now, a rather trivial) encoding of {@link HistogramKey} the form short length and bytes
   *
   * Consider using chronicle-values or making this stateful with a local byte[]  / Stringbuffers to be a little more
   * efficient about encodings.
   */
  public static class HistogramKeyMarshaller implements BytesReader<HistogramKey>, BytesWriter<HistogramKey>, ReadResolvable<HistogramKeyMarshaller> {
    private static final HistogramKeyMarshaller INSTANCE = new HistogramKeyMarshaller();

    private static final Histogram accumulatorKeySizes =
        Metrics.newHistogram(new MetricName("histogram", "", "accumulatorKeySize"));

    private HistogramKeyMarshaller() {
      // Private Singleton
    }

    public static HistogramKeyMarshaller get() {
      return INSTANCE;
    }

    @Override
    public HistogramKeyMarshaller readResolve() {
      return INSTANCE;
    }

    private static void writeString(Bytes out, String s) {
      try {
        Preconditions.checkArgument(s == null || s.length() <= Short.MAX_VALUE, "String too long (more than 32K)");
        byte[] bytes = s == null ? new byte[0] : s.getBytes("UTF-8");
        out.writeShort((short) bytes.length);
        out.write(bytes);
      } catch (UnsupportedEncodingException e) {
        logger.log(Level.SEVERE, "Likely programmer error, String to Byte encoding failed: ", e);
        e.printStackTrace();
      }
    }

    private static String readString(Bytes in) {
      byte[] bytes = new byte[in.readShort()];
      in.read(bytes);
      return new String(bytes);
    }

    @Override
    public void readMarshallable(@NotNull WireIn wire) throws IORuntimeException {
      // ignore, stateless
    }

    @Override
    public void writeMarshallable(@NotNull WireOut wire) {
      // ignore, stateless
    }

    @NotNull
    @Override
    public HistogramKey read(Bytes in, @Nullable HistogramKey using) {
      if (using == null) {
        using = new HistogramKey();
      }
      using.granularityOrdinal = in.readByte();
      using.binId = in.readInt();
      using.metric = readString(in);
      using.source = readString(in);
      int numTags = in.readShort();
      if (numTags > 0) {
        using.tags = new String[numTags];
        for (int i = 0; i < numTags; ++i) {
          using.tags[i] = readString(in);
        }
      }
      return using;
    }

    @Override
    public void write(Bytes out, @NotNull HistogramKey toWrite) {
      int accumulatorKeySize = 5;
      out.writeByte(toWrite.granularityOrdinal);
      out.writeInt(toWrite.binId);
      accumulatorKeySize += 2 + toWrite.metric.length();
      writeString(out, toWrite.metric);
      accumulatorKeySize += 2 + (toWrite.source == null ? 0 : toWrite.source.length());
      writeString(out, toWrite.source);
      short numTags = toWrite.tags == null ? 0 : (short) toWrite.tags.length;
      accumulatorKeySize += 2;
      out.writeShort(numTags);
      for (short i = 0; i < numTags; ++i) {
        accumulatorKeySize += 2 + (toWrite.tags[i] == null ? 0 : toWrite.tags[i].length());
        writeString(out, toWrite.tags[i]);
      }
      accumulatorKeySizes.update(accumulatorKeySize);
    }
  }
}
