package com.tdunning.math.stats;

import com.google.common.base.Preconditions;

import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.MetricName;

import net.jafama.FastMath;
import net.openhft.chronicle.bytes.Bytes;
import net.openhft.chronicle.core.io.IORuntimeException;
import net.openhft.chronicle.core.util.ReadResolvable;
import net.openhft.chronicle.hash.serialization.SizedReader;
import net.openhft.chronicle.hash.serialization.SizedWriter;
import net.openhft.chronicle.wire.WireIn;
import net.openhft.chronicle.wire.WireOut;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import wavefront.report.Histogram;
import wavefront.report.HistogramType;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * NOTE: This is a pruned and modified version of {@link MergingDigest}. It does not support queries (cdf/quantiles) or
 * the traditional encodings.
 * <p/>
 * Maintains a t-digest by collecting new points in a buffer that is then sorted occasionally and merged into a sorted
 * array that contains previously computed centroids.
 * <p/>
 * This can be very fast because the cost of sorting and merging is amortized over several insertion. If we keep N
 * centroids total and have the input array is k long, then the amortized cost is something like
 * <p/>
 * N/k + log k
 * <p/>
 * These costs even out when N/k = log k.  Balancing costs is often a good place to start in optimizing an algorithm.
 * For different values of compression factor, the following table shows estimated asymptotic values of N and suggested
 * values of k: <table> <thead> <tr><td>Compression</td><td>N</td><td>k</td></tr> </thead> <tbody>
 * <tr><td>50</td><td>78</td><td>25</td></tr> <tr><td>100</td><td>157</td><td>42</td></tr>
 * <tr><td>200</td><td>314</td><td>73</td></tr> </tbody> </table>
 * <p/>
 * The virtues of this kind of t-digest implementation include: <ul> <li>No allocation is required after
 * initialization</li> <li>The data structure automatically compresses existing centroids when possible</li> <li>No Java
 * object overhead is incurred for centroids since data is kept in primitive arrays</li> </ul>
 * <p/>
 * The current implementation takes the liberty of using ping-pong buffers for implementing the merge resulting in a
 * substantial memory penalty, but the complexity of an in place merge was not considered as worthwhile since even with
 * the overhead, the memory cost is less than 40 bytes per centroid which is much less than half what the AVLTreeDigest
 * uses.  Speed tests are still not complete so it is uncertain whether the merge strategy is faster than the tree
 * strategy.
 */
public class AgentDigest extends AbstractTDigest {

  private final short compression;
  // points to the centroid that is currently being merged
  // if weight[lastUsedCell] == 0, then this is the number of centroids
  // else the number is lastUsedCell+1
  private int lastUsedCell;

  // sum_i weight[i]  See also unmergedWeight
  private double totalWeight = 0;

  // number of points that have been added to each merged centroid
  private double[] weight;
  // mean of points added to each merged centroid
  private double[] mean;

  // history of all data added to centroids (for testing purposes)
  private List<List<Double>> data = null;

  // buffers for merging
  private double[] mergeWeight;
  private double[] mergeMean;
  private List<List<Double>> mergeData = null;

  // sum_i tempWeight[i]
  private double unmergedWeight = 0;

  // this is the index of the next temporary centroid
  // this is a more Java-like convention than lastUsedCell uses
  private int tempUsed = 0;
  private final double[] tempWeight;
  private final double[] tempMean;
  private List<List<Double>> tempData = null;

  // array used for sorting the temp centroids.  This is a field
  // to avoid allocations during operation
  private final int[] order;

  private long dispatchTimeMillis;

  // should only need ceiling(compression * PI / 2).  Double the allocation for now for safety
  private static int defaultSizeForCompression(short compression) {
    return (int) (Math.PI * compression + 0.5);
  }

  // magic formula created by regressing against known sizes for sample compression values
  private static int bufferSizeForCompression(short compression) {
    return (int) (7.5 + 0.37 * compression - 2e-4 * compression * compression);
  }

  public AgentDigest(short compression, long dispatchTimeMillis) {
    Preconditions.checkArgument(compression >= 20D);
    Preconditions.checkArgument(compression <= 1000D);

    int numCentroids = defaultSizeForCompression(compression);
    int numBuffered = bufferSizeForCompression(compression);

    this.compression = compression;
    weight = new double[numCentroids];
    mean = new double[numCentroids];
    mergeWeight = new double[numCentroids];
    mergeMean = new double[numCentroids];
    tempWeight = new double[numBuffered];
    tempMean = new double[numBuffered];
    order = new int[numBuffered];

    lastUsedCell = 0;
    this.dispatchTimeMillis = dispatchTimeMillis;
  }

  /**
   * Turns on internal data recording.
   */
  @Override
  public TDigest recordAllData() {
    super.recordAllData();
    data = new ArrayList<>();
    mergeData = new ArrayList<>();
    return this;
  }

  @Override
  void add(double x, int w, Centroid base) {
    add(x, w, base.data());
  }

  @Override
  public void add(double x, int w) {
    add(x, w, (List<Double>) null);
  }

  @Override
  public void add(List<? extends TDigest> others) {
    for (TDigest other : others) {
      setMinMax(Math.min(min, other.getMin()), Math.max(max, other.getMax()));
      for (Centroid centroid : other.centroids()) {
        add(centroid.mean(), centroid.count(), recordAllData ? centroid.data() : null);
      }
    }
  }

  public void add(double x, int w, List<Double> history) {
    if (Double.isNaN(x)) {
      throw new IllegalArgumentException("Cannot add NaN to t-digest");
    }
    if (tempUsed >= tempWeight.length) {
      mergeNewValues();
    }
    int where = tempUsed++;
    tempWeight[where] = w;
    tempMean[where] = x;
    unmergedWeight += w;

    if (data != null) {
      if (tempData == null) {
        tempData = new ArrayList<>();
      }
      while (tempData.size() <= where) {
        tempData.add(new ArrayList<>());
      }
      if (history == null) {
        history = Collections.singletonList(x);
      }
      tempData.get(where).addAll(history);
    }
  }

  private void mergeNewValues() {
    if (unmergedWeight > 0) {
      Sort.sort(order, tempMean, tempUsed);

      double wSoFar = 0;
      double k1 = 0;
      int i = 0;
      int j = 0;
      int n = 0;
      if (totalWeight > 0) {
        if (weight[lastUsedCell] > 0) {
          n = lastUsedCell + 1;
        } else {
          n = lastUsedCell;
        }
      }
      lastUsedCell = 0;
      totalWeight += unmergedWeight;
      unmergedWeight = 0;

      // merge tempWeight,tempMean and weight,mean into mergeWeight,mergeMean
      while (i < tempUsed && j < n) {
        int ix = order[i];
        if (tempMean[ix] <= mean[j]) {
          wSoFar += tempWeight[ix];
          k1 = mergeCentroid(wSoFar, k1, tempWeight[ix], tempMean[ix], tempData != null ? tempData.get(ix) : null);
          i++;
        } else {
          wSoFar += weight[j];
          k1 = mergeCentroid(wSoFar, k1, weight[j], mean[j], data != null ? data.get(j) : null);
          j++;
        }
      }

      while (i < tempUsed) {
        int ix = order[i];
        wSoFar += tempWeight[ix];
        k1 = mergeCentroid(wSoFar, k1, tempWeight[ix], tempMean[ix], tempData != null ? tempData.get(ix) : null);
        i++;
      }

      while (j < n) {
        wSoFar += weight[j];
        k1 = mergeCentroid(wSoFar, k1, weight[j], mean[j], data != null ? data.get(j) : null);
        j++;
      }
      tempUsed = 0;

      // swap pointers for working space and merge space
      double[] z = weight;
      weight = mergeWeight;
      mergeWeight = z;
      Arrays.fill(mergeWeight, 0);

      z = mean;
      mean = mergeMean;
      mergeMean = z;

      if (data != null) {
        data = mergeData;
        mergeData = new ArrayList<>();
        tempData = new ArrayList<>();
      }
    }
  }

  private double mergeCentroid(double wSoFar, double k1, double w, double m, List<Double> newData) {
    double k2 = integratedLocation(wSoFar / totalWeight);
    if (k2 - k1 <= 1 || mergeWeight[lastUsedCell] == 0) {
      // merge into existing centroid
      mergeWeight[lastUsedCell] += w;
      mergeMean[lastUsedCell] = mergeMean[lastUsedCell] + (m - mergeMean[lastUsedCell]) * w / mergeWeight[lastUsedCell];
    } else {
      // create new centroid
      lastUsedCell++;
      mergeMean[lastUsedCell] = m;
      mergeWeight[lastUsedCell] = w;

      k1 = integratedLocation((wSoFar - w) / totalWeight);
    }
    if (mergeData != null) {
      while (mergeData.size() <= lastUsedCell) {
        mergeData.add(new ArrayList<>());
      }
      mergeData.get(lastUsedCell).addAll(newData);
    }
    return k1;
  }

  /**
   * Exposed for testing.
   */
  int checkWeights() {
    return checkWeights(weight, totalWeight, lastUsedCell);
  }

  private int checkWeights(double[] w, double total, int last) {
    int badCount = 0;

    int n = last;
    if (w[n] > 0) {
      n++;
    }

    double k1 = 0;
    double q = 0;
    for (int i = 0; i < n; i++) {
      double dq = w[i] / total;
      double k2 = integratedLocation(q + dq);
      if (k2 - k1 > 1 && w[i] != 1) {
        System.out.printf("Oversize centroid at %d, k0=%.2f, k1=%.2f, dk=%.2f, w=%.2f, q=%.4f\n", i, k1, k2, k2 - k1, w[i], q);
        badCount++;
      }
      if (k2 - k1 > 1.5 && w[i] != 1) {
        throw new IllegalStateException(String.format("Egregiously oversized centroid at %d, k0=%.2f, k1=%.2f, dk=%.2f, w=%.2f, q=%.4f\n", i, k1, k2, k2 - k1, w[i], q));
      }
      q += dq;
      k1 = k2;
    }

    return badCount;
  }

  /**
   * Converts a quantile into a centroid scale value.  The centroid scale is nominally
   * the number k of the centroid that a quantile point q should belong to.  Due to
   * round-offs, however, we can't align things perfectly without splitting points
   * and centroids.  We don't want to do that, so we have to allow for offsets.
   * In the end, the criterion is that any quantile range that spans a centroid
   * scale range more than one should be split across more than one centroid if
   * possible.  This won't be possible if the quantile range refers to a single point
   * or an already existing centroid.
   * <p/>
   * This mapping is steep near q=0 or q=1 so each centroid there will correspond to
   * less q range.  Near q=0.5, the mapping is flatter so that centroids there will
   * represent a larger chunk of quantiles.
   *
   * @param q The quantile scale value to be mapped.
   * @return The centroid scale value corresponding to q.
   */
  private double integratedLocation(double q) {
    return compression * (FastMath.asin(2 * q - 1) + Math.PI / 2) / Math.PI;
  }

  @Override
  public void compress() {
    mergeNewValues();
  }

  @Override
  public long size() {
    return (long) (totalWeight + unmergedWeight);
  }

  @Override
  public double cdf(double x) {
    // Not supported
    return Double.NaN;
  }

  @Override
  public double quantile(double q) {
    return Double.NaN;
  }

  /**
   * Not clear to me that this is a good idea, maybe just add the temp points and existing centroids rather then merging
   * first?
   */
  @Override
  public Collection<Centroid> centroids() {
    // we don't actually keep centroid structures around so we have to fake it
    List<Centroid> r = new ArrayList<>();
    int count = centroidCount();
    for (int i = 0; i < count; i++) {
      r.add(new Centroid(mean[i], (int) weight[i], data != null ? data.get(i) : null));
    }
    return r;
  }

  @Override
  public double compression() {
    return compression;
  }

  @Override
  public int byteSize() {
    return 0;
  }

  @Override
  public int smallByteSize() {
    return 0;
  }


  /**
   * Number of centroids of this AgentDigest (does compress if necessary)
   */
  public int centroidCount() {
    mergeNewValues();
    return lastUsedCell + (weight[lastUsedCell] == 0 ? 0 : 1);
  }

  /**
   * Creates a reporting Histogram from this AgentDigest (marked with the supplied duration).
   */
  public Histogram toHistogram(int duration) {
    int numCentroids = centroidCount();
    // NOTE: now merged as a side-effect

    List<Double> means = new ArrayList<>(centroidCount());
    List<Integer> count = new ArrayList<>(centroidCount());

    for (int i = 0; i < numCentroids; ++i) {
      means.add(mean[i]);
      count.add((int) Math.round(weight[i]));
    }

    return Histogram.newBuilder()
        .setDuration(duration)
        .setBins(means)
        .setCounts(count)
        .setType(HistogramType.TDIGEST)
        .build();
  }

  /**
   * Comprises of the dispatch-time (8 bytes) + compression (2 bytes)
   */
  private static final int FIXED_SIZE = 8 + 2;
  /**
   * Weight, mean float pair
   */
  private static final int PER_CENTROID_SIZE = 8;

  private int encodedSize() {
    return FIXED_SIZE + centroidCount() * PER_CENTROID_SIZE;
  }

  /**
   * Stateless AgentDigest codec for chronicle maps
   */
  public static class AgentDigestMarshaller implements SizedReader<AgentDigest>,
      SizedWriter<AgentDigest>, ReadResolvable<AgentDigestMarshaller> {
    private static final AgentDigestMarshaller INSTANCE = new AgentDigestMarshaller();
    private static final com.yammer.metrics.core.Histogram accumulatorValueSizes =
        Metrics.newHistogram(new MetricName("histogram", "", "accumulatorValueSize"));


    private AgentDigestMarshaller() {
    }

    public static AgentDigestMarshaller get() {
      return INSTANCE;
    }

    @Nonnull
    @Override
    public AgentDigest read(Bytes in, long size, @Nullable AgentDigest using) {
      Preconditions.checkArgument(size >= FIXED_SIZE);
      short compression = in.readShort();

      if (using == null || using.compression != compression) {
        using = new AgentDigest(compression, in.readLong());
      } else {
        using.dispatchTimeMillis = in.readLong();
      }
      using.totalWeight = 0d;
      using.lastUsedCell = (int) ((size - FIXED_SIZE) / PER_CENTROID_SIZE);
      using.tempUsed = 0;
      using.unmergedWeight = 0D;

      // need explicit nulling of weight past lastUsedCell
      Arrays.fill(using.weight, using.lastUsedCell, using.weight.length, 0D);

      for (int i = 0; i < using.lastUsedCell; ++i) {
        float weight = in.readFloat();
        using.weight[i] = weight;
        using.mean[i] = in.readFloat();
        using.totalWeight += weight;
      }

      return using;
    }

    @Override
    public long size(@Nonnull AgentDigest toWrite) {
      long size = toWrite.encodedSize();
      accumulatorValueSizes.update(size);
      return size;
    }

    @Override
    public void write(Bytes out, long size, @Nonnull AgentDigest toWrite) {
      // Merge in all buffered values
      int numCentroids = toWrite.centroidCount();

      // Just for sanity, comment out for production use
      Preconditions.checkArgument(size == toWrite.encodedSize());

      // Write compression
      out.writeShort(toWrite.compression);

      // Time
      out.writeLong(toWrite.dispatchTimeMillis);

      // Centroids
      for (int i = 0; i < numCentroids; ++i) {
        out.writeFloat((float) toWrite.weight[i]);
        out.writeFloat((float) toWrite.mean[i]);
      }
    }

    @Nonnull
    @Override
    public AgentDigestMarshaller readResolve() {
      return INSTANCE;
    }

    @Override
    public void readMarshallable(@Nonnull WireIn wire) throws IORuntimeException {
      // ignore
    }

    @Override
    public void writeMarshallable(@Nonnull WireOut wire) {
      // ignore
    }
  }

  @Override
  public void asBytes(ByteBuffer buf) {
    // Ignore
  }

  @Override
  public void asSmallBytes(ByteBuffer buf) {
    // Ignore
  }

  /**
   * Time at which this digest should be dispatched to wavefront.
   */
  public long getDispatchTimeMillis() {
    return dispatchTimeMillis;
  }
}
