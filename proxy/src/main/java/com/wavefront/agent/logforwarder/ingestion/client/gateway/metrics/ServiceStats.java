package com.wavefront.agent.logforwarder.ingestion.client.gateway.metrics;

import java.net.URI;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.Map;
import java.util.SortedMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.TimeUnit;

/**
 * @author Manoj Ramakrishnan (rmanoj@vmware.com).
 * @since 9/17/21 2:11 PM
 */
public class ServiceStats {
  public static final String STAT_NAME_SUFFIX_PER_DAY = "PerDay";
  public static final String STAT_NAME_SUFFIX_PER_HOUR = "PerHour";
  public Map<String, ServiceStat> entries = new HashMap();

  public ServiceStats() {
  }

  public static class ServiceStatLogHistogram {
    public long[] bins = new long[15];

    public ServiceStatLogHistogram() {
    }
  }

  public static class TimeSeriesStats {
    public SortedMap<Long, TimeBin> bins;
    public int numBins;
    public long binDurationMillis;
    public EnumSet<AggregationType> aggregationType;

    public TimeSeriesStats() {
    }

    public TimeSeriesStats(int numBins, long binDurationMillis, EnumSet<ServiceStats.TimeSeriesStats.AggregationType> aggregationType) {
      this.numBins = numBins;
      this.binDurationMillis = binDurationMillis;
      this.bins = new ConcurrentSkipListMap();
      this.aggregationType = aggregationType;
    }

    public void add(long timestampMicros, double value, double delta) {
      synchronized(this) {
        long binId = this.normalizeTimestamp(timestampMicros, this.binDurationMillis);
        ServiceStats.TimeSeriesStats.TimeBin dataBin = null;
        if (this.bins.containsKey(binId)) {
          dataBin = (ServiceStats.TimeSeriesStats.TimeBin)this.bins.get(binId);
        } else {
          if (this.bins.size() == this.numBins) {
            if ((Long)this.bins.firstKey() > timestampMicros) {
              return;
            }

            this.bins.remove(this.bins.firstKey());
          }

          dataBin = new ServiceStats.TimeSeriesStats.TimeBin();
          this.bins.put(binId, dataBin);
        }

        if (this.aggregationType.contains(ServiceStats.TimeSeriesStats.AggregationType.AVG)) {
          if (dataBin.avg == null) {
            dataBin.avg = value;
            dataBin.var = 0.0D;
            dataBin.count = 1.0D;
          } else {
            ++dataBin.count;
            double diff = value - dataBin.avg;
            dataBin.avg = dataBin.avg + diff / dataBin.count;
            double diffAfter = value - dataBin.avg;
            dataBin.var = dataBin.var + diff * diffAfter;
          }
        }

        if (this.aggregationType.contains(ServiceStats.TimeSeriesStats.AggregationType.SUM)) {
          if (dataBin.sum == null) {
            dataBin.sum = delta;
          } else {
            dataBin.sum = dataBin.sum + delta;
          }
        }

        if (this.aggregationType.contains(ServiceStats.TimeSeriesStats.AggregationType.MAX)) {
          if (dataBin.max == null) {
            dataBin.max = value;
          } else if (dataBin.max < value) {
            dataBin.max = value;
          }
        }

        if (this.aggregationType.contains(ServiceStats.TimeSeriesStats.AggregationType.MIN)) {
          if (dataBin.min == null) {
            dataBin.min = value;
          } else if (dataBin.min > value) {
            dataBin.min = value;
          }
        }

        if (this.aggregationType.contains(ServiceStats.TimeSeriesStats.AggregationType.LATEST)) {
          dataBin.latest = value;
        }

      }
    }

    private long normalizeTimestamp(long timestampMicros, long binDurationMillis) {
      long timeMillis = TimeUnit.MICROSECONDS.toMillis(timestampMicros);
      timeMillis -= timeMillis % binDurationMillis;
      return timeMillis;
    }

    public static class TimeBin {
      public Double avg;
      public Double var;
      public Double min;
      public Double max;
      public Double sum;
      public Double latest;
      public double count;

      public TimeBin() {
      }
    }

    public static enum AggregationType {
      AVG,
      MIN,
      MAX,
      SUM,
      LATEST;

      private AggregationType() {
      }
    }
  }

  public static class ServiceStat {
    public static final String FIELD_NAME_NAME = "name";
    public static final String FIELD_NAME_VERSION = "version";
    public static final String FIELD_NAME_LAST_UPDATE_TIME_MICROS_UTC = "lastUpdateMicrosUtc";
    public static final String FIELD_NAME_LATEST_VALUE = "latestValue";
    public static final String FIELD_NAME_UNIT = "unit";
    public String name;
    public double latestValue;
    public double accumulatedValue;
    public long version;
    public long lastUpdateMicrosUtc;
    public String unit;
    public Long sourceTimeMicrosUtc;
    public URI serviceReference;
    public ServiceStats.ServiceStatLogHistogram logHistogram;
    public ServiceStats.TimeSeriesStats timeSeriesStats;

    public ServiceStat() {
    }
  }
}
