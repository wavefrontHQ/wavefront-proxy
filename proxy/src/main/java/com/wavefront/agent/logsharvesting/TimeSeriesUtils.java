package com.wavefront.agent.logsharvesting;

import com.yammer.metrics.core.DeltaCounter;
import com.yammer.metrics.core.MetricName;

import org.apache.avro.io.DatumReader;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.specific.SpecificDatumReader;

import java.io.IOException;

import wavefront.report.TimeSeries;

/**
 * @author Mori Bellamy (mori@wavefront.com)
 */
public class TimeSeriesUtils {

  private static DatumReader<TimeSeries> datumReader = new SpecificDatumReader<>(TimeSeries.class);

  public static TimeSeries fromMetricName(MetricName metricName) throws IOException {
    String name = metricName.getName();
    // check if it is a delta counter, then remove the delta prefix before parsing
    // (delta prefix had to be removed, because the name contains json and jackson parser fails
    // to parse the delta prefix)
    boolean deltaCounter = DeltaCounter.isDelta(name);
    if (deltaCounter) {
      name = DeltaCounter.getNameWithoutDeltaPrefix(name);
    }

    Decoder decoder = DecoderFactory.get().jsonDecoder(TimeSeries.SCHEMA$, name);
    TimeSeries toReturn = datumReader.read(null, decoder);

    // add the delta prefix back
    if (deltaCounter) {
      String newName = DeltaCounter.getDeltaCounterName(toReturn.getMetric());
      toReturn.setMetric(newName);
    }
    return toReturn;
  }

  public static MetricName toMetricName(TimeSeries timeSeries) {
    return new MetricName("group", "type", timeSeries.toString());
  }

}
