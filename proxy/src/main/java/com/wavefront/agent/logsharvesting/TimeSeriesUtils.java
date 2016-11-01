package com.wavefront.agent.logsharvesting;

import com.yammer.metrics.core.MetricName;

import org.apache.avro.io.DatumReader;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.specific.SpecificDatumReader;

import java.io.IOException;

import sunnylabs.report.TimeSeries;

/**
 * @author Mori Bellamy (mori@wavefront.com)
 */
public class TimeSeriesUtils {

  private static DatumReader<TimeSeries> datumReader = new SpecificDatumReader<>(TimeSeries.class);

  public static TimeSeries fromMetricName(MetricName metricName) throws IOException {
    Decoder decoder = DecoderFactory.get().jsonDecoder(TimeSeries.SCHEMA$, metricName.getName());
    return datumReader.read(null, decoder);
  }

  public static MetricName toMetricName(TimeSeries timeSeries) {
    return new MetricName("group", "type", timeSeries.toString());
  }

}
