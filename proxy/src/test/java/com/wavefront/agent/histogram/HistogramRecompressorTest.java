package com.wavefront.agent.histogram;

import com.google.common.collect.ImmutableList;
import org.junit.Test;
import wavefront.report.Histogram;
import wavefront.report.HistogramType;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * @author vasily@wavefront.com
 */
public class HistogramRecompressorTest {

  @Test
  public void testHistogramRecompressor() {
    HistogramRecompressor recompressor = new HistogramRecompressor(() -> (short) 32);
    Histogram testHistogram = Histogram.newBuilder().
        setType(HistogramType.TDIGEST).
        setDuration(60000).
        setBins(ImmutableList.of(1.0, 2.0, 3.0)).
        setCounts(ImmutableList.of(3, 2, 1)).
        build();
    Histogram outputHistoram = recompressor.apply(testHistogram);
    // nothing to compress
    assertEquals(outputHistoram, testHistogram);

    testHistogram.setBins(ImmutableList.of(1.0, 1.0, 1.0, 2.0, 3.0, 3.0, 3.0));
    testHistogram.setCounts(ImmutableList.of(3, 1, 2, 2, 1, 2, 3));
    outputHistoram = recompressor.apply(testHistogram);
    // compacted histogram
    assertEquals(ImmutableList.of(1.0, 2.0, 3.0), outputHistoram.getBins());
    assertEquals(ImmutableList.of(6, 2, 6), outputHistoram.getCounts());

    List<Double> bins = new ArrayList<>();
    List<Integer> counts = new ArrayList<>();
    for (int i = 0; i < 1000; i++) {
      bins.add((double)i);
      counts.add(1);
    }
    testHistogram.setBins(bins);
    testHistogram.setCounts(counts);
    outputHistoram = recompressor.apply(testHistogram);
    assertTrue(outputHistoram.getBins().size() < 48);
    assertEquals(1000, outputHistoram.getCounts().stream().mapToInt(x -> x).sum());

    testHistogram.setBins(bins.subList(0, 65));
    testHistogram.setCounts(counts.subList(0, 65));
    outputHistoram = recompressor.apply(testHistogram);
    assertTrue(outputHistoram.getBins().size() < 48);
    assertEquals(65, outputHistoram.getCounts().stream().mapToInt(x -> x).sum());

    testHistogram.setBins(bins.subList(0, 64));
    testHistogram.setCounts(counts.subList(0, 64));
    outputHistoram = recompressor.apply(testHistogram);
    assertEquals(64, outputHistoram.getBins().size());
    assertEquals(64, outputHistoram.getCounts().stream().mapToInt(x -> x).sum());
  }
}