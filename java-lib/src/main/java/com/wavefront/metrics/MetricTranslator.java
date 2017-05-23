package com.wavefront.metrics;

import com.wavefront.common.Pair;
import com.yammer.metrics.core.Metric;
import com.yammer.metrics.core.MetricName;

import java.util.function.Function;

/**
 * @author Mori Bellamy (mori@wavefront.com)
 */
public interface MetricTranslator extends Function<Pair<MetricName, Metric>, Pair<MetricName, Metric>> {
}
