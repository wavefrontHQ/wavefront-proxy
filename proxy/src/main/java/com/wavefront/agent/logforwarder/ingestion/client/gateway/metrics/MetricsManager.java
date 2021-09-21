/*
 * Copyright (c) 2017 VMware, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License.  You may obtain a copy of
 * the License at http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, without warranties or
 * conditions of any kind, EITHER EXPRESS OR IMPLIED.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package com.wavefront.agent.logforwarder.ingestion.client.gateway.metrics;


import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;
import io.dropwizard.metrics5.ConsoleReporter;
import io.dropwizard.metrics5.Histogram;
import io.dropwizard.metrics5.Meter;
import io.dropwizard.metrics5.MetricRegistry;
import io.dropwizard.metrics5.Timer;

public class MetricsManager {

    public static final Pattern CHILD_PATTERN = Pattern.compile("^/le-mans/v1/(?<factory>\\w+)/(?<child>[a-zA-Z_0-9-]+)/?.*");
    public static final String STATUS_TOKEN = "STATUS";
    public static final String SUCCESS = "success";
    public static final String FAILED = "failed";
    public static final String RETRY = "retry";
    public static final String TOTAL = "total";
    public static final String TIMER = "timer";
    public static final String BODY_SIZE = "body_size";
    public static final String METRICS_PREFIX = "lemans.";
    public static final String METRICS_RESOURCES_PREFIX = "lemans.resources.";
    public static final String METRICS_GATEWAY_PREFIX = "lemans.gateway.";
    public static final String METRICS_RECEIVERS_PREFIX = "lemans.gateway.receivers.";
    public static final String METRICS_CLIENT_PREFIX = "lemans.client.";
    public static final String DEFAULT_TAG = "null";
    public static final String AGENT_TAG = "[agent:%s]";
    public static final String PROPERTY_NAME_WAVEFRONT_PROXY = "lemans.wavefront.proxy";
    public static final String PROPERTY_NAME_WAVEFRONT_TAGS = "lemans.wavefront.tags";
    public static final String WAVEFRONT_METRICS_PREFIX = "wf-reporter";
    public static final String WAVEFRONT_VERTX_METRICS_PREFIX = "wf-reporter-vertx";
    public static final String CACHE_METRICS_PATCH_PREFIX_FORMAT = "lemans.gateway.caches.%s.patch.";
    public static final String CACHE_METRICS_COUNT_PREFIX_FORMAT = "lemans.gateway.caches.%s.count";
    public static final String PROPERTY_NAME_GATEWAY_PROMETHEUS_METRICS_URI = "lemans.gateway.prometheus.metrics.uri";
    private final Map<String, MetricsManager.MetricDetails> supportedApisMap;
    private final ConcurrentMap<String, String> metricsStatusNameMap = new ConcurrentHashMap();
    public static final String PROPERTY_NAME_LOG_OPTION = "lemans.MetricsManager.logOption";
    public final MetricRegistry metricRegistry = new MetricRegistry();
    protected final ConcurrentMap<String, Meter> metricsMaps = new ConcurrentHashMap();
    protected final ConcurrentMap<String, Timer> timersMap = new ConcurrentHashMap();
    protected final ConcurrentMap<String, Histogram> histrogramsMap = new ConcurrentHashMap();
    private final MetricsManager.LogOption logOption;

    public MetricsManager(Map<String, MetricsManager.MetricDetails> supportedApisMap) {
        this.supportedApisMap = supportedApisMap;
        List<String> statusValues = Arrays.asList("success", "failed", "total", "timer");
        this.supportedApisMap.values().forEach((metricDetails) -> {
            statusValues.forEach((status) -> {
                String var10000 = (String)this.metricsStatusNameMap.put(status + metricDetails.nameFormat, String.format(metricDetails.nameFormat, "null").replaceAll("STATUS", status));
            });
        });
        String logOptionString = System.getProperty("lemans.MetricsManager.logOption", MetricsManager.LogOption.INFO.toString());

        MetricsManager.LogOption logOption;
        try {
            logOption = MetricsManager.LogOption.valueOf(logOptionString);
        } catch (IllegalArgumentException var6) {
            logOption = MetricsManager.LogOption.INFO;
        }

        this.logOption = logOption;
    }

    public void startConsoleReporter(int intervalMinutes) {
        ConsoleReporter console = ConsoleReporter.forRegistry(this.metricRegistry).convertRatesTo(TimeUnit.SECONDS).convertDurationsTo(TimeUnit.MILLISECONDS).build();
        console.start((long)intervalMinutes, TimeUnit.MINUTES);
    }

    public static enum LogOption {
        NONE,
        INFO,
        DEBUG;

        private LogOption() {
        }
    }

    public static class MetricDetails {
        public String nameFormat;
        public boolean discoverChildren;
        public boolean trackBodySize;
        public String metricName;
        public String tagName;
        public String tagValue;
        public List<String> actions;

        protected MetricDetails() {
            this.actions = new ArrayList();
        }

        public MetricDetails(MetricsManager.MetricDetails copy, String metricName) {
            this(copy, metricName, copy.tagValue);
        }

        public MetricDetails(MetricsManager.MetricDetails copy, String metricName, String tagValue) {
            this.actions = new ArrayList();
            this.nameFormat = copy.nameFormat;
            this.discoverChildren = copy.discoverChildren;
            this.trackBodySize = copy.trackBodySize;
            this.tagName = copy.tagName;
            this.tagValue = tagValue;
            this.metricName = metricName;
            this.actions = new ArrayList(copy.actions);
        }

        public String getMetricName() {
            return this.metricName != null ? this.metricName : this.nameFormat;
        }

        public String getTagValue() {
            return this.tagValue != null ? this.tagValue : "null";
        }

        public static class Builder {
            MetricsManager.MetricDetails metricDetails = new MetricsManager.MetricDetails();

            protected Builder() {
            }

            public static MetricsManager.MetricDetails.Builder create() {
                return new MetricsManager.MetricDetails.Builder();
            }

            public MetricsManager.MetricDetails.Builder withNameFormat(String nameFormat) {
                this.metricDetails.nameFormat = nameFormat;
                return this;
            }

            public MetricsManager.MetricDetails.Builder withDiscoverChildren(boolean discoverChildren) {
                this.metricDetails.discoverChildren = discoverChildren;
                return this;
            }

            public MetricsManager.MetricDetails.Builder withTrackBodySize(boolean trackBodySize) {
                this.metricDetails.trackBodySize = trackBodySize;
                return this;
            }

            public MetricsManager.MetricDetails.Builder withActions(String... actions) {
                this.metricDetails.actions.addAll(Arrays.asList(actions));
                return this;
            }

            public MetricsManager.MetricDetails.Builder withTagName(String tagName) {
                this.metricDetails.tagName = tagName;
                return this;
            }

            public MetricsManager.MetricDetails build() {
                return this.metricDetails;
            }
        }
    }
}
