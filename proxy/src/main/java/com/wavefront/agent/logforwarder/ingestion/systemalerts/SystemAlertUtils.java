/*
 * Copyright (c) 2019 VMware, Inc. All Rights Reserved.
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

package com.wavefront.agent.logforwarder.ingestion.systemalerts;

import com.google.gson.reflect.TypeToken;

import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.vmware.lemans.client.gateway.GatewayClient;
import com.vmware.lemans.client.gateway.GatewayOperation;
import com.vmware.lemans.client.gateway.GatewayRequest;
import com.vmware.lemans.client.gateway.LemansClient;
import com.vmware.lemans.common.LemansUris;
import com.vmware.lint.commons.system.alerts.dto.SystemAlertDto;
import com.vmware.lint.commons.system.alerts.dto.SystemAlertOrgConfigsDto;
import com.vmware.log.forwarder.host.LogForwarderConfigProperties;
import com.vmware.log.forwarder.lemansclient.LemansClientState;
import com.vmware.log.forwarder.lemansclient.LogForwarderAgentHost;
import com.vmware.log.forwarder.utils.EndPointDestinationType;
import com.vmware.xenon.common.DeferredResult;
import com.vmware.xenon.common.Operation;
import com.vmware.xenon.common.Utils;
import com.wavefront.agent.logforwarder.ingestion.util.LogForwarderUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.invoke.MethodHandles;
import java.lang.reflect.Type;
import java.net.URI;
import java.util.List;

public class SystemAlertUtils {

    private static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    private static final String LEMANS_SELECTOR_NAME = "/lint-system-alert-config-service";

    public static MetricRegistry systemAlertMetricRegistry = new MetricRegistry();

    public static final String LOG_FORWARDER_MESSAGES_DROPPED_METRIC = "log-forwarder-messages-dropped-overall";

    public static final String LOG_FORWARDER_INGESTION_FAILURE_METRIC = "log-forwarder-ingestion-failed";

    public static final String SYSTEM_ALERTS_POSTED_METRIC = "system-alerts-posted";

    public static final String LOG_FORWARDING_BLOBS_FAILED_METRIC = "log-forwarding-blobs-failed";

    static final String SEPARATOR = "_";

    /**
     * get system alert org config from saas side via lemans route
     *
     * @return deferred result containing system alert config for org
     */
    public static DeferredResult<SystemAlertOrgConfigsDto> getSystemAlertOrgConfig() {
        if (!LogForwarderConfigProperties.logForwarderArgs.enableVertx) {
            LogForwarderAgentHost lemansClientHost = LemansClientState
                    .accessKeyVsLemansClientHost
                    .get(LogForwarderUtils.getLemansClientAccessKey());
            Operation getConfigOp = Operation
                    .createGet(lemansClientHost,
                            LemansUris.LEMANS_CLIENT_PREFIX + LEMANS_SELECTOR_NAME +
                                    "/lint/system-alert/config/internal")
                    .setReferer(lemansClientHost.getUri());
            return lemansClientHost.sendWithDeferredResult(getConfigOp, SystemAlertOrgConfigsDto.class);
        } else {
            LemansClient lemansClient = LemansClientState
                    .accessKeyVsLemansClient.get(LogForwarderUtils.getLemansClientAccessKey());
            GatewayClient gatewayClient = lemansClient.getGatewayClient();
            GatewayRequest gatewayRequest = GatewayRequest.createRequest(GatewayOperation.Action.GET,
                    URI.create(LEMANS_SELECTOR_NAME + "/lint/system-alert/config/internal"));
            DeferredResult<SystemAlertOrgConfigsDto> deferredResult = new DeferredResult<>();
            gatewayClient.sendRequest(gatewayRequest)
                    .whenComplete((resp, ex) -> {
                        if (ex != null) {
                            deferredResult.fail(ex);
                        } else {
                            SystemAlertOrgConfigsDto configsDto = resp.getBody(SystemAlertOrgConfigsDto.class);
                            deferredResult.complete(configsDto);
                        }

                    });
            return deferredResult;
        }
    }

    /**
     * get existing triggered alerts for reference id
     *
     * @return deferred result
     */
    public static DeferredResult<List<SystemAlertDto>> getExistingTriggeredSystemAlerts(String referenceId) {
        if (!LogForwarderConfigProperties.logForwarderArgs.enableVertx) {
            LogForwarderAgentHost lemansClientHost = LemansClientState
                    .accessKeyVsLemansClientHost
                    .get(LogForwarderUtils.getLemansClientAccessKey());
            Operation getConfigOp = Operation
                    .createGet(lemansClientHost,
                            LemansUris.LEMANS_CLIENT_PREFIX + LEMANS_SELECTOR_NAME +
                                    "/lint/system-alert/referenceId/" + referenceId + "/internal")
                    .setReferer(lemansClientHost.getUri());
            return lemansClientHost.sendWithDeferredResult(getConfigOp)
                    .thenApply(op -> SystemAlertUtils.stringToSystemAlertList(op.getBodyRaw().toString()));
        } else {
            LemansClient lemansClient = LemansClientState
                    .accessKeyVsLemansClient.get(LogForwarderUtils.getLemansClientAccessKey());
            GatewayClient gatewayClient = lemansClient.getGatewayClient();
            GatewayRequest gatewayRequest = GatewayRequest.createRequest(GatewayOperation.Action.GET,
                    URI.create(LEMANS_SELECTOR_NAME + "/lint/system-alert/referenceId/" + referenceId + "/internal"));
            DeferredResult<List<SystemAlertDto>> deferredResult = new DeferredResult<>();
            gatewayClient.sendRequest(gatewayRequest)
                    .whenComplete((resp, ex) -> {
                        if (ex != null) {
                            deferredResult.fail(ex);
                        } else {
                            Type type = new TypeToken<List<SystemAlertDto>>() {}.getType();
                            List<SystemAlertDto> alerts = Utils.fromJson(resp.getBody(), type);
                            deferredResult.complete(alerts);
                        }

                    });
            return deferredResult;
        }
    }

    /**
     * post system alert instances to lint saas
     *
     * @param systemAlertDtos system alert instances, not empty
     * @return                deferred result containing post alert instances operation
     */
    public static DeferredResult<List<SystemAlertDto>> fireSystemAlertsToLintSaas(
            List<SystemAlertDto> systemAlertDtos) {
        logger.debug("fire system alerts to lint-saas " + Utils.toJson(systemAlertDtos));
        if (!LogForwarderConfigProperties.logForwarderArgs.enableVertx) {
            LogForwarderAgentHost lemansClientHost = LemansClientState.accessKeyVsLemansClientHost.get(
                    LogForwarderUtils.getLemansClientAccessKey());

            Operation operation = Operation
                    .createPost(lemansClientHost, "/le-mans-client/streams/lint-system-alerts-stream")
                    .setBody(systemAlertDtos)
                    .addRequestHeader("Content-Type", "application/json")
                    .setReferer(lemansClientHost.getUri());

            return lemansClientHost.sendWithDeferredResult(operation)
                    .thenApply(r -> systemAlertDtos);
        } else {
            LemansClient lemansClient = LemansClientState
                    .accessKeyVsLemansClient.get(LogForwarderUtils.getLemansClientAccessKey());
            GatewayClient gatewayClient = lemansClient.getGatewayClient();
            GatewayRequest gatewayRequest = GatewayRequest.createRequest(GatewayOperation.Action.POST,
                    URI.create("/streams/lint-system-alerts-stream"))
                    .setBody(systemAlertDtos);
            DeferredResult<List<SystemAlertDto>> deferredResult = new DeferredResult<>();
            gatewayClient.sendRequest(gatewayRequest)
                    .whenComplete((resp, ex) -> {
                        if (ex != null) {
                            deferredResult.fail(ex);
                        } else {
                            deferredResult.complete(systemAlertDtos);
                        }

                    });
            return deferredResult;
        }
    }

    /**
     * update metric related to messages dropped
     *
     * @param messagesDroppedCount number of messages dropped
     */
    public static void updateMessagesDroppedMetric(int messagesDroppedCount) {
        getMessagesDroppedMeter().mark(messagesDroppedCount);
    }

    /**
     * meter that defines messages dropped
     *
     * @return messages dropped meter
     */
    public static Meter getMessagesDroppedMeter() {
        return systemAlertMetricRegistry.meter(LOG_FORWARDER_MESSAGES_DROPPED_METRIC);
    }

    /**
     * update metric  related to ingestion failure
     *
     * @param messagesFailedIngestion messages count
     */
    public static void updatePostToLintFailureMetric(int messagesFailedIngestion) {
        getPostToLintFailureMetric().mark(messagesFailedIngestion);
    }

    /**
     * meter that defines ingestion failure
     *
     * @return ingestion failure metric
     */
    public static Meter getPostToLintFailureMetric() {
        return systemAlertMetricRegistry.meter(LOG_FORWARDER_INGESTION_FAILURE_METRIC);
    }

    /**
     * meter that defines system alerts posted related data
     *
     * @return system alerts posted
     */
    public static Meter getSystemAlertsPostedMetric() {
        return systemAlertMetricRegistry.meter(SYSTEM_ALERTS_POSTED_METRIC);
    }

    public static void updateSystemAlertsPostedMetric(int systemAlertsPosted) {
        getSystemAlertsPostedMetric().mark(systemAlertsPosted);
    }

    /**
     *  get meter for log-forwarding failed metric
     *
     * @param logForwardingConfigId log forwarding config id, not null
     * @return                      log-forwarding failed meter
     */
    public static Meter getLogForwardingFailedMetric(String logForwardingConfigId) {
        return systemAlertMetricRegistry
                .meter(LOG_FORWARDING_BLOBS_FAILED_METRIC + SEPARATOR + logForwardingConfigId);
    }

    /**
     * update post to end point failed metric
     *
     * @param endPointType          end point type, not null
     * @param endpointIdentifier    end point identifier, not null
     * @param msgsFailedToPost      messages failed to post, (not used for now)
     */
    public static void updatePostToEndPointFailed(EndPointDestinationType endPointType,
                                                  String endpointIdentifier,
                                                  int msgsFailedToPost) {
        if (EndPointDestinationType.EVENT_FORWARDING_ENDPOINT == endPointType) {
            getLogForwardingFailedMetric(endpointIdentifier).mark();
        }
    }

    /**
     * get log forwarding id from metric name
     *
     * assumption : metric belongs to log-forwarding related system alert metric
     *
     * @param metricName metric name, not null
     * @return           log forwarding id
     */
    public static String getLogForwardingIdFromMetricName(String metricName) {
        return  metricName.split(SEPARATOR)[1];
    }

    /**
     * convert json into list of system alert dto
     *
     * @param json system alert dtos json, valid one
     * @return     list of system alert dto
     */
    public static List<SystemAlertDto> stringToSystemAlertList(String json) {
        Type type = new TypeToken<List<SystemAlertDto>>() {

        }.getType();

        return Utils.fromJson(json, type);
    }

}
