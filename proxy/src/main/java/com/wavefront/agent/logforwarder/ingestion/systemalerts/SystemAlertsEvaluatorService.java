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

import com.google.common.collect.Maps;

import com.vmware.lint.commons.system.alerts.dto.SystemAlertConfigDto;
import com.vmware.lint.commons.system.alerts.dto.SystemAlertDto;
import com.vmware.lint.commons.system.alerts.dto.SystemAlertOrgConfigsDto;
import com.vmware.lint.commons.system.alerts.enums.CustomPropertyKey;
import com.vmware.lint.commons.system.alerts.enums.SystemAlertStatus;
import com.vmware.lint.commons.system.alerts.enums.SystemAlertType;
import com.vmware.log.forwarder.generalsettings.GeneralSettingsService;
import com.vmware.log.forwarder.generalsettings.GeneralSettingsState;
import com.vmware.xenon.common.DeferredResult;
import com.vmware.xenon.common.Utils;
import com.wavefront.agent.logforwarder.config.LogForwarderConfigProperties;
import com.wavefront.agent.logforwarder.ingestion.util.LogForwarderUtils;

import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

/**
 * TODO FILL WHAT IT DOES
 */
public class SystemAlertsEvaluatorService {

    private static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    private final long SYSTEM_ALERTS_EVALUATION_TASK_INITIAL_DELAY_MILLIS = TimeUnit.MINUTES.toMillis(1);

    private final long SYSTEM_ALERTS_EVALUATION_TASK_INTERVAL_MILLIS = TimeUnit.MINUTES.toMillis(3);

    private final long SYSTEM_ALERTS_CONFIG_TASK_INITIAL_DELAY_MILLIS = TimeUnit.SECONDS.toMillis(10);

    private final long SYSTEM_ALERTS_CONFIG_TASK_INTERVAL_MILLIS = TimeUnit.MINUTES.toMillis(10);

    private SystemAlertStatus logsGettingDroppedAlertStatus = SystemAlertStatus.RESOLVED;

    private SystemAlertStatus ingestionFailureAlertStatus = SystemAlertStatus.RESOLVED;

    /** key=log-forwarding config id, value=system alert status */
    private Map<String, SystemAlertStatus> logForwardingIdVsAlertState = new ConcurrentHashMap<>();

    private TimerTask alertsEvaluatorTimerTask = null;

    private TimerTask alertsConfigTimerTask = null;

    private volatile SystemAlertOrgConfigsDto systemAlertOrgConfig;

    /**
     * 1. create a timer task that executes the method to trigger/resolve system alerts
     * 2. create a timer task that executes the method to get system alert config
     */
    public void scheduleAlertEvaluator() {
        alertsEvaluatorTimerTask = new TimerTask() {
            @Override
            public void run() {
                try {
                    getConfigAndEvaluateSystemAlerts();
                } catch (Exception e) {
                    logger.error("Error in system alerts evaluation workflow", e);
                }
            }
        };
        alertsConfigTimerTask = new TimerTask() {
            @Override
            public void run() {
                try {
                    refreshAlertsConfigAndEvaluate();
                } catch (Exception e) {
                    logger.error("Error in get system alerts config workflow", e);
                }
            }
        };
        Timer alertsTimer = new Timer();
        alertsTimer.scheduleAtFixedRate(alertsEvaluatorTimerTask,
                SYSTEM_ALERTS_EVALUATION_TASK_INITIAL_DELAY_MILLIS,
                SYSTEM_ALERTS_EVALUATION_TASK_INTERVAL_MILLIS);
        alertsTimer.scheduleAtFixedRate(alertsConfigTimerTask,
                SYSTEM_ALERTS_CONFIG_TASK_INITIAL_DELAY_MILLIS,
                SYSTEM_ALERTS_CONFIG_TASK_INTERVAL_MILLIS);
    }

    /**
     * if the system alert config is not present in memory, get the config and evaluate alerts
     * else, evaluate alerts
     */
    void getConfigAndEvaluateSystemAlerts() {
        if (this.systemAlertOrgConfig != null) {
            syncAlertStatusAndEvaluateAlerts();
        } else {
            refreshAlertsConfigAndEvaluate();
        }
    }

    /**
     * get system alerts config from saas side, sync alert status with saas and evaluate alerts
     */
    private void refreshAlertsConfigAndEvaluate() {
        SystemAlertUtils
                .getSystemAlertOrgConfig()
                .thenAccept(alertOrgConfigDto -> {
                    if (alertOrgConfigDto.getCspOrgId() != null) {
                        /* remove the configs if alert-type is null,
                            this will handle the case where a new alert-type got added on the saas side.
                            the log-forwarder needs to ignore the alert-type
                         */
                        alertOrgConfigDto
                                .getConfigs()
                                .removeIf(config -> {
                                    return config.getAlertType() != null ? false : true;
                                });
                        this.systemAlertOrgConfig = alertOrgConfigDto;

                        syncAlertStatusAndEvaluateAlerts();
                    }
                }).exceptionally(e -> {
                    logger.error("exception in system alerts evaluation workflow ", e);
                    return null;
                });
    }

    /**
     * 1. sync system-alert status from saas side
     * 2. upon completion of above step, evaluate alerts
     */
    private void syncAlertStatusAndEvaluateAlerts() {
        syncAlertStatusWithSaaS()
                .thenAccept(v -> {
                    evaluateAlerts();
                });
    }

    /**
     * evaluate system alerts
     *
     * 1. get system alert config from lint-saas
     * 2. evaluate system alerts
     *      a. {@link SystemAlertType#LOG_FORWARDER_DROPPING_LOGS}
     *      b. {@link SystemAlertType#LOG_FORWARDER_INGESTION_FAILURE}
     * 3. if there are alert instances, post them to lint-saas
     * 4. if alert-instances are POSTed successfully, reset local state
     */
    private void evaluateAlerts() {
        Map<SystemAlertType, SystemAlertConfigDto> alertTypeVsStatus =
                Maps.uniqueIndex(this.systemAlertOrgConfig.getConfigs(), SystemAlertConfigDto::getAlertType);
        List<SystemAlertDto> systemAlertDtos = new ArrayList<>();
        SystemAlertDto logsDroppedAlert = evaluateLogsDroppedAlert(
                alertTypeVsStatus.get(SystemAlertType.LOG_FORWARDER_DROPPING_LOGS));
        SystemAlertDto ingestionFailureAlert = processIngestionFailureSystemAlert(
                alertTypeVsStatus.get(SystemAlertType.LOG_FORWARDER_INGESTION_FAILURE));
        Map<String, SystemAlertDto> logForwardingIdVsAlert =
                processLogForwardingFailureSystemAlert(
                        alertTypeVsStatus.get(SystemAlertType.LOG_FORWARDING_FAILED));
        if (logsDroppedAlert != null) {
            systemAlertDtos.add(logsDroppedAlert);
        }
        if (ingestionFailureAlert != null) {
            systemAlertDtos.add(ingestionFailureAlert);
        }
        if (logForwardingIdVsAlert.size() > 0) {
            systemAlertDtos.addAll(logForwardingIdVsAlert.values());
        }
        if (systemAlertDtos.size() > 0) {
            SystemAlertUtils
                    .fireSystemAlertsToLintSaas(systemAlertDtos)
                    .thenAccept(v -> {
                        SystemAlertUtils.updateSystemAlertsPostedMetric(systemAlertDtos.size());
                        if (logsDroppedAlert != null) {
                            logsGettingDroppedAlertStatus = logsDroppedAlert.getStatus();
                        }
                        if (ingestionFailureAlert != null) {
                            ingestionFailureAlertStatus = ingestionFailureAlert.getStatus();
                        }
                        if (logForwardingIdVsAlert.size() > 0) {
                            logForwardingIdVsAlert
                                    .entrySet()
                                    .forEach(entry -> {
                                        logForwardingIdVsAlertState.put(entry.getKey(),
                                                entry.getValue().getStatus());
                                    });
                        }
                    }).exceptionally(e -> {
                        logger.error("exception when posting system alert instances", e);
                        return null;
                    });
        }
    }

    /**
     *  1. check if system alert is enabled for org
     *  2. get the dropwizard metric related to messages getting dropped
     *  3. if the metric has exceeded threshold value and alert is not in triggered state, create a TRIGGERED alert
     *  4. if alert is in triggered state and metric has come down, , create a RESOLVED alert
     *
     * @param orgConfig system alert config for org, not null
     * @return          system alert instance, can be null
     */
    private SystemAlertDto evaluateLogsDroppedAlert(SystemAlertConfigDto orgConfig) {
        GeneralSettingsService generalSettingsService = LogForwarderConfigProperties.generalSettingsService;
        GeneralSettingsState generalSettingsState = generalSettingsService.getGeneralSettingsState();
        Double rateFromMeter = generalSettingsService.getRateFromMeter(generalSettingsState
                .getLogForwarderDroppingLogsRate(), SystemAlertUtils.getMessagesDroppedMeter());

        if (rateFromMeter > generalSettingsState.getLogForwarderDroppingLogsThreshold()) {
            if (SystemAlertStatus.TRIGGERED != logsGettingDroppedAlertStatus) {
                return createAlertInstance(SystemAlertType.LOG_FORWARDER_DROPPING_LOGS, SystemAlertStatus.TRIGGERED,
                        forwarderCustomProperties(), LogForwarderUtils.getForwarderId());
            }
        } else {
            if (SystemAlertStatus.RESOLVED != logsGettingDroppedAlertStatus) {
                return createAlertInstance(SystemAlertType.LOG_FORWARDER_DROPPING_LOGS, SystemAlertStatus.RESOLVED,
                        forwarderCustomProperties(), LogForwarderUtils.getForwarderId());
            }
        }
        return null;
    }

    /**
     *  1. check if system alert is enabled for org
     *  2. get the dropwizard metric related to ingestion failure metric
     *  3. if the metric has exceeded threshold value and alert is not in triggered state, create a TRIGGERED alert
     *  4. if alert is in triggered state and metric has come down, , create a RESOLVED alert
     *
     * @param orgConfig system alert config for org, not null
     * @return          system alert instance, can be null
     */
    private SystemAlertDto processIngestionFailureSystemAlert(SystemAlertConfigDto orgConfig) {
        GeneralSettingsService generalSettingsService = LogForwarderConfigProperties.generalSettingsService;
        GeneralSettingsState generalSettingsState = generalSettingsService.getGeneralSettingsState();
        Double rateFromMeter = generalSettingsService.getRateFromMeter(generalSettingsState
                .getLogForwarderIngestionFailureRate(), SystemAlertUtils.getPostToLintFailureMetric());
        logger.info("m15 rate for log-forwarder-ingestion-failed metric " + rateFromMeter);

        if (rateFromMeter > generalSettingsState
                .getLogForwarderIngestionFailureThreshold()) {
            if (SystemAlertStatus.TRIGGERED != ingestionFailureAlertStatus) {
                return createAlertInstance(SystemAlertType.LOG_FORWARDER_INGESTION_FAILURE, SystemAlertStatus.TRIGGERED,
                        forwarderCustomProperties(), LogForwarderUtils.getForwarderId());
            }
        } else {
            if (SystemAlertStatus.RESOLVED != ingestionFailureAlertStatus) {
                return createAlertInstance(SystemAlertType.LOG_FORWARDER_INGESTION_FAILURE, SystemAlertStatus.RESOLVED,
                        forwarderCustomProperties(), LogForwarderUtils.getForwarderId());
            }
        }
        return null;
    }

    /**
     *  1. check if system alert is enabled for org
     *  2. iterate through all the log forwarding failed related metrics
     *  3. get the dropwizard metric related to log forwarding failed related metrics
     *  3. if the metric has exceeded threshold value and alert is not in triggered state, create a TRIGGERED alert
     *  4. if alert is in triggered state and metric has come down, , create a RESOLVED alert
     *
     * @param orgConfig system alert config for org, not null
     * @return          map where key=log-forwarding config id, value=system alert dto
     */
    private Map<String, SystemAlertDto> processLogForwardingFailureSystemAlert(SystemAlertConfigDto orgConfig) {
        Map<String, SystemAlertDto> logForwardingIdVsAlert = new HashMap<>();
        GeneralSettingsService generalSettingsService = LogForwarderConfigProperties.generalSettingsService;
        GeneralSettingsState generalSettingsState = generalSettingsService.getGeneralSettingsState();
        SystemAlertUtils
                .systemAlertMetricRegistry
                .getMeters()
                .forEach((metricName, meter) -> {
                    if (metricName.startsWith(SystemAlertUtils.LOG_FORWARDING_BLOBS_FAILED_METRIC)) {
                        String logForwardingId = SystemAlertUtils
                                .getLogForwardingIdFromMetricName(metricName);
                        Map<CustomPropertyKey, String> customProperties =
                                logForwardingFailedAlertProperties(logForwardingId);
                        Double rate = generalSettingsService
                                .getRateFromMeter(generalSettingsState.getLogForwardingFailedRate(), meter);
                        if (rate > generalSettingsState
                                .getLogForwardingFailedThreshold()) {
                            if (SystemAlertStatus.TRIGGERED != logForwardingIdVsAlertState.get(logForwardingId)) {
                                SystemAlertDto alertDto = createAlertInstance(
                                        SystemAlertType.LOG_FORWARDING_FAILED,
                                        SystemAlertStatus.TRIGGERED,
                                        customProperties, logForwardingId);
                                logForwardingIdVsAlert.put(logForwardingId, alertDto);
                            }
                        } else {
                            if (SystemAlertStatus.RESOLVED != logForwardingIdVsAlertState.get(logForwardingId)) {
                                SystemAlertDto alertDto = createAlertInstance(
                                        SystemAlertType.LOG_FORWARDING_FAILED,
                                        SystemAlertStatus.RESOLVED,
                                        customProperties, logForwardingId);
                                logForwardingIdVsAlert.put(logForwardingId, alertDto);
                            }
                        }
                    }
                });
        return logForwardingIdVsAlert;
    }


    /**
     * create system alert instance
     *
     * @param alertType                 alert type
     * @param alertStatus               alert status
     * @param customProperties          properties related to the alert
     * @param referenceId               system alert reference id
     * @return                          system alert instance
     */
    private SystemAlertDto createAlertInstance(SystemAlertType alertType,
                                               SystemAlertStatus alertStatus,
                                               Map<CustomPropertyKey, String> customProperties,
                                               String referenceId) {
        SystemAlertDto systemAlertDto = new SystemAlertDto();
        systemAlertDto.setCspOrgId(systemAlertOrgConfig.getCspOrgId());
        systemAlertDto.setAlertType(alertType);
        systemAlertDto.setReportedAtMillisUTC(System.currentTimeMillis());
        systemAlertDto.setStatus(alertStatus);
        systemAlertDto.setReferenceId(referenceId);
        systemAlertDto.setCustomProperties(customProperties);
        return systemAlertDto;
    }

    public SystemAlertStatus getLogsGettingDroppedAlertStatus() {
        return logsGettingDroppedAlertStatus;
    }

    public SystemAlertStatus getIngestionFailureAlertStatus() {
        return ingestionFailureAlertStatus;
    }

    /**
     * stop system alert evaluation
     */
    public void stopEvaluation() {
        if (alertsEvaluatorTimerTask != null) {
            alertsEvaluatorTimerTask.cancel();
        }
        if (alertsConfigTimerTask != null) {
            alertsConfigTimerTask.cancel();
        }
    }

    public Map<String, SystemAlertStatus> getLogForwardingIdVsAlertState() {
        return logForwardingIdVsAlertState;
    }

    /**
     * create custom properties for log-forwarding failed alert
     *
     * @param logForwardingConfigId    log-forwarding config id
     * @return                         custom properties
     */
    private Map<CustomPropertyKey, String> logForwardingFailedAlertProperties(String logForwardingConfigId) {
        Pair<String, String> nameAndUrl = LogForwarderConfigProperties
                .logForwardingIdVsConfig.get(logForwardingConfigId);
        Map<CustomPropertyKey, String> properties = new HashMap<>();
        properties.put(CustomPropertyKey.LOG_FORWARDER_PROXY_NAME,
                LogForwarderConfigProperties.logForwarderArgs.proxyName);
        properties.put(CustomPropertyKey.LOG_FORWARDING_CONFIG_NAME, nameAndUrl.getLeft());
        properties.put(CustomPropertyKey.LOG_FORWARDING_ENDPOINT_URL, nameAndUrl.getRight());
        return properties;
    }

    private Map<CustomPropertyKey, String> forwarderCustomProperties() {
        Map<CustomPropertyKey, String> properties = new HashMap<>();
        properties.put(CustomPropertyKey.LOG_FORWARDER_PROXY_NAME,
                LogForwarderConfigProperties.logForwarderArgs.proxyName);
        properties.put(CustomPropertyKey.LOG_FORWARDER_HOST_NAME,
                LogForwarderConfigProperties.logForwarderArgs.hostName);
        properties.put(CustomPropertyKey.LOG_FORWARDER_REGION,
                LogForwarderConfigProperties.logForwarderArgs.region);
        properties.put(CustomPropertyKey.LOG_FORWARDER_ORG_TYPE,
                LogForwarderConfigProperties.logForwarderArgs.orgType);
        properties.put(CustomPropertyKey.LOG_FORWARDER_SDDC_ENV,
                LogForwarderConfigProperties.logForwarderArgs.sddcEnv);
        return properties;
    }

    /**
     * sync alert status with SaaS
     *
     * 1. get existing system alerts for the log-forwarder (referenceId=log-forwarder id)
     * 2. set/reset the alert status for alerts related to log-forwarder
     *
     * @return deferred result
     */
    DeferredResult<Void> syncAlertStatusWithSaaS() {

        return SystemAlertUtils
                .getExistingTriggeredSystemAlerts(LogForwarderUtils.getForwarderId())
                .thenAccept(existingAlerts -> {
                    logger.info("existing triggered system alerts for log-forwarder " + Utils.toJson(existingAlerts));
                    existingAlerts
                            .forEach(existingAlert -> {
                                if (SystemAlertType.LOG_FORWARDER_INGESTION_FAILURE
                                        .equals(existingAlert.getAlertType())) {
                                    ingestionFailureAlertStatus = SystemAlertStatus.TRIGGERED;
                                } else if (SystemAlertType.LOG_FORWARDER_DROPPING_LOGS
                                        .equals(existingAlert.getAlertType())) {
                                    logsGettingDroppedAlertStatus = SystemAlertStatus.TRIGGERED;
                                }
                            });
                }).exceptionally(e -> {
                    logger.error("error in get existing triggered system alerts workflow", e);
                    return null;
                });
    }

}
