package com.wavefront.api.agent;

import com.google.common.base.Preconditions;
import com.google.common.collect.Sets;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;

import java.io.File;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.UUID;

/**
 * Configuration for the SSH Daemon.
 *
 * @author Clement Pang (clement@sunnylabs.com)
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonIgnoreProperties(ignoreUnknown = true)
public class AgentConfiguration {

  public String name;
  public String defaultUsername;
  public String defaultPublicKey;
  public boolean allowAnyHostKeys;
  public Long currentTime;
  private List<SshTargetDTO> targets;
  private List<WorkUnit> workUnits;
  private Boolean collectorSetsPointsPerBatch;
  private Long pointsPerBatch;
  private Boolean collectorSetsRetryBackoff;
  private Double retryBackoffBaseSeconds;
  private Boolean collectorSetsRateLimit;
  private Long collectorRateLimit;
  private Boolean shutOffAgents = false;
  private Boolean showTrialExpired = false;
  // If the value is true, then histogram feature is disabled.
  // If the value is null or false, then histograms are not disabled i.e. enabled (default behavior)
  private Boolean histogramDisabled;
  // If the value is true, then trace feature is disabled; feature enabled if the value is null or false
  private Boolean traceDisabled;
  private ValidationConfiguration validationConfiguration;

  public Boolean getCollectorSetsRetryBackoff() {
    return collectorSetsRetryBackoff;
  }

  public void setCollectorSetsRetryBackoff(Boolean collectorSetsRetryBackoff) {
    this.collectorSetsRetryBackoff = collectorSetsRetryBackoff;
  }

  public Double getRetryBackoffBaseSeconds() {
    return retryBackoffBaseSeconds;
  }

  public void setRetryBackoffBaseSeconds(Double retryBackoffBaseSeconds) {
    this.retryBackoffBaseSeconds = retryBackoffBaseSeconds;
  }

  public Boolean getCollectorSetsRateLimit() {
    return this.collectorSetsRateLimit;
  }

  public void setCollectorSetsRateLimit(Boolean collectorSetsRateLimit) {
    this.collectorSetsRateLimit = collectorSetsRateLimit;
  }

  public Long getCollectorRateLimit() {
    return this.collectorRateLimit;
  }

  public void setCollectorRateLimit(Long collectorRateLimit) {
    this.collectorRateLimit = collectorRateLimit;
  }

  public List<WorkUnit> getWorkUnits() {
    if (workUnits == null) return Collections.emptyList();
    return workUnits;
  }

  public List<SshTargetDTO> getTargets() {
    if (targets == null) return Collections.emptyList();
    return targets;
  }

  public void setCollectorSetsPointsPerBatch(Boolean collectorSetsPointsPerBatch) {
    this.collectorSetsPointsPerBatch = collectorSetsPointsPerBatch;
  }

  public Boolean getCollectorSetsPointsPerBatch() {
    return collectorSetsPointsPerBatch;
  }

  public void setTargets(List<SshTargetDTO> targets) {
    this.targets = targets;
  }

  public void setWorkUnits(List<WorkUnit> workUnits) {
    this.workUnits = workUnits;
  }

  public Long getPointsPerBatch() {
    return pointsPerBatch;
  }

  public void setPointsPerBatch(Long pointsPerBatch) {
    this.pointsPerBatch = pointsPerBatch;
  }

  public Boolean getShutOffAgents() { return shutOffAgents; }

  public void setShutOffAgents(Boolean shutOffAgents) {
    this.shutOffAgents = shutOffAgents;
  }

  public Boolean getShowTrialExpired() { return showTrialExpired; }

  public void setShowTrialExpired(Boolean trialExpired) {
    this.showTrialExpired = trialExpired;
  }

  public Boolean getHistogramDisabled() {
    return histogramDisabled;
  }

  public void setHistogramDisabled(Boolean histogramDisabled) {
    this.histogramDisabled = histogramDisabled;
  }

  public Boolean getTraceDisabled() {
    return this.traceDisabled;
  }

  public void setTraceDisabled(Boolean traceDisabled) {
    this.traceDisabled = traceDisabled;
  }

  public ValidationConfiguration getValidationConfiguration() {
    return this.validationConfiguration;
  }

  public void setValidationConfiguration(ValidationConfiguration value) {
    this.validationConfiguration = value;
  }

  public void validate(boolean local) {
    Set<UUID> knownHostUUIDs = Collections.emptySet();
    if (targets != null) {
      if (defaultPublicKey != null) {
        Preconditions.checkArgument(new File(defaultPublicKey).exists(), "defaultPublicKey does not exist");
      }
      knownHostUUIDs = Sets.newHashSetWithExpectedSize(targets.size());
      for (SshTargetDTO target : targets) {
        Preconditions.checkNotNull(target, "target cannot be null");
        target.validate(this);
        Preconditions.checkState(knownHostUUIDs.add(target.id), "duplicate target id: " + target.id);
        if (target.user == null) {
          Preconditions.checkNotNull(defaultUsername,
              "must have default username if user is not specified, host entry: " + target.host);
        }
        if (target.publicKey == null) {
          Preconditions.checkNotNull(defaultPublicKey,
              "must have default publickey if publicKey is not specified, host entry: " + target.host);
        }
        if (!allowAnyHostKeys) {
          Preconditions.checkNotNull(target.hostKey, "must specify hostKey if " +
              "'allowAnyHostKeys' is set to false, host entry: " + target.host);
        }
      }
    }
    if (workUnits != null) {
      for (WorkUnit unit : workUnits) {
        Preconditions.checkNotNull(unit, "workUnit cannot be null");
        unit.validate();
        if (!local) {
          Preconditions.checkState(knownHostUUIDs.containsAll(unit.targets), "workUnit: " +
              unit.name + " refers to a target host that does not exist");
        }
      }
    }
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;

    AgentConfiguration that = (AgentConfiguration) o;

    if (allowAnyHostKeys != that.allowAnyHostKeys) return false;
    if (defaultPublicKey != null ? !defaultPublicKey.equals(that.defaultPublicKey) : that.defaultPublicKey != null)
      return false;
    if (defaultUsername != null ? !defaultUsername.equals(that.defaultUsername) : that.defaultUsername != null)
      return false;
    if (name != null ? !name.equals(that.name) : that.name != null) return false;
    if (targets != null ? !targets.equals(that.targets) : that.targets != null) return false;
    if (workUnits != null ? !workUnits.equals(that.workUnits) : that.workUnits != null)
      return false;

    return true;
  }

  @Override
  public int hashCode() {
    int result = name != null ? name.hashCode() : 0;
    result = 31 * result + (defaultUsername != null ? defaultUsername.hashCode() : 0);
    result = 31 * result + (defaultPublicKey != null ? defaultPublicKey.hashCode() : 0);
    result = 31 * result + (allowAnyHostKeys ? 1 : 0);
    result = 31 * result + (targets != null ? targets.hashCode() : 0);
    result = 31 * result + (workUnits != null ? workUnits.hashCode() : 0);
    return result;
  }
}
