package com.wavefront.api.agent;

import com.google.common.base.Preconditions;
import com.google.common.collect.Sets;

import java.util.Set;
import java.util.UUID;

/**
 * A work unit to execute.
 *
 * @author Clement Pang (clement@sunnylabs.com)
 */
public class WorkUnit {
  /**
   * Unique id for the work unit (used for reporting).
   */
  public UUID id;
  /**
   * Friendly name for the work unit.
   */
  public String name;
  /**
   * Seconds between work unit executions.
   */
  public long delay;
  /**
   * Command to execute.
   */
  public String command;
  /**
   * Stage of this work unit -- trial, per-fetch, or active.
   */
  public MetricStage stage;
  /**
   * Targets that participate in this work unit.
   */
  public Set<UUID> targets = Sets.newHashSet();

  public void validate() {
    Preconditions.checkNotNull(id, "id cannot be null for a work unit");
    Preconditions.checkNotNull(name, "name cannot be null for a work unit");
    Preconditions.checkNotNull(targets, "targets cannot be null for work unit: " + name);
    Preconditions.checkNotNull(command, "command must not be null for work unit: " + name);
    Preconditions.checkNotNull(stage, "stage cannot be null for work unit: " + name);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;

    WorkUnit workUnit = (WorkUnit) o;

    if (delay != workUnit.delay) return false;
    if (command != null ? !command.equals(workUnit.command) : workUnit.command != null)
      return false;
    if (!id.equals(workUnit.id)) return false;
    if (name != null ? !name.equals(workUnit.name) : workUnit.name != null) return false;
    if (targets != null ? !targets.equals(workUnit.targets) : workUnit.targets != null)
      return false;
    if (stage != null ? !stage.equals(workUnit.stage) : workUnit.stage != null) return false;

    return true;
  }

  @Override
  public int hashCode() {
    int result = id.hashCode();
    result = 31 * result + (name != null ? name.hashCode() : 0);
    result = 31 * result + (int) (delay ^ (delay >>> 32));
    result = 31 * result + (command != null ? command.hashCode() : 0);
    result = 31 * result + (targets != null ? targets.hashCode() : 0);
    result = 31 * result + (stage != null ? stage.hashCode() : 0);
    return result;
  }

  public WorkUnit clone() {
    WorkUnit cloned = new WorkUnit();
    cloned.delay = this.delay;
    cloned.name = this.name;
    cloned.command = this.command;
    cloned.targets = Sets.newHashSet(this.targets);
    cloned.stage = this.stage;
    cloned.id = this.id;
    return cloned;
  }
}
