package com.wavefront.agent.data;

/**
 * Controls conditions under which proxy would actually queue data.
 *
 * @author vasily@wavefront.com
 */
public enum TaskQueueLevel {
  NEVER(0),     // never queue (not used, placeholder for future use)
  MEMORY(1),    // queue on memory pressure (heap threshold or pushMemoryBufferLimit exceeded)
  PUSHBACK(2),  // queue on pushback + memory pressure
  ANY_ERROR(3), // queue on any errors, pushback or memory pressure
  ALWAYS(4);    // queue before send attempts (maximum durability - placeholder for future use)

  private final int level;

  TaskQueueLevel(int level) {
    this.level = level;
  }

  public boolean isLessThan(TaskQueueLevel other) {
    return this.level < other.level;
  }

  public static TaskQueueLevel fromString(String name) {
    for (TaskQueueLevel level : TaskQueueLevel.values()) {
      if (level.toString().equalsIgnoreCase(name)) {
        return level;
      }
    }
    return null;
  }
}
