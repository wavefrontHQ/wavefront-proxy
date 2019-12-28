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

  private int level;

  TaskQueueLevel(int level) {
    this.level = level;
  }

  public boolean isGreaterThan(TaskQueueLevel other) {
    return this.level > other.level;
  }

  public boolean isLessThan(TaskQueueLevel other) {
    return this.level < other.level;
  }

}
