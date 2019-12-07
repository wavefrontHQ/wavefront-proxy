package com.wavefront.agent.data;

public enum TaskQueueingDirective {
  NEVER,    // never queue (not used, placeholder for future use)
  MEMORY,   // queue on memory pressure (heap threshold or pushMemoryBufferLimit exceeded)
  PUSHBACK, // queue on pushback + memory pressure
  DEFAULT,  // queue on any errors, pushback or memory pressure
  ALWAYS    // queue before send attempts (maximum durability - placeholder for future use)
}
