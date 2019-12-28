package com.wavefront.agent.data;

/**
 * Additional context to help understand why a certain batch was queued.
 *
 * @author vasily@wavefront.com
 */
public enum QueueingReason {
  PUSHBACK("pushback"),              // server pushback
  AUTH("auth"),                      // feature not enabled or auth error
  SPLIT("split"),                    // splitting batches
  RETRY("retry"),                    // all other errors (http error codes or network errors)
  BUFFER_SIZE("bufferSize"),         // buffer size threshold exceeded
  MEMORY_PRESSURE("memoryPressure"), // heap memory limits exceeded
  DURABILITY("durability");          // force-flush for maximum durability (for future use)

  private final String name;

  QueueingReason(String name) {
    this.name = name;
  }

  public String toString() {
    return this.name;
  }
}
