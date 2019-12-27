package com.wavefront.agent.data;

public enum TaskResult {
  DELIVERED, PERSISTED_RETRY, PERSISTED, RETRY_IMMEDIATELY, RETRY_LATER
}
