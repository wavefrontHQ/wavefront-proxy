package com.wavefront.agent.core.buffers;

import org.apache.commons.lang.StringUtils;

public class SQSBufferConfig {
  public String template;
  public String region;
  public String id;
  public int vto = 60;

  public void validate() {
    if (StringUtils.isBlank(id)) {
      throw new IllegalArgumentException(
          "sqsQueueIdentifier cannot be blank! Please correct " + "your configuration settings.");
    }

    if (!(template.contains("{{id}}") && template.contains("{{entity}}"))) {
      throw new IllegalArgumentException(
          "sqsQueueNameTemplate is invalid! Must contain " + "{{id}} and {{entity}} replacements.");
    }
  }
}
