package com.wavefront.agent.data;

import com.wavefront.data.ReportableEntityType;

/**
 * @author vasily@wavefront.com
 */
public class DefaultEntityPropertiesFactoryForTesting implements EntityPropertiesFactory {
  private final EntityProperties props = new DefaultEntityPropertiesForTesting();
  private final GlobalProperties globalProps = new DefaultGlobalPropertiesForTesting();

  @Override
  public EntityProperties get(ReportableEntityType entityType) {
    return props;
  }

  @Override
  public GlobalProperties getGlobalProperties() {
    return globalProps;
  }
}
