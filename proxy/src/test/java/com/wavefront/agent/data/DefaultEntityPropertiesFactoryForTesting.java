package com.wavefront.agent.data;

import com.wavefront.data.ReportableEntityType;

/**
 * @author vasily@wavefront.com
 */
public class DefaultEntityPropertiesFactoryForTesting implements EntityPropertiesFactory {
  private final EntityProperties props = new DefaultEntityPropertiesForTesting();

  @Override
  public EntityProperties get(ReportableEntityType entityType) {
    return props;
  }

  @Override
  public EntityProperties.GlobalProperties getGlobalProperties() {
    return props.getGlobalProperties();
  }
}
