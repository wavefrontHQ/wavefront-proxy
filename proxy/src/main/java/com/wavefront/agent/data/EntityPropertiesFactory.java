package com.wavefront.agent.data;

import com.wavefront.data.ReportableEntityType;

/**
 * Generates entity-specific wrappers for dynamic proxy settings.
 *
 * @author vasily@wavefront.com
 */
public interface EntityPropertiesFactory {

  /**
   * Get an entity-specific wrapper for proxy runtime properties.
   *
   * @param entityType entity type to get wrapper for
   * @return EntityProperties wrapper
   */
  EntityProperties get(ReportableEntityType entityType);
}
