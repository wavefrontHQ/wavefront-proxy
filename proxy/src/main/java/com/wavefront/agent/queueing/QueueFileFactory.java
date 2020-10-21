package com.wavefront.agent.queueing;

import java.io.IOException;

/**
 * Factory for {@link QueueFile} instances.
 *
 * @author vasily@wavefront.com
 */
public interface QueueFileFactory {

  /**
   * Creates, or accesses an existing file, with the specified name.
   *
   * @param fileName file name to use
   * @return queue file instance
   * @throws IOException if file could not be created or accessed
   */
  QueueFile get(String fileName) throws IOException;
}
