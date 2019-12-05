package com.wavefront.agent.api;

import com.wavefront.dto.Event;

import java.util.List;
import java.util.UUID;

import javax.ws.rs.HeaderParam;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Response;

/**
 * Wrapper around WavefrontV2API that supports forced writing of tasks to the backing queue.
 *
 * @author Andrew Kao (akao@wavefront.com)
 * @author vasily@wavefront.com
 */
public interface ForceQueueEnabledProxyAPI extends WavefrontV2API {

  /**
   * Report batched data (metrics, histograms, spans, etc) to Wavefront servers.
   *
   * @param proxyId       Proxy Id reporting the result.
   * @param format        The format of the data (wavefront, histogram, trace, spanLogs)
   * @param pushData      Push data batch (newline-delimited)
   * @param forceToQueue  Whether to bypass posting data to the API and write to queue instead.
   */
  Response proxyReport(@HeaderParam("X-WF-PROXY-ID") final UUID proxyId,
                       @QueryParam("format") final String format,
                       final String pushData,
                       boolean forceToQueue);

  /**
   * Add a single tag to a source.
   *
   * @param id           source ID.
   * @param tagValue     tag value to add.
   * @param forceToQueue Whether to bypass posting data to the API and write to queue instead.
   */
  Response appendTag(String id, String tagValue, boolean forceToQueue);

  /**
   * Remove a single tag from a source.
   *
   * @param id           source ID.
   * @param tagValue     tag to remove.
   * @param forceToQueue Whether to bypass posting data to the API and write to queue instead.
   */
  Response removeTag(String id, String tagValue, boolean forceToQueue);

  /**
   * Sets tags for a host, overriding existing tags.
   *
   * @param id              source ID.
   * @param tagsValuesToSet tags to set.
   * @param forceToQueue    Whether to bypass posting data to the API and write to queue instead.
   */
  Response setTags(String id, List<String> tagsValuesToSet, boolean forceToQueue);

  /**
   * Set description for a source.
   *
   * @param id           source ID.
   * @param desc         description.
   * @param forceToQueue Whether to bypass posting data to the API and write to queue instead.
   */
  Response setDescription(String id, String desc, boolean forceToQueue);

  /**
   * Remove description from a source.
   *
   * @param id           source ID.
   * @param forceToQueue Whether to bypass posting data to the API and write to queue instead.
   */
  Response removeDescription(String id, boolean forceToQueue);

  /**
   * Create an event.
   *
   * @param proxyId      id of the proxy submitting events.
   * @param eventBatch   events to create.
   * @param forceToQueue Whether to bypass posting data to the API and write to queue instead.
   */
  Response proxyEvents(UUID proxyId, List<Event> eventBatch, boolean forceToQueue);
}
