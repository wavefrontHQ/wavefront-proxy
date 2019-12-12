package com.wavefront.agent.api;

import com.wavefront.api.EventAPI;
import com.wavefront.api.ProxyV2API;
import com.wavefront.api.SourceTagAPI;

/**
 * Container for all Wavefront back-end API objects (proxy, source tag, event)
 *
 * @author vasily@wavefront.com
 */
public class APIContainer {
  private ProxyV2API proxyV2API;
  private SourceTagAPI sourceTagAPI;
  private EventAPI eventAPI;

  /**
   * @param proxyV2API   API endpoint for line-delimited payloads
   * @param sourceTagAPI API endpoint for source tags
   * @param eventAPI     API endpoint for events
   */
  public APIContainer(ProxyV2API proxyV2API, SourceTagAPI sourceTagAPI, EventAPI eventAPI) {
    this.proxyV2API = proxyV2API;
    this.sourceTagAPI = sourceTagAPI;
    this.eventAPI = eventAPI;
  }

  public ProxyV2API getProxyV2API() {
    return proxyV2API;
  }

  public void setProxyV2API(ProxyV2API proxyV2API) {
    this.proxyV2API = proxyV2API;
  }

  public SourceTagAPI getSourceTagAPI() {
    return sourceTagAPI;
  }

  public void setSourceTagAPI(SourceTagAPI sourceTagAPI) {
    this.sourceTagAPI = sourceTagAPI;
  }

  public EventAPI getEventAPI() {
    return eventAPI;
  }

  public void setEventAPI(EventAPI eventAPI) {
    this.eventAPI = eventAPI;
  }
}
