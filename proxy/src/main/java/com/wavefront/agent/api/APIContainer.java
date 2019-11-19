package com.wavefront.agent.api;

import com.wavefront.api.EventAPI;
import com.wavefront.api.ProxyV2API;
import com.wavefront.api.SourceTagAPI;

/**
 *
 * @author vasily@wavefront.com
 */
public class APIContainer {
  private ProxyV2API proxyV2API;
  private SourceTagAPI sourceTagAPI;
  private EventAPI eventAPI;

  public APIContainer() {
  }

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
