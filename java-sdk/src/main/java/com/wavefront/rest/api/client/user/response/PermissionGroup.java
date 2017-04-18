package com.wavefront.rest.api.client.user.response;

public enum PermissionGroup {
 
// User Profile Permission Groups	
	BROWSE,
	AGENT_MANAGEMENT,
	ALERTS_MANAGEMENT,
	DASHBOARD_MANAGEMENT,
	INGESTION,
	EVENTS_MANAGEMENT,
	HOST_TAG_MANAGEMENT,
	USER_MANAGEMENT;
	
	@Override
	   public String toString() {
	        return name().toLowerCase();
	    }
}
