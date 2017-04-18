package com.wavefront.rest.api.client.maintenancewindow.response;

import java.util.Arrays;
import java.util.List;

public class MaintenanceWindowSummaryStub {
	
	private List<MaintenanceWindow>  UPCOMING;
	
	private List<MaintenanceWindow> CURRENT;

	/**
	 * @return the uPCOMING
	 */
	public List<MaintenanceWindow> getUPCOMING() {
		return UPCOMING;
	}

	/**
	 * @param uPCOMING the uPCOMING to set
	 */
	public void setUPCOMING(List<MaintenanceWindow> uPCOMING) {
		UPCOMING = uPCOMING;
	}

	/**
	 * @return the cURRENT
	 */
	public List<MaintenanceWindow> getCURRENT() {
		return CURRENT;
	}

	/**
	 * @param cURRENT the cURRENT to set
	 */
	public void setCURRENT(List<MaintenanceWindow> cURRENT) {
		CURRENT = cURRENT;
	}

	/* (non-Javadoc)
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString() {
		return "MaintenanceWindowSummaryStub [UPCOMING=" + UPCOMING + ", CURRENT=" + CURRENT + "]";
	}
 
	 
 	
	
}
