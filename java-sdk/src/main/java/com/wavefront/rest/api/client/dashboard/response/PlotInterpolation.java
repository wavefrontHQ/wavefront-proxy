package com.wavefront.rest.api.client.dashboard.response;

public enum PlotInterpolation {

	LINEAR, STEP_BEFORE, STEP_AFTER,

	BASIS, CARDINAL, MONOTONE;

	@Override
	public String toString() {
		return name().toLowerCase();
	}

}
