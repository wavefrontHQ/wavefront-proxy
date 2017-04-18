package com.wavefront.rest.api.client.utils;

public enum Severity {

	SMOKE , INFO, WARN, SEVERE;
 	
	@Override
	  public String toString(){
		  return name().toLowerCase();
		  
	  }
}
