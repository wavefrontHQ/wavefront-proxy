package com.wavefront.common;

import org.junit.Test;

import javax.ws.rs.core.Response;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * @author vasily@wavefront.com
 */
public class HistogramUtilsTest {

  @Test
  public void testIsWavefrontResponse() {
    // normal response
    assertTrue(Utils.isWavefrontResponse(
        Response.status(408).entity("{\"code\":408,\"message\":\"some_message\"}").build()
    ));
    // order does not matter, should be true
    assertTrue(Utils.isWavefrontResponse(
        Response.status(407).entity("{\"message\":\"some_message\",\"code\":407}").build()
    ));
    // extra fields ok
    assertTrue(Utils.isWavefrontResponse(
        Response.status(408).entity("{\"code\":408,\"message\":\"some_message\",\"extra\":0}").
            build()
    ));
    // non well formed JSON: closing curly brace missing, should be false
    assertFalse(Utils.isWavefrontResponse(
        Response.status(408).entity("{\"code\":408,\"message\":\"some_message\"").build()
    ));
    // code is not the same as status code, should be false
    assertFalse(Utils.isWavefrontResponse(
        Response.status(407).entity("{\"code\":408,\"message\":\"some_message\"}").build()
    ));
    // message missing
    assertFalse(Utils.isWavefrontResponse(
        Response.status(407).entity("{\"code\":408}").build()
    ));
  }
}