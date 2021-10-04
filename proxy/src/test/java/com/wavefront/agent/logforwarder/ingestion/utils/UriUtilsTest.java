package com.wavefront.agent.logforwarder.ingestion.utils;

import com.wavefront.agent.logforwarder.ingestion.util.UriUtils;

import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.net.URI;

/**
 * @author Manoj Ramakrishnan (rmanoj@vmware.com).
 * @since 10/4/21 12:16 PM
 */
public class UriUtilsTest {

  @Rule
  public ExpectedException expectedException = ExpectedException.none();

  @Test
  public void testNormalizeUriPath() {
    Assert.assertEquals("", UriUtils.normalizeUriPath(null));
    Assert.assertEquals("", UriUtils.normalizeUriPath(""));
    Assert.assertEquals("/customer/abc", UriUtils.normalizeUriPath("customer/abc/"));
    Assert.assertEquals("/customer/abc", UriUtils.normalizeUriPath("/customer/abc/"));
    Assert.assertEquals("/customer/abc", UriUtils.normalizeUriPath("customer/abc/"));
  }

  @Test
  public void testBuildUriPath() {
    Assert.assertEquals("/customer", UriUtils.buildUriPath("customer"));
    Assert.assertEquals("/customer/abc", UriUtils.buildUriPath("customer", "abc"));
    Assert.assertEquals("/customer/abc", UriUtils.buildUriPath("customer", "/abc"));
    Assert.assertEquals("/customer/abc", UriUtils.buildUriPath("/customer", "/abc"));
    Assert.assertEquals("/customer/abc", UriUtils.buildUriPath("/customer", "/abc/", null, ""));
  }

  @Test
  public void testGetLastPathSegment() throws Exception {
    Assert.assertEquals("", UriUtils.getLastPathSegment("customer/abc/"));
    Assert.assertEquals("abc", UriUtils.getLastPathSegment("customer/abc"));

    Assert.assertEquals("", UriUtils.getLastPathSegment(new URI("http://localhost:8000/")));
    Assert.assertEquals("abc", UriUtils.getLastPathSegment(new URI("http://localhost:8000/abc")));
  }
}
