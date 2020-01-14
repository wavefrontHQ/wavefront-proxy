package com.wavefront.agent.auth;

import com.google.common.collect.ImmutableList;

import com.wavefront.agent.TestUtils;

import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.MetricName;
import org.apache.http.client.HttpClient;
import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.message.BasicNameValuePair;
import org.easymock.EasyMock;
import org.junit.Test;

import javax.annotation.concurrent.NotThreadSafe;
import java.io.IOException;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;

import static com.wavefront.agent.TestUtils.assertTrueWithTimeout;
import static com.wavefront.agent.TestUtils.httpEq;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

@NotThreadSafe
public class Oauth2TokenIntrospectionAuthenticatorTest {

  @Test
  public void testIntrospectionUrlInvocation() throws Exception {
    HttpClient client = EasyMock.createMock(HttpClient.class);

    AtomicLong fakeClock = new AtomicLong(1_000_000);
    TokenAuthenticator authenticator = new Oauth2TokenIntrospectionAuthenticator(client,
        "http://acme.corp/oauth", null, 300, 600, fakeClock::get);
    assertTrue(authenticator.authRequired());
    assertFalse(authenticator.authorize(null));

    String uuid = UUID.randomUUID().toString();

    HttpPost request = new HttpPost("http://acme.corp/oauth");
    request.setHeader("Content-Type", "application/x-www-form-urlencoded");
    request.setHeader("Accept", "application/json");
    request.setEntity(new UrlEncodedFormEntity(ImmutableList.of(new BasicNameValuePair("token", uuid))));

    TestUtils.expectHttpResponse(client, request, "{\"active\": false}".getBytes(), 200);

    assertFalse(authenticator.authorize(uuid)); // should call http
    fakeClock.getAndAdd(60_000);
    assertFalse(authenticator.authorize(uuid)); // should be cached
    fakeClock.getAndAdd(300_000);
    EasyMock.verify(client);
    EasyMock.reset(client);
    TestUtils.expectHttpResponse(client, request, "{\"active\": true}".getBytes(), 200);
    assertFalse(authenticator.authorize(uuid)); // cache expired - should trigger a refresh
    // should call http and get an updated token
    assertTrueWithTimeout(100, () -> authenticator.authorize(uuid));
    fakeClock.getAndAdd(180_000);
    assertTrue(authenticator.authorize(uuid)); // should be cached
    fakeClock.getAndAdd(180_000);
    EasyMock.verify(client);
    EasyMock.reset(client);
    TestUtils.expectHttpResponse(client, request, "{\"active\": false}".getBytes(), 200);
    assertTrue(authenticator.authorize(uuid)); // cache expired - should trigger a refresh
    //Thread.sleep(100);
    assertTrueWithTimeout(100, () -> !authenticator.authorize(uuid)); // should call http
    EasyMock.verify(client);
  }

  @Test
  public void testIntrospectionUrlCachedLastResultExpires() throws Exception {
    HttpClient client = EasyMock.createMock(HttpClient.class);
    AtomicLong fakeClock = new AtomicLong(1_000_000);
    TokenAuthenticator authenticator = new Oauth2TokenIntrospectionAuthenticator(client,
        "http://acme.corp/oauth", "Bearer: abcde12345", 300, 600, fakeClock::get);

    String uuid = UUID.randomUUID().toString();

    HttpPost request = new HttpPost("http://acme.corp/oauth");
    request.setHeader("Content-Type", "application/x-www-form-urlencoded");
    request.setHeader("Accept", "application/json");
    request.setEntity(new UrlEncodedFormEntity(ImmutableList.of(new BasicNameValuePair("token", uuid))));

    TestUtils.expectHttpResponse(client, request, "{\"active\": true}".getBytes(), 204);

    assertTrue(authenticator.authorize(uuid)); // should call http
    Thread.sleep(100);

    fakeClock.getAndAdd(630_000);

    EasyMock.verify(client);
    EasyMock.reset(client);

    EasyMock.expect(client.execute(httpEq(new HttpPost("http://acme.corp/oauth")))).
        andThrow(new IOException("Timeout!")).times(3);
    EasyMock.replay(client);

    assertTrue(authenticator.authorize(uuid)); // should call http, fail, but still return last valid result
    Thread.sleep(100);
    assertFalse(authenticator.authorize(uuid)); // TTL expired - should fail
    assertFalse(authenticator.authorize(uuid)); // Should call http again - TTL expired

    EasyMock.verify(client);
  }

  @Test
  public void testIntrospectionUrlInvalidResponseThrows() throws Exception {
    HttpClient client = EasyMock.createMock(HttpClient.class);
    AtomicLong fakeClock = new AtomicLong(1_000_000);
    TokenAuthenticator authenticator = new Oauth2TokenIntrospectionAuthenticator(client,
        "http://acme.corp/oauth", "Bearer: abcde12345", 300, 600, fakeClock::get);

    String uuid = UUID.randomUUID().toString();

    HttpPost request = new HttpPost("http://acme.corp/oauth");
    request.setHeader("Content-Type", "application/x-www-form-urlencoded");
    request.setHeader("Accept", "application/json");
    request.setEntity(new UrlEncodedFormEntity(ImmutableList.of(new BasicNameValuePair("token", uuid))));

    TestUtils.expectHttpResponse(client, request, "{\"inActive\": true}".getBytes(), 204);

    long count = Metrics.newCounter(new MetricName("auth", "", "api-errors")).count();
    authenticator.authorize(uuid); // should call http
    assertEquals(1, Metrics.newCounter(new MetricName("auth", "", "api-errors")).count() - count);
  }
}
