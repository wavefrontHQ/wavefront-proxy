package com.wavefront.agent.auth;

import org.apache.http.HttpVersion;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.message.BasicHttpResponse;
import org.easymock.EasyMock;
import org.junit.Test;

import java.io.IOException;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;

import static com.wavefront.agent.TestUtils.assertTrueWithTimeout;
import static com.wavefront.agent.TestUtils.httpEq;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class HttpGetTokenIntrospectionAuthenticatorTest {

  @Test
  public void testIntrospectionUrlInvocation() throws Exception {
    HttpClient client = EasyMock.createMock(HttpClient.class);
    AtomicLong fakeClock = new AtomicLong(1_000_000);
    TokenAuthenticator authenticator = new HttpGetTokenIntrospectionAuthenticator(client,
        "http://acme.corp/{{token}}/something", "Bearer: abcde12345", 300, 600, fakeClock::get);
    assertTrue(authenticator.authRequired());
    assertFalse(authenticator.authorize(null));

    String uuid = UUID.randomUUID().toString();
    EasyMock.expect(client.execute(httpEq(new HttpGet("http://acme.corp/" + uuid + "/something")))).
        andReturn(new BasicHttpResponse(HttpVersion.HTTP_1_1, 401, "Auth failed")).once().
        andReturn(new BasicHttpResponse(HttpVersion.HTTP_1_1, 204, "")).once().
        andReturn(new BasicHttpResponse(HttpVersion.HTTP_1_1, 401, "Auth failed")).once();
    EasyMock.replay(client);
    assertFalse(authenticator.authorize(uuid)); // should call http
    fakeClock.getAndAdd(60_000);
    assertFalse(authenticator.authorize(uuid)); // should be cached
    fakeClock.getAndAdd(300_000);
    assertFalse(authenticator.authorize(uuid)); // cache expired - should trigger a refresh
    // should call http and get an updated token
    assertTrueWithTimeout(100, () -> authenticator.authorize(uuid));
    fakeClock.getAndAdd(180_000);
    assertTrue(authenticator.authorize(uuid)); // should be cached
    fakeClock.getAndAdd(180_000);
    assertTrue(authenticator.authorize(uuid)); // cache expired - should trigger a refresh
    assertTrueWithTimeout(100, () -> !authenticator.authorize(uuid)); // should call http
    EasyMock.verify(client);
  }

  @Test
  public void testIntrospectionUrlCachedLastResultExpires() throws Exception {
    HttpClient client = EasyMock.createMock(HttpClient.class);
    AtomicLong fakeClock = new AtomicLong(1_000_000);
    TokenAuthenticator authenticator = new HttpGetTokenIntrospectionAuthenticator(client,
        "http://acme.corp/{{token}}/something", null, 300, 600, fakeClock::get);

    String uuid = UUID.randomUUID().toString();
    EasyMock.expect(client.execute(httpEq(new HttpGet("http://acme.corp/" + uuid + "/something")))).
        andReturn(new BasicHttpResponse(HttpVersion.HTTP_1_1, 204, "")).once().
        andThrow(new IOException("Timeout!")).times(3);
    EasyMock.replay(client);
    assertTrue(authenticator.authorize(uuid)); // should call http
    fakeClock.getAndAdd(630_000);
    assertTrue(authenticator.authorize(uuid)); // should call http, fail, but still return last valid result
    //Thread.sleep(100);
    assertTrueWithTimeout(100, () -> !authenticator.authorize(uuid)); // TTL expired - should fail
    //Thread.sleep(100);
    // Should call http again - TTL expired
    assertTrueWithTimeout(100, () -> !authenticator.authorize(uuid));
    EasyMock.verify(client);
  }
}
