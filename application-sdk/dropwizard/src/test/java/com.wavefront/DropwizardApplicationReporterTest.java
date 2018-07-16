package com.wavefront;

import com.wavefront.dropwizard.app.SampleApp;

import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.net.URISyntaxException;

import javax.annotation.Nullable;

import okhttp3.HttpUrl;
import okhttp3.MediaType;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.RequestBody;
import okhttp3.Response;
import okio.BufferedSink;

import static org.junit.Assert.assertEquals;

/**
 * Test class to test reported metric/histogram for Dropwizard (Jersey) apps requests/responses
 *
 * @author Sushant Dewan (sushant@wavefront.com).
 */
public class DropwizardApplicationReporterTest {

  private final SampleApp sampleApp = new SampleApp();

  @Before
  public void setup() throws Exception {
    sampleApp.run("server");
  }

  @Test
  public void testCRUD() throws URISyntaxException, IOException {
    testCreate();
    testRead();
    testUpdate();
    testDelete();
    testGetAll();
  }

  private void testCreate() throws IOException {
    assertEquals(204, invokePostRequest("sample/foo/bar"));
    // Request counter metric
    assertEquals(1, sampleApp.reportedValue("request.sample.foo.bar.POST"));
    // Response counter metric
    assertEquals(1, sampleApp.reportedValue("response.sample.foo.bar.POST.204"));
    // Response latency histogram
    assertEquals(1, sampleApp.reportedValue("response.sample.foo.bar.POST.204.latency"));
  }

  private void testRead() throws IOException {
    assertEquals(200, invokeGetRequest("sample/foo/bar/123"));
    // Request counter metric
    assertEquals(1, sampleApp.reportedValue("request.sample.foo.bar._id_.GET"));
    // Response counter metric
    assertEquals(1, sampleApp.reportedValue("response.sample.foo.bar._id_.GET.200"));
    // Response latency histogram
    assertEquals(1, sampleApp.reportedValue("response.sample.foo.bar._id_.GET.200.latency"));
  }

  private void testUpdate() throws IOException {
    assertEquals(204, invokePutRequest("sample/foo/bar/123"));
    // Request counter metric
    assertEquals(1, sampleApp.reportedValue("request.sample.foo.bar._id_.PUT"));
    // Response counter metric
    assertEquals(1, sampleApp.reportedValue("response.sample.foo.bar._id_.PUT.204"));
    // Response latency histogram
    assertEquals(1, sampleApp.reportedValue("response.sample.foo.bar._id_.PUT.204.latency"));
  }

  private void testDelete() throws IOException {
    assertEquals(204, invokeDeleteRequest("sample/foo/bar/123"));
    // Request counter metric
    assertEquals(1, sampleApp.reportedValue("request.sample.foo.bar._id_.DELETE"));
    // Response counter metric
    assertEquals(1, sampleApp.reportedValue("response.sample.foo.bar._id_.DELETE.204"));
    // Response latency histogram
    assertEquals(1, sampleApp.reportedValue("response.sample.foo.bar._id_.DELETE.204.latency"));
  }

  private void testGetAll() throws IOException {
    assertEquals(200, invokeGetRequest("sample/foo/bar"));
    // Request counter metric
    assertEquals(1, sampleApp.reportedValue("request.sample.foo.bar.GET"));
    // Response counter metric
    assertEquals(1, sampleApp.reportedValue("response.sample.foo.bar.GET.200"));
    // Response latency histogram
    assertEquals(1, sampleApp.reportedValue("response.sample.foo.bar.GET.200.latency"));
  }

  private int invokePostRequest(String pathSegments) throws IOException {
    HttpUrl url = new HttpUrl.Builder().scheme("http").host("localhost").port(8080).
        addPathSegments(pathSegments).build();
    Request request = new Request.Builder().url(url).
        post(new RequestBody() {
          @Nullable
          @Override
          public MediaType contentType() {
            return null;
          }

          @Override
          public void writeTo(BufferedSink sink) throws IOException {

          }
        }).build();
    OkHttpClient okHttpClient = new OkHttpClient().newBuilder().build();
    Response response = okHttpClient.newCall(request).execute();
    return response.code();
  }

  private int invokeGetRequest(String pathSegments) throws IOException {
    HttpUrl url = new HttpUrl.Builder().scheme("http").host("localhost").port(8080).
        addPathSegments(pathSegments).build();
    Request.Builder requestBuilder = new Request.Builder().url(url);
    OkHttpClient okHttpClient = new OkHttpClient().newBuilder().build();
    Request request = requestBuilder.build();
    Response response = okHttpClient.newCall(request).execute();
    return response.code();
  }

  private int invokePutRequest(String pathSegments) throws IOException {
    HttpUrl url = new HttpUrl.Builder().scheme("http").host("localhost").port(8080).
        addPathSegments(pathSegments).build();
    Request request = new Request.Builder().url(url).
        put(new RequestBody() {
          @Nullable
          @Override
          public MediaType contentType() {
            return null;
          }

          @Override
          public void writeTo(BufferedSink sink) throws IOException {

          }
        }).build();
    OkHttpClient okHttpClient = new OkHttpClient().newBuilder().build();
    Response response = okHttpClient.newCall(request).execute();
    return response.code();
  }

  private int invokeDeleteRequest(String pathSegments) throws IOException {
    HttpUrl url = new HttpUrl.Builder().scheme("http").host("localhost").port(8080).
        addPathSegments(pathSegments).build();
    Request request = new Request.Builder().url(url).
        delete(new RequestBody() {
          @Nullable
          @Override
          public MediaType contentType() {
            return null;
          }

          @Override
          public void writeTo(BufferedSink sink) throws IOException {

          }
        }).build();
    OkHttpClient okHttpClient = new OkHttpClient().newBuilder().build();
    Response response = okHttpClient.newCall(request).execute();
    return response.code();
  }
}
