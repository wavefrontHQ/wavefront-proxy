package com.wavefront.agent;

import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.wavefront.agent.auth.TokenAuthenticator;
import com.wavefront.agent.channel.HealthCheckManager;
import com.wavefront.agent.listeners.AbstractHttpOnlyHandler;
import com.wavefront.common.Clock;
import com.wavefront.ingester.TcpIngester;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.HttpResponse;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.util.CharsetUtil;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.File;
import java.net.URI;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

import static com.wavefront.agent.ProxyUtil.createInitializer;
import static com.wavefront.agent.TestUtils.findAvailablePort;
import static com.wavefront.agent.TestUtils.gzippedHttpPost;
import static com.wavefront.agent.channel.ChannelUtils.makeResponse;
import static com.wavefront.agent.channel.ChannelUtils.writeHttpResponse;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * @author vasily@wavefront.com
 */
public class HttpEndToEndTest {
  private PushAgent proxy;
  private AtomicBoolean trackingFlag = new AtomicBoolean(false);
  private MutableFunc<FullHttpRequest, HttpResponse> server = new MutableFunc<>(x -> null);
  private Thread thread;
  private int backendPort;
  private AtomicInteger test2Counter = new AtomicInteger(0);
  private long time = Clock.now() / 1000;
  private String expectedTest1part1 =
      "\"metric.name\" 1.0 " + time + " source=\"metric.source\" \"tagk1\"=\"tagv1\"\n" +
      "\"metric.name\" 2.0 " + time + " source=\"metric.source\" \"tagk1\"=\"tagv2\"";
  private String expectedTest1part2 =
      "\"metric.name\" 3.0 " + time + " source=\"metric.source\" \"tagk1\"=\"tagv3\"\n" +
      "\"metric.name\" 4.0 " + time + " source=\"metric.source\" \"tagk1\"=\"tagv4\"";


  @Before
  public void setup() {
    backendPort = findAvailablePort(8081);
    ChannelHandler channelHandler = new WrappingHttpHandler(null, null,
        String.valueOf(backendPort), server);
    thread = new Thread(new TcpIngester(createInitializer(channelHandler,
        backendPort, 32768, 16 * 1024 * 1024, 5), backendPort));
    thread.start();
  }

  @After
  public void teardown() {
    thread.interrupt();
  }

  @Test
  public void testEndToEndFunctionality() throws Exception {
    int proxyPort = findAvailablePort(2898);
    String buffer = File.createTempFile("proxyTestBuffer", null).getPath();

    proxy = new PushAgent();
    proxy.proxyConfig.server = "http://localhost:" + backendPort + "/api/";
    proxy.proxyConfig.flushThreads = 1;
    proxy.proxyConfig.pushListenerPorts = String.valueOf(proxyPort);
    proxy.proxyConfig.pushFlushInterval = 100;
    proxy.proxyConfig.bufferFile = buffer;
    proxy.start(new String[]{});
    Thread.sleep(500);

    String payload =
        "metric.name 1 " + time + " source=metric.source tagk1=tagv1\n" +
        "metric.name 2 " + time + " source=metric.source tagk1=tagv2\n" +
        "metric.name 3 " + time + " source=metric.source tagk1=tagv3\n" +
        "metric.name 4 " + time + " source=metric.source tagk1=tagv4\n";

    server.update(this::test1);
    gzippedHttpPost("http://localhost:" + proxyPort + "/", payload);
    Thread.sleep(250);
    assertTrue(trackingFlag.getAndSet(false));

    server.update(this::test2);
    gzippedHttpPost("http://localhost:" + proxyPort + "/", payload);
    Thread.sleep(250);
    gzippedHttpPost("http://localhost:" + proxyPort + "/", payload);
    Thread.sleep(1000);
    assertTrue(trackingFlag.getAndSet(false));
  }

  private HttpResponse test1(FullHttpRequest req) {
    String content = req.content().toString(CharsetUtil.UTF_8);
    //System.out.println(content);
    assertEquals(expectedTest1part1 + "\n" + expectedTest1part2, content);
    trackingFlag.set(true);
    return makeResponse(HttpResponseStatus.OK, "");
  }

  private HttpResponse test2(FullHttpRequest req) {
    //req.headers().entries().forEach(x -> System.out.println(x.getKey() + "=" + x.getValue()));
    String content = req.content().toString(CharsetUtil.UTF_8);
    System.out.println(content);
    switch (test2Counter.incrementAndGet()) {
      case 1:
        assertEquals(expectedTest1part1 + "\n" + expectedTest1part2, content);
        return makeResponse(HttpResponseStatus.TOO_MANY_REQUESTS, "");
      case 2:
        assertEquals(expectedTest1part1 + "\n" + expectedTest1part2, content);
        return makeResponse(HttpResponseStatus.OK, "");
      case 3:
        assertEquals(expectedTest1part1 + "\n" + expectedTest1part2, content);
        return makeResponse(HttpResponseStatus.valueOf(407), "");
      case 4:
        assertEquals(expectedTest1part1 + "\n" + expectedTest1part2, content);
        return makeResponse(HttpResponseStatus.REQUEST_ENTITY_TOO_LARGE, "");
      case 5:
        assertEquals(expectedTest1part1, content);
        return makeResponse(HttpResponseStatus.OK, "");
      case 6:
        assertEquals(expectedTest1part2, content);
        trackingFlag.set(true);
        return makeResponse(HttpResponseStatus.OK, "");
    }
    throw new IllegalStateException();
  }


  private static class WrappingHttpHandler extends AbstractHttpOnlyHandler {
    private final Function<FullHttpRequest, HttpResponse> func;
    public WrappingHttpHandler(@Nullable TokenAuthenticator tokenAuthenticator,
                               @Nullable HealthCheckManager healthCheckManager,
                               @Nullable String handle,
                               @Nonnull Function<FullHttpRequest, HttpResponse> func) {
      super(tokenAuthenticator, healthCheckManager, handle);
      this.func = func;
    }

    @Override
    protected void handleHttpMessage(ChannelHandlerContext ctx, FullHttpRequest request) {
      URI uri;
      try {
         uri = new URI(request.uri());
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
      String path = uri.getPath();
      System.out.println("Path: " + uri.getPath());
      if (path.endsWith("/checkin") &&
          (path.startsWith("/api/daemon") || path.contains("wfproxy"))) {
        // simulate checkin response for proxy chaining
        ObjectNode jsonResponse = JsonNodeFactory.instance.objectNode();
        jsonResponse.put("currentTime", Clock.now());
        jsonResponse.put("allowAnyHostKeys", true);
        writeHttpResponse(ctx, HttpResponseStatus.OK, jsonResponse, request);
        return;
      } else if (path.endsWith("/config/processed")){
        writeHttpResponse(ctx, HttpResponseStatus.OK, "", request);
        return;
      }
      writeHttpResponse(ctx, func.apply(request), request);
    }
  }

  private static class MutableFunc<T, R> implements Function<T, R> {
    private Function<T, R> delegate;

    public MutableFunc(Function<T, R> delegate) {
      this.delegate = delegate;
    }

    public void update(Function<T, R> delegate) {
      this.delegate = delegate;
    }

    @Override
    public R apply(T t) {
      return delegate.apply(t);
    }
  }
}
