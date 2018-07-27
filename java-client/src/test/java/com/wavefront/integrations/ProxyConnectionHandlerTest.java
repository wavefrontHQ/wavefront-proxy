package com.wavefront.integrations;

import org.junit.Test;

import javax.net.SocketFactory;
import java.net.InetSocketAddress;

import static org.junit.Assert.assertEquals;

/**
 * Test for {@link ProxyConnectionHandler}
 *
 * @author Han Zhang (zhanghan@vmware.com)
 */
public class ProxyConnectionHandlerTest {
  @Test
  public void testIncrementAndGetFailureCount() {
    ProxyConnectionHandler handler = new ProxyConnectionHandler(new InetSocketAddress("localhost", 2878),
        SocketFactory.getDefault());
    assertEquals(0, handler.getFailureCount());
    assertEquals(1, handler.incrementAndGetFailureCount());
  }
}
