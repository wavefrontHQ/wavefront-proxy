package com.wavefront.agent;

import static org.junit.Assert.assertEquals;

import com.google.common.base.Charsets;
import com.google.common.io.Files;
import java.io.File;
import java.util.UUID;
import org.junit.Test;

/** @author vasily@wavefront.com */
public class ProxyUtilTest {

  @Test
  public void testLoadProxyIdFromFile() throws Exception {
    UUID proxyId = UUID.randomUUID();
    String path = File.createTempFile("proxyTestIdFile", null).getPath();
    Files.asCharSink(new File(path), Charsets.UTF_8).write(proxyId.toString());
    UUID uuid = ProxyUtil.getOrCreateProxyIdFromFile(path);
    assertEquals(proxyId, uuid);

    path = File.createTempFile("proxyTestIdFile", null).getPath() + ".id";
    uuid = ProxyUtil.getOrCreateProxyIdFromFile(path);
    assertEquals(uuid, ProxyUtil.getOrCreateProxyIdFromFile(path));
  }
}
