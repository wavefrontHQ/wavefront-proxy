package com.wavefront.agent;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;

import com.google.common.base.Charsets;
import com.google.common.io.Files;
import java.io.File;
import java.util.UUID;
import org.easymock.EasyMock;
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

  /** Reads a pre-written id file and returns the stored UUID. */
  @Test
  public void testLoadProxyIdFromFileReadsExistingId() throws Exception {
    UUID stored = UUID.randomUUID();
    File idFile = File.createTempFile("proxyTestIdFile", ".id");
    idFile.deleteOnExit();
    Files.asCharSink(idFile, Charsets.UTF_8).write(stored.toString());

    assertEquals(stored, ProxyUtil.getOrCreateProxyIdFromFile(idFile.getPath()));
  }

  /** Creates a new id file when the path does not yet exist, then persists the same UUID. */
  @Test
  public void testLoadProxyIdFromFileCreatesAndPersists() throws Exception {
    File idFile = File.createTempFile("proxyTestIdFile", ".new");
    idFile.delete(); // remove so the method creates it fresh
    idFile.deleteOnExit();

    UUID first = ProxyUtil.getOrCreateProxyIdFromFile(idFile.getPath());
    UUID second = ProxyUtil.getOrCreateProxyIdFromFile(idFile.getPath());
    assertNotNull(first);
    assertEquals("Persisted id must be returned on subsequent calls", first, second);
  }

  /** A file containing garbage content must throw RuntimeException. */
  @Test(expected = RuntimeException.class)
  public void testLoadProxyIdFromFileMalformedContent() throws Exception {
    File idFile = File.createTempFile("proxyTestIdFile", ".bad");
    idFile.deleteOnExit();
    Files.asCharSink(idFile, Charsets.UTF_8).write("not-a-valid-uuid");
    ProxyUtil.getOrCreateProxyIdFromFile(idFile.getPath());
  }

  /** A directory path instead of a file must throw RuntimeException. */
  @Test(expected = RuntimeException.class)
  public void testLoadProxyIdFromFilePathIsDirectory() throws Exception {
    File dir = File.createTempFile("proxyTestIdDir", null);
    dir.delete();
    dir.mkdir();
    dir.deleteOnExit();
    ProxyUtil.getOrCreateProxyIdFromFile(dir.getPath());
  }

  /** Ephemeral mode: every call returns a fresh unique UUID (nothing is persisted). */
  @Test
  public void testGetOrCreateProxyIdEphemeral() {
    ProxyConfig proxyConfig = EasyMock.createMock(ProxyConfig.class);
    EasyMock.expect(proxyConfig.isEphemeral()).andReturn(true).times(2);
    EasyMock.replay(proxyConfig);

    UUID id1 = ProxyUtil.getOrCreateProxyId(proxyConfig);
    UUID id2 = ProxyUtil.getOrCreateProxyId(proxyConfig);
    assertNotNull(id1);
    assertNotEquals("Each ephemeral call must produce a different UUID", id1, id2);
  }

  /** Non-ephemeral mode: reads and returns the UUID stored in the id file. */
  @Test
  public void testGetOrCreateProxyIdNonEphemeral() throws Exception {
    UUID expectedId = UUID.randomUUID();
    File idFile = File.createTempFile("proxyTestIdFile", ".id");
    idFile.deleteOnExit();
    Files.asCharSink(idFile, Charsets.UTF_8).write(expectedId.toString());

    ProxyConfig proxyConfig = EasyMock.createMock(ProxyConfig.class);
    EasyMock.expect(proxyConfig.isEphemeral()).andReturn(false);
    EasyMock.expect(proxyConfig.getIdFile()).andReturn(idFile.getPath());
    EasyMock.replay(proxyConfig);

    assertEquals(expectedId, ProxyUtil.getOrCreateProxyId(proxyConfig));
  }

  /** Ephemeral mode: every per-tenant call returns a fresh UUID (never persisted). */
  @Test
  public void testGetOrCreateProxyIdForTenantEphemeral() {
    ProxyConfig proxyConfig = EasyMock.createMock(ProxyConfig.class);
    EasyMock.expect(proxyConfig.isEphemeral()).andReturn(true).times(2);
    EasyMock.replay(proxyConfig);

    UUID id1 = ProxyUtil.getOrCreateProxyId(proxyConfig, "tenant1");
    UUID id2 = ProxyUtil.getOrCreateProxyId(proxyConfig, "tenant1");
    assertNotNull(id1);
    assertNotEquals("Ephemeral tenant ids must differ on each call", id1, id2);
  }

  /**
   * Non-ephemeral mode: tenant id is stored in {@code baseIdFile_tenantName} and the same UUID is
   * returned on repeated calls.
   */
  @Test
  public void testGetOrCreateProxyIdForTenantNonEphemeral() throws Exception {
    UUID expectedId = UUID.randomUUID();
    File baseIdFile = File.createTempFile("proxyTestIdFile", ".base");
    baseIdFile.deleteOnExit();

    File tenantIdFile = new File(baseIdFile.getPath() + "_sidecar");
    tenantIdFile.deleteOnExit();
    Files.asCharSink(tenantIdFile, Charsets.UTF_8).write(expectedId.toString());

    ProxyConfig proxyConfig = EasyMock.createMock(ProxyConfig.class);
    EasyMock.expect(proxyConfig.isEphemeral()).andReturn(false).times(2);
    EasyMock.expect(proxyConfig.getIdFile()).andReturn(baseIdFile.getPath()).times(2);
    EasyMock.replay(proxyConfig);

    UUID id1 = ProxyUtil.getOrCreateProxyId(proxyConfig, "sidecar");
    UUID id2 = ProxyUtil.getOrCreateProxyId(proxyConfig, "sidecar");
    assertEquals(expectedId, id1);
    assertEquals("Same tenant id must be returned on repeated calls", id1, id2);
  }

  /**
   * Non-ephemeral mode with a null base id file: falls back to
   * {@code .wavefront_proxy_id_{tenantName}} in the working directory.
   */
  @Test
  public void testGetOrCreateProxyIdForTenantNullBaseIdFile() throws Exception {
    String tenantName = "nullBaseTenant_" + UUID.randomUUID().toString().replace("-", "");
    File tenantIdFile = new File(".wavefront_proxy_id_" + tenantName);
    tenantIdFile.deleteOnExit();

    ProxyConfig proxyConfig = EasyMock.createMock(ProxyConfig.class);
    EasyMock.expect(proxyConfig.isEphemeral()).andReturn(false).times(2);
    EasyMock.expect(proxyConfig.getIdFile()).andReturn(null).times(2);
    EasyMock.replay(proxyConfig);

    try {
      UUID id1 = ProxyUtil.getOrCreateProxyId(proxyConfig, tenantName);
      UUID id2 = ProxyUtil.getOrCreateProxyId(proxyConfig, tenantName);
      assertNotNull(id1);
      assertEquals("Same id must be returned when base id file is null", id1, id2);
    } finally {
      tenantIdFile.delete();
    }
  }
}
