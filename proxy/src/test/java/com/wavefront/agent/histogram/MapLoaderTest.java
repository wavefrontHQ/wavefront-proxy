package com.wavefront.agent.histogram;

import com.tdunning.math.stats.AgentDigest;
import com.tdunning.math.stats.AgentDigest.AgentDigestMarshaller;
import com.wavefront.agent.histogram.Utils.HistogramKey;
import com.wavefront.agent.histogram.Utils.HistogramKeyMarshaller;

import net.openhft.chronicle.map.ChronicleMap;
import net.openhft.chronicle.map.VanillaChronicleMap;

import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.concurrent.ConcurrentMap;

import static com.google.common.truth.Truth.assertThat;
import static com.wavefront.agent.histogram.TestUtils.makeKey;

/**
 * Unit tests around {@link MapLoader}.
 *
 * @author Tim Schmidt (tim@wavefront.com).
 */
public class MapLoaderTest {
  private final static short COMPRESSION = 100;

  private HistogramKey key = makeKey("mapLoaderTest");
  private AgentDigest digest = new AgentDigest(COMPRESSION, 100L);
  private File file;
  private MapLoader<HistogramKey, AgentDigest, HistogramKeyMarshaller, AgentDigestMarshaller> loader;

  @Before
  public void setup() {
    try {
      file = new File(File.createTempFile("test-file-chronicle", null).getPath() + ".map");

    } catch (IOException e) {
      e.printStackTrace();
    }

    loader = new MapLoader<>(
        HistogramKey.class,
        AgentDigest.class,
        100,
        200,
        1000,
        HistogramKeyMarshaller.get(),
        AgentDigestMarshaller.get(),
        true);
  }

  @After
  public void cleanup() {
    file.delete();
  }

  private void testPutRemove(ConcurrentMap<HistogramKey, AgentDigest> map) {
    assertThat(map).isNotNull();
    map.put(key, digest);
    assertThat(map).containsKey(key);
    map.remove(key, digest);
    assertThat(map).doesNotContainKey(key);
  }

  @Test
  public void testReconfigureMap() {
    ChronicleMap<HistogramKey, AgentDigest> map = loader.get(file);
    map.put(key, digest);
    map.close();
    loader = new MapLoader<>(HistogramKey.class, AgentDigest.class, 50, 100, 500,
        HistogramKeyMarshaller.get(), AgentDigestMarshaller.get(), true);
    map = loader.get(file);
    assertThat(map).containsKey(key);
  }

  @Test
  public void testPersistence() throws Exception {
    ChronicleMap<HistogramKey, AgentDigest> map = loader.get(file);
    map.put(key, digest);
    map.close();
    Thread.sleep(1000);
    loader = new MapLoader<>(HistogramKey.class, AgentDigest.class, 100, 200, 1000,
        HistogramKeyMarshaller.get(), AgentDigestMarshaller.get(), true);
    map = loader.get(file);
    assertThat(map).containsKey(key);
  }

  @Test
  public void testFileDoesNotExist() throws IOException {
    file.delete();
    ConcurrentMap<HistogramKey, AgentDigest> map = loader.get(file);
    assertThat(((VanillaChronicleMap)map).file()).isNotNull();
    testPutRemove(map);
  }

  @Test
  public void testDoNotPersist() throws IOException {
    loader = new MapLoader<>(
        HistogramKey.class,
        AgentDigest.class,
        100,
        200,
        1000,
        HistogramKeyMarshaller.get(),
        AgentDigestMarshaller.get(),
        false);

    ConcurrentMap<HistogramKey, AgentDigest> map = loader.get(file);
    assertThat(((VanillaChronicleMap)map).file()).isNull();
    testPutRemove(map);
  }


  // NOTE: Chronicle's repair attempt takes >1min for whatever reason.
  @Ignore
  @Test
  public void testCorruptedFileFallsBackToInMemory() throws IOException {
    FileOutputStream fos = new FileOutputStream(file);
    fos.write("Nonsense".getBytes());
    fos.flush();

    ConcurrentMap<HistogramKey, AgentDigest> map = loader.get(file);
    assertThat(((VanillaChronicleMap)map).file()).isNull();

    testPutRemove(map);
  }
}
