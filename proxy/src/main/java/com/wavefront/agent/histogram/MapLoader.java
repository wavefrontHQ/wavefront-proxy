package com.wavefront.agent.histogram;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Preconditions;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import net.openhft.chronicle.hash.serialization.BytesReader;
import net.openhft.chronicle.hash.serialization.BytesWriter;
import net.openhft.chronicle.hash.serialization.SizedReader;
import net.openhft.chronicle.hash.serialization.SizedWriter;
import net.openhft.chronicle.map.ChronicleMap;
import net.openhft.chronicle.map.VanillaChronicleMap;

import javax.annotation.Nonnull;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Writer;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Loader for {@link ChronicleMap}. If a file already exists at the given location, will make an attempt to load the map
 * from the existing file. Will fall-back to an in memory representation if the file cannot be loaded (see logs).
 *
 * @author Tim Schmidt (tim@wavefront.com).
 */
public class MapLoader<K, V, KM extends BytesReader<K> & BytesWriter<K>, VM extends SizedReader<V> & SizedWriter<V>> {
  private static final Logger logger = Logger.getLogger(MapLoader.class.getCanonicalName());

  /**
   * Allow ChronicleMap to grow beyond initially allocated size instead of crashing. Since it makes the map a lot less
   * efficient, we should log a warning if the actual number of elements exceeds the allocated.
   * A bloat factor of 1000 is the highest possible value which we are going to use here, as we need to prevent
   * crashes at all costs.
   */
  private static final double MAX_BLOAT_FACTOR = 1000;

  private static final ObjectMapper JSON_PARSER = new ObjectMapper();

  private final Class<K> keyClass;
  private final Class<V> valueClass;
  private final long entries;
  private final double avgKeySize;
  private final double avgValueSize;
  private final KM keyMarshaller;
  private final VM valueMarshaller;
  private final boolean doPersist;
  private final LoadingCache<File, ChronicleMap<K, V>> maps =
      CacheBuilder.newBuilder().build(new CacheLoader<File, ChronicleMap<K, V>>() {

        private ChronicleMap<K, V> newPersistedMap(File file) throws IOException {
          return ChronicleMap.of(keyClass, valueClass)
              .keyMarshaller(keyMarshaller)
              .valueMarshaller(valueMarshaller)
              .entries(entries)
              .averageKeySize(avgKeySize)
              .averageValueSize(avgValueSize)
              .maxBloatFactor(MAX_BLOAT_FACTOR)
              .createPersistedTo(file);
        }

        private ChronicleMap<K, V> newInMemoryMap() {
          return ChronicleMap.of(keyClass, valueClass)
              .keyMarshaller(keyMarshaller)
              .valueMarshaller(valueMarshaller)
              .entries(entries)
              .averageKeySize(avgKeySize)
              .averageValueSize(avgValueSize)
              .maxBloatFactor(MAX_BLOAT_FACTOR)
              .create();
        }

        private MapSettings loadSettings(File file) throws IOException {
          return JSON_PARSER.readValue(new FileReader(file), MapSettings.class);
        }

        private void saveSettings(MapSettings settings, File file) throws IOException {
          Writer writer = new FileWriter(file);
          JSON_PARSER.writeValue(writer, settings);
          writer.close();
        }

        @Override
        public ChronicleMap<K, V> load(@Nonnull File file) throws Exception {
          if (!doPersist) {
            logger.log(Level.WARNING, "Accumulator persistence is disabled, unflushed histograms " +
                "will be lost on proxy shutdown.");
            return newInMemoryMap();
          }

          MapSettings newSettings = new MapSettings(entries, avgKeySize, avgValueSize);
          File settingsFile = new File(file.getAbsolutePath().concat(".settings"));
          try {
            if (file.exists()) {
              if (settingsFile.exists()) {
                MapSettings settings = loadSettings(settingsFile);
                if (!settings.equals(newSettings)) {
                  logger.info(file.getName() + " settings changed, reconfiguring (this may take a few moments)...");
                  File originalFile = new File(file.getAbsolutePath());
                  File oldFile = new File(file.getAbsolutePath().concat(".temp"));
                  if (oldFile.exists()) {
                    //noinspection ResultOfMethodCallIgnored
                    oldFile.delete();
                  }
                  //noinspection ResultOfMethodCallIgnored
                  file.renameTo(oldFile);

                  ChronicleMap<K, V> toMigrate = ChronicleMap
                      .of(keyClass, valueClass)
                      .entries(settings.getEntries())
                      .averageKeySize(settings.getAvgKeySize())
                      .averageValueSize(settings.getAvgValueSize())
                      .recoverPersistedTo(oldFile, false);

                  ChronicleMap<K, V> result = newPersistedMap(originalFile);

                  if (toMigrate.size() > 0) {
                    logger.info(originalFile.getName() + " starting data migration (" + toMigrate.size() + " records)");
                    for (K key : toMigrate.keySet()) {
                      result.put(key, toMigrate.get(key));
                    }
                    toMigrate.close();
                    logger.info(originalFile.getName() + " data migration finished");
                  }

                  saveSettings(newSettings, settingsFile);
                  //noinspection ResultOfMethodCallIgnored
                  oldFile.delete();
                  logger.info(originalFile.getName() + " reconfiguration finished");

                  return result;
                }
              }

              logger.fine("Restoring accumulator state from " + file.getAbsolutePath());
              // Note: this relies on an uncorrupted header, which according to the docs would be due to a hardware error or fs bug.
              ChronicleMap<K, V> result = ChronicleMap
                  .of(keyClass, valueClass)
                  .entries(entries)
                  .averageKeySize(avgKeySize)
                  .averageValueSize(avgValueSize)
                  .recoverPersistedTo(file, false);

              if (result.isEmpty()) {
                // Create a new map with the supplied settings to be safe.
                result.close();
                //noinspection ResultOfMethodCallIgnored
                file.delete();
                logger.fine("Empty accumulator - reinitializing: " + file.getName());
                result = newPersistedMap(file);
              } else {
                // Note: as of 3.10 all instances are.
                if (result instanceof VanillaChronicleMap) {
                  logger.fine("Accumulator map restored from " + file.getAbsolutePath());
                  VanillaChronicleMap vcm = (VanillaChronicleMap) result;
                  if (!vcm.keyClass().equals(keyClass) ||
                      !vcm.valueClass().equals(valueClass)) {
                    throw new IllegalStateException("Persisted map params are not matching expected map params "
                        + " key " + "exp: " + keyClass.getSimpleName() + " act: " + vcm.keyClass().getSimpleName()
                        + " val " + "exp: " + valueClass.getSimpleName() + " act: " + vcm.valueClass().getSimpleName());
                  }
                }
              }
              saveSettings(newSettings, settingsFile);
              return result;

            } else {
              logger.fine("Accumulator map initialized as " + file.getName());
              saveSettings(newSettings, settingsFile);
              return newPersistedMap(file);
            }
          } catch (Exception e) {
            logger.log(Level.SEVERE, "Failed to load/create map from '" + file.getAbsolutePath() +
                    "'. Please move or delete the file and restart the proxy! Reason: ", e);
            throw new RuntimeException(e);
          }
        }
      });

  /**
   * Creates a new {@link MapLoader}
   *
   * @param keyClass the Key class
   * @param valueClass the Value class
   * @param entries the maximum number of entries
   * @param avgKeySize the average marshaled key size in bytes
   * @param avgValueSize the average marshaled value size in bytes
   * @param keyMarshaller the key codec
   * @param valueMarshaller the value codec
   * @param doPersist whether to persist the map
   */
  public MapLoader(Class<K> keyClass,
                   Class<V> valueClass,
                   long entries,
                   double avgKeySize,
                   double avgValueSize,
                   KM keyMarshaller,
                   VM valueMarshaller,
                   boolean doPersist) {
    this.keyClass = keyClass;
    this.valueClass = valueClass;
    this.entries = entries;
    this.avgKeySize = avgKeySize;
    this.avgValueSize = avgValueSize;
    this.keyMarshaller = keyMarshaller;
    this.valueMarshaller = valueMarshaller;
    this.doPersist = doPersist;
  }

  public ChronicleMap<K, V> get(File f) throws Exception {
    Preconditions.checkNotNull(f);
    return maps.get(f);
  }

  @Override
  public String toString() {
    return "MapLoader{" +
        "keyClass=" + keyClass +
        ", valueClass=" + valueClass +
        ", entries=" + entries +
        ", avgKeySize=" + avgKeySize +
        ", avgValueSize=" + avgValueSize +
        ", keyMarshaller=" + keyMarshaller +
        ", valueMarshaller=" + valueMarshaller +
        ", doPersist=" + doPersist +
        ", maps=" + maps +
        '}';
  }
}
