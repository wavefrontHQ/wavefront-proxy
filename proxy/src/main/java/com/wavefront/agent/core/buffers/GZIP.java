package com.wavefront.agent.core.buffers;

import static java.nio.charset.StandardCharsets.UTF_8;

import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.Histogram;
import com.yammer.metrics.core.MetricName;
import java.io.*;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;
import org.apache.activemq.artemis.api.core.ICoreMessage;
import org.apache.commons.io.IOUtils;

class GZIP {
  private static final Histogram compressTime =
      Metrics.newHistogram(new MetricName("buffer.gzip.compress", "", "time"));
  private static final Histogram decompressTime =
      Metrics.newHistogram(new MetricName("buffer.gzip.decompress", "", "time"));

  protected static byte[] compress(final String stringToCompress) {
    long start = System.currentTimeMillis();
    try (final ByteArrayOutputStream baos = new ByteArrayOutputStream();
        final GZIPOutputStream gzipOutput = new GZIPOutputStream(baos)) {
      gzipOutput.write(stringToCompress.getBytes(UTF_8));
      gzipOutput.finish();
      return baos.toByteArray();
    } catch (IOException e) {
      throw new UncheckedIOException("Error while compression!", e);
    } finally {
      compressTime.update(System.currentTimeMillis() - start);
    }
  }

  protected static String decompress(ICoreMessage msg) {
    byte[] array = msg.getBodyBuffer().byteBuf().array();

    long start = System.currentTimeMillis();
    try (final ByteArrayInputStream is = new ByteArrayInputStream(array)) {
      is.read(); // First 4 byte are the message length
      is.read();
      is.read();
      is.read();
      try (final GZIPInputStream gzipInput = new GZIPInputStream(is);
          final StringWriter stringWriter = new StringWriter()) {
        IOUtils.copy(gzipInput, stringWriter, UTF_8);
        return stringWriter.toString();
      } catch (IOException e) {
        throw new UncheckedIOException("Error while decompression!", e);
      } finally {
        System.out.println("-->" + (System.currentTimeMillis() - start));
        decompressTime.update(System.currentTimeMillis() - start);
      }
    } catch (IOException e) {
      throw new UncheckedIOException("Error while decompression!", e);
    }
  }
}
