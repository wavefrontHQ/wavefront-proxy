package com.wavefront.agent.histogram.tape;

import com.google.common.base.Preconditions;

import com.squareup.tape.FileObjectQueue;

import net.jpountz.lz4.LZ4BlockInputStream;
import net.jpountz.lz4.LZ4BlockOutputStream;

import org.apache.commons.io.IOUtils;

import java.io.ByteArrayInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Logger;
import java.util.zip.GZIPInputStream;

/**
 * [Square] Tape converter for Lists of strings with LZ4 compression support.
 *
 * Binary format is signature[4b] = 0x54415045, version[4b] = 0x00000001, length[4b], {utf8Len[4b], utfSeq}*
 *
 * @author Tim Schmidt (tim@wavefront.com).
 * @author Vasily Vorontsov (vasily@wavefront.com)
 *
 */
public class TapeStringListConverter implements FileObjectQueue.Converter<List<String>> {
  private static final Logger logger = Logger.getLogger(TapeStringListConverter.class.getCanonicalName());

  private static final TapeStringListConverter INSTANCE_DEFAULT = new TapeStringListConverter(false);
  private static final TapeStringListConverter INSTANCE_COMPRESSION_ENABLED = new TapeStringListConverter(true);
  private static final Charset UTF8 = Charset.forName("UTF-8");
  private static final int CONTAINER_SIGNATURE = 0x54415045;
  private static final int CONTAINER_VERSION = 0x00000001;

  private final boolean isCompressionEnabled;

  private TapeStringListConverter(boolean isCompressionEnabled) {
    this.isCompressionEnabled = isCompressionEnabled;
  }

  /**
   * Returns the TapeStringListConverter object instance with default settings (no compression)
   *
   * @return <code>TapeStringListConverter</code> object instance
   */
  public static TapeStringListConverter getDefaultInstance() {
    return INSTANCE_DEFAULT;
  }

  /**
   * Returns the TapeStringListConverter object instance with LZ4 compression enabled
   *
   * @return <code>TapeStringListConverter</code> object instance
   */
  public static TapeStringListConverter getCompressionEnabledInstance() {
    return INSTANCE_COMPRESSION_ENABLED;
  }

  @Override
  public List<String> from(byte[] bytes) throws IOException {
    try {
      byte[] uncompressedData;
      if (bytes.length > 2 && bytes[0] == (byte) 0x1f && bytes[1] == (byte) 0x8b) { // gzip signature
        uncompressedData = IOUtils.toByteArray(new GZIPInputStream(new ByteArrayInputStream(bytes)));
      } else if (bytes.length > 2 && bytes[0] == (byte) 0x4c && bytes[1] == (byte) 0x5a) { // LZ block signature
        uncompressedData = IOUtils.toByteArray(new LZ4BlockInputStream(new ByteArrayInputStream(bytes)));
      } else {
        uncompressedData = bytes;
      }
      Preconditions.checkArgument(uncompressedData.length > 12, "Uncompressed container must be at least 12 bytes");
      ByteBuffer in = ByteBuffer.wrap(uncompressedData);
      int signature = in.getInt();
      int version = in.getInt();
      if (signature != CONTAINER_SIGNATURE || version != CONTAINER_VERSION) {
        logger.severe("WF-502: Unexpected data format retrieved from tape (signature = " + signature +
            ", version = " + version);
        return null;
      }
      int count = in.getInt();
      List<String> result = new ArrayList<>(count);
      for (int i = 0; i < count; ++i) {
        int len = in.getInt();
        result.add(new String(uncompressedData, in.position(), len, UTF8));
        in.position(in.position() + len);
      }
      return result;
    } catch (Throwable t) {
      logger.severe("WF-501: Corrupt data retrieved from tape, ignoring: " + t);
      return null;
    }
  }

  @Override
  public void toStream(List<String> stringList, OutputStream outputStream) throws IOException {
    OutputStream wrapperStream = null;
    DataOutputStream dOut;
    if (isCompressionEnabled) {
      wrapperStream = new LZ4BlockOutputStream(outputStream);
      dOut = new DataOutputStream(wrapperStream);
    } else {
      dOut = new DataOutputStream(outputStream);
    }
    dOut.writeInt(CONTAINER_SIGNATURE);
    dOut.writeInt(CONTAINER_VERSION);
    dOut.writeInt(stringList.size());
    for (String s : stringList) {
      byte[] b = s.getBytes(UTF8);
      dOut.writeInt(b.length);
      dOut.write(b);
    }
    // flush?
    dOut.close();
    if (wrapperStream != null) {
      wrapperStream.close();
    }
  }
}
