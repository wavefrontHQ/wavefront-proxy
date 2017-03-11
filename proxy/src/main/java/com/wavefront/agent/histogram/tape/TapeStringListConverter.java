package com.wavefront.agent.histogram.tape;

import com.squareup.tape.FileObjectQueue;

import net.jpountz.lz4.LZ4BlockInputStream;
import net.jpountz.lz4.LZ4BlockOutputStream;

import org.apache.commons.io.IOUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.ByteArrayInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;

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
  private static final Logger logger = LogManager.getLogger(TapeStringListConverter.class.getCanonicalName());

  private static final TapeStringListConverter INSTANCE_DEFAULT = new TapeStringListConverter(false);
  private static final TapeStringListConverter INSTANCE_COMPRESSION_ENABLED = new TapeStringListConverter(true);
  private static final Charset UTF8 = Charset.forName("UTF-8");
  private static final int CONTAINER_SIGNATURE = 0x54415045;
  private static final int CONTAINER_VERSION = 0x00000001;

  private final boolean isCompressionEnabled;

  private TapeStringListConverter(boolean isCompressionEnabled) {
    this.isCompressionEnabled = isCompressionEnabled;
  }

  public static TapeStringListConverter getDefaultInstance() {
    return INSTANCE_DEFAULT;
  }

  public static TapeStringListConverter getCompressionEnabledInstance() {
    return INSTANCE_COMPRESSION_ENABLED;
  }

  @Override
  public List<String> from(byte[] bytes) throws IOException {
    try {
      ByteBuffer in;
      byte[] uncompressedData;
      if (bytes.length > 2 && bytes[0] == (byte) 0x4C && bytes[1] == (byte) 0x5A) { // LZ block signature
        uncompressedData = IOUtils.toByteArray(new LZ4BlockInputStream(new ByteArrayInputStream(bytes)));
        in = ByteBuffer.wrap(uncompressedData);
      } else {
        uncompressedData = bytes;
        in = ByteBuffer.wrap(bytes);
      }
      int signature = in.getInt();
      int version = in.getInt();
      if (signature != CONTAINER_SIGNATURE || version != CONTAINER_VERSION) {
        logger.error("Unknown data format retrieved from tape (signature = " + signature + ", version = " + version);
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
      logger.error("Corrupt data retrieved from tape, ignoring: " + t);
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
