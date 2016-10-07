package com.wavefront.agent.histogram.tape;

import com.squareup.tape.FileObjectQueue;

import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

/**
 * [Square] Tape converter for Lists of strings.
 *
 * Binary format is length[2b], {utf8Len[2b], utfSeq}*
 *
 * @author Tim Schmidt (tim@wavefront.com).
 */
public class TapeStringListConverter implements FileObjectQueue.Converter<List<String>> {
  private static final TapeStringListConverter INSTANCE = new TapeStringListConverter();

  private TapeStringListConverter() {
    // Singleton
  }

  public static TapeStringListConverter get() {
    return INSTANCE;
  }

  @Override
  public List<String> from(byte[] bytes) throws IOException {
    ByteBuffer in = ByteBuffer.wrap(bytes);
    int count = in.getShort();
    List<String> result = new ArrayList<>(count);
    for (int i = 0; i < count; ++i) {
      int len = in.getShort();
      result.add(new String(bytes, in.position(), len));
      in.position(in.position() + len);
    }
    return result;
  }

  @Override
  public void toStream(List<String> stringList, OutputStream outputStream) throws IOException {
    DataOutputStream dOut = new DataOutputStream(outputStream);
    dOut.writeShort(stringList.size());
    for (String s : stringList) {
      byte[] b = s.getBytes();
      dOut.writeShort(b.length);
      dOut.write(b);
    }
    // flush?
  }
}
