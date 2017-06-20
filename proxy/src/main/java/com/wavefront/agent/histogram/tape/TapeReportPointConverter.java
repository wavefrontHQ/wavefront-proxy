package com.wavefront.agent.histogram.tape;

import com.squareup.tape.FileObjectQueue;

import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;

import java.io.IOException;
import java.io.OutputStream;

import wavefront.report.ReportPoint;

/**
 * Adapter exposing the Avro's {@link org.apache.avro.specific.SpecificRecord} encoding/decoding to Square tape's {@link
 * com.squareup.tape.FileObjectQueue.Converter} interface.
 *
 * @author Tim Schmidt (tim@wavefront.com).
 */
public class TapeReportPointConverter implements FileObjectQueue.Converter<ReportPoint> {
  private static final TapeReportPointConverter INSTANCE = new TapeReportPointConverter();

  private TapeReportPointConverter() {
    // Singleton
  }

  public static TapeReportPointConverter get() {
    return INSTANCE;
  }

  @Override
  public ReportPoint from(byte[] bytes) throws IOException {
    SpecificDatumReader<ReportPoint> reader = new SpecificDatumReader<>(ReportPoint.SCHEMA$);
    org.apache.avro.io.Decoder decoder = DecoderFactory.get().binaryDecoder(bytes, null);

    return reader.read(null, decoder);
  }

  @Override
  public void toStream(ReportPoint point, OutputStream outputStream) throws IOException {
    BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(outputStream, null);
    DatumWriter<ReportPoint> writer = new SpecificDatumWriter<>(ReportPoint.SCHEMA$);

    writer.write(point, encoder);
    encoder.flush();
  }
}
