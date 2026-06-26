package org.logstash.beats;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufOutputStream;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ByteToMessageDecoder;
import java.nio.charset.Charset;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.zip.Inflater;
import java.util.zip.InflaterOutputStream;
import java.util.logging.Level;
import java.util.logging.Logger;

public class BeatsParser extends ByteToMessageDecoder {
  private static final Logger logger = Logger.getLogger(BeatsParser.class.getCanonicalName());

  private Batch batch;

  private enum States {
    READ_HEADER(1),
    READ_FRAME_TYPE(1),
    READ_WINDOW_SIZE(4),
    READ_JSON_HEADER(8),
    READ_COMPRESSED_FRAME_HEADER(4),
    READ_COMPRESSED_FRAME(
        -1), // -1 means the length to read is variable and defined in the frame itself.
    READ_JSON(-1),
    READ_DATA_FIELDS_HEADER(8),
    READ_DATA_FIELDS(-1);

    private int length;

    States(int length) {
      this.length = length;
    }
  }

  private static final int MAX_FIELDS_COUNT = 1024;
  private static final int MAX_FIELD_LENGTH = 1024;
  private static final int MAX_DATA_LENGTH = 1024 * 1024;
  private static final int MAX_JSON_PAYLOAD_SIZE = 5 * 1024 * 1024;
  private static final int MAX_COMPRESSED_FRAME_SIZE = 10 * 1024 * 1024;
  // Limit decompressed and aggregate batch size to 100MB to prevent heap exhaustion.
  private static final int MAX_DECOMPRESSED_FRAME_SIZE = 100 * 1024 * 1024;
  private static final int MAX_BATCH_BYTE_SIZE = 100 * 1024 * 1024;
  private static final int MAX_WINDOW_SIZE = 16384;

  private States currentState = States.READ_HEADER;
  private int compressionLevel = 0; // Counter to track nested compression frames
  private int requiredBytes = 0;
  private int sequence = 0;

  @Override
  protected void decode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out) throws Exception {
    if (!hasEnoughBytes(in)) {
      return;
    }

    switch (currentState) {
      case READ_HEADER:
        {
          logger.finest("Running: READ_HEADER");

          byte currentVersion = in.readByte();
          if (batch == null) {
            if (Protocol.isVersion2(currentVersion)) {
              batch = new V2Batch();
              logger.finest("Frame version 2 detected");
            } else {
              logger.finest("Frame version 1 detected");
              batch = new V1Batch();
            }
          }
          transition(States.READ_FRAME_TYPE);
          break;
        }
      case READ_FRAME_TYPE:
        {
          byte frameType = in.readByte();

          switch (frameType) {
            case Protocol.CODE_WINDOW_SIZE:
              {
                transition(States.READ_WINDOW_SIZE);
                break;
              }
            case Protocol.CODE_JSON_FRAME:
              {
                // Reading Sequence + size of the payload
                transition(States.READ_JSON_HEADER);
                break;
              }
            case Protocol.CODE_COMPRESSED_FRAME:
              {
                // Prevent nested compression frames to avoid decompression bomb attacks.
                if (compressionLevel > 0) {
                  throw new InvalidFrameProtocolException("Nested compressed frames are not allowed");
                }
                transition(States.READ_COMPRESSED_FRAME_HEADER);
                break;
              }
            case Protocol.CODE_FRAME:
              {
                // Ensure 8-byte header (sequence + fieldsCount) is present before reading.
                transition(States.READ_DATA_FIELDS_HEADER);
                break;
              }
            default:
              {
                throw new InvalidFrameProtocolException(
                    "Invalid Frame Type, received: " + frameType);
              }
          }
          break;
        }
      case READ_WINDOW_SIZE:
        {
          logger.finest("Running: READ_WINDOW_SIZE");
          int windowSize = safeReadUnsignedInt(in, MAX_WINDOW_SIZE, "window size");
          batch.setBatchSize(windowSize);

          // This is unlikely to happen but I have no way to known when a frame is
          // actually completely done other than checking the windows and the sequence
          // number,
          // If the FSM read a new window and I have still
          // events buffered I should send the current batch down to the next handler.
          if (!batch.isEmpty()) {
            logger.warning(
                "New window size received but the current batch was not complete, sending the current batch");
            out.add(batch);
            batchComplete();
          }

          transition(States.READ_HEADER);
          break;
        }
      case READ_DATA_FIELDS_HEADER:
        {
          logger.finest("Running: READ_DATA_FIELDS_HEADER");
          sequence = (int) in.readUnsignedInt();
          int fieldsCount = safeReadUnsignedInt(in, MAX_FIELDS_COUNT, "number of fields");
          transition(States.READ_DATA_FIELDS, fieldsCount);
          break;
        }
      case READ_DATA_FIELDS:
        {
          // Lumberjack version 1 protocol, which use the Key:Value format.
          logger.finest("Running: READ_DATA_FIELDS");
          // Retrieve fieldsCount which was already validated in the READ_DATA_FIELDS_HEADER state.
          int fieldsCount = requiredBytes;
          int count = 0;

          Map dataMap = new HashMap<String, String>(fieldsCount);

          while (count < fieldsCount) {
            int fieldLength = safeReadUnsignedInt(in, MAX_FIELD_LENGTH, "field length");
            ByteBuf fieldBuf = in.readBytes(fieldLength);
            String field = fieldBuf.toString(Charset.forName("UTF8"));
            fieldBuf.release();

            int dataLength = safeReadUnsignedInt(in, MAX_DATA_LENGTH, "data length");
            ByteBuf dataBuf = in.readBytes(dataLength);
            String data = dataBuf.toString(Charset.forName("UTF8"));
            dataBuf.release();

            dataMap.put(field, data);

            count++;
          }
          Message message = new Message(sequence, dataMap);
          ((V1Batch) batch).addMessage(message);

          if (batch.isComplete()) {
            out.add(batch);
            batchComplete();
          }
          transition(States.READ_HEADER);

          break;
        }
      case READ_JSON_HEADER:
        {
          logger.finest("Running: READ_JSON_HEADER");

          sequence = (int) in.readUnsignedInt();
          int jsonPayloadSize = safeReadUnsignedInt(in, MAX_JSON_PAYLOAD_SIZE, "json length");

          transition(States.READ_JSON, jsonPayloadSize);
          break;
        }
      case READ_COMPRESSED_FRAME_HEADER:
        {
          logger.finest("Running: READ_COMPRESSED_FRAME_HEADER");

          int compressedFrameSize =
              safeReadUnsignedInt(in, MAX_COMPRESSED_FRAME_SIZE, "compressed frame length");
          transition(States.READ_COMPRESSED_FRAME, compressedFrameSize);
          break;
        }

      case READ_COMPRESSED_FRAME:
        {
          logger.finest("Running: READ_COMPRESSED_FRAME");
          // Limit decompressed output size to prevent heap exhaustion.
          ByteBuf buffer = ctx.alloc().buffer(requiredBytes, MAX_DECOMPRESSED_FRAME_SIZE);
          compressionLevel++;
          Inflater inflater = new Inflater();
          try (ByteBufOutputStream buffOutput = new ByteBufOutputStream(buffer);
              InflaterOutputStream ios = new InflaterOutputStream(buffOutput, inflater)) {
            in.readBytes(ios, requiredBytes);
            transition(States.READ_HEADER);
            try {
              while (buffer.readableBytes() > 0) {
                decode(ctx, buffer, out);
              }
            } finally {
              buffer.release();
            }
          } finally {
            compressionLevel--;
            // Explicitly call end to prevent native memory leaks
            inflater.end();
          }

          break;
        }
      case READ_JSON:
        {
          logger.finest("Running: READ_JSON");
          // Prevent aggregate batch size from exceeding memory limits.
          if (batch instanceof V2Batch && ((V2Batch) batch).byteSize() + requiredBytes > MAX_BATCH_BYTE_SIZE) {
            throw new InvalidFrameProtocolException("Batch size exceeds maximum limit of " + MAX_BATCH_BYTE_SIZE + " bytes");
          }
          ((V2Batch) batch).addMessage(sequence, in, requiredBytes);
          if (batch.isComplete()) {
            if (logger.isLoggable(Level.FINEST)) {
              logger.finest(
                  "Sending batch size: "
                      + this.batch.size()
                      + ", windowSize: "
                      + batch.getBatchSize()
                      + " , seq: "
                      + sequence);
            }
            out.add(batch);
            batchComplete();
          }

          transition(States.READ_HEADER);
          break;
        }
    }
  }

  private boolean hasEnoughBytes(ByteBuf in) {
    return in.readableBytes() >= requiredBytes;
  }

  private void transition(States next) {
    transition(next, next.length);
  }

  private void transition(States nextState, int requiredBytes) {
    if (logger.isLoggable(Level.FINEST)) {
      logger.finest(
          "Transition, from: "
              + currentState
              + ", to: "
              + nextState
              + ", requiring "
              + requiredBytes
              + " bytes");
    }
    this.currentState = nextState;
    this.requiredBytes = requiredBytes;
  }

  private void batchComplete() {
    requiredBytes = 0;
    sequence = 0;
    batch = null;
  }

  private int safeReadUnsignedInt(ByteBuf in, int max, String fieldName)
      throws InvalidFrameProtocolException {
    int value = (int) in.readUnsignedInt();
    if (value <= 0 || value > max) {
      throw new InvalidFrameProtocolException(
          "Invalid " + fieldName + ", received: " + value + " (max: " + max + ")");
    }
    return value;
  }

  public class InvalidFrameProtocolException extends Exception {
    InvalidFrameProtocolException(String message) {
      super(message);
    }
  }
}
