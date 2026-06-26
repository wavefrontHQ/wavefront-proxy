package org.logstash.beats;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.embedded.EmbeddedChannel;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;
import java.util.zip.DeflaterOutputStream;

import static org.junit.Assert.*;

public class BeatsParserTest {

  private EmbeddedChannel channel;

  @Before
  public void setUp() {
    channel = new EmbeddedChannel(new BeatsParser());
  }

  @After
  public void tearDown() {
    channel.finishAndReleaseAll();
  }

  // READ_HEADER / READ_FRAME_TYPE

  /*
   * Test V2Batch creation upon version 2 detection.
   */
  @Test
  public void testVersion2BatchCreated() {
    ByteBuf buf = buildV2SingleJsonFrame(1, 1, "{\"message\":\"hello\"}");
    channel.writeInbound(buf);
    Object out = channel.readInbound();
    assertNotNull("Expected a V2Batch to be emitted", out);
    assertTrue(out instanceof V2Batch);
    ((V2Batch) out).release();
  }

  /*
   * Test V1Batch creation upon version 1 detection.
   */
  @Test
  public void testVersion1BatchCreated() {
    String key = "message";
    String value = "hello";
    byte[] keyBytes = key.getBytes(StandardCharsets.UTF_8);
    byte[] valBytes = value.getBytes(StandardCharsets.UTF_8);
    ByteBuf buf = Unpooled.buffer();
    // Window frame (version 1)
    buf.writeByte(Protocol.VERSION_1);
    buf.writeByte(Protocol.CODE_WINDOW_SIZE);
    buf.writeInt(1);
    // Data frame: version=1, type='D', sequence=1, fieldsCount=1, key, value
    buf.writeByte(Protocol.VERSION_1);
    buf.writeByte(Protocol.CODE_FRAME);
    buf.writeInt(1);      // sequence
    buf.writeInt(1);      // fields count
    buf.writeInt(keyBytes.length);
    buf.writeBytes(keyBytes);
    buf.writeInt(valBytes.length);
    buf.writeBytes(valBytes);
    channel.writeInbound(buf);
    Object out = channel.readInbound();
    assertNotNull("Expected a V1Batch to be emitted", out);
    assertTrue(out instanceof V1Batch);
  }

  /*
   * Tests unrecognised frame type byte throws InvalidFrameProtocolException.
   */
  @Test
  public void testInvalidFrameTypeRejected() {
    ByteBuf buf = Unpooled.buffer();
    buf.writeByte(Protocol.VERSION_2);
    buf.writeByte((byte) 'X'); // unknown frame type
    try {
      channel.writeInbound(buf);
      fail("Expected exception for invalid frame type");
    } catch (Exception e) {
      assertTrue(e.getMessage().contains("Invalid Frame Type"));
    }
  }

  // READ_WINDOW_SIZE

  /*
   * Tests window size from the wire is stored on the batch as its capacity.
   */
  @Test
  public void testWindowSizeSetOnBatch() {
    // windowSize=1 so the single JSON frame completes the batch, and it gets emitted.
    ByteBuf buf = buildV2SingleJsonFrame(1, 1, "{\"a\":\"b\"}");
    channel.writeInbound(buf);
    V2Batch batch = channel.readInbound();
    assertNotNull(batch);
    assertEquals(1, batch.getBatchSize());
    batch.release();
  }

  /*
   * Test new window frame flushes a partial batch that has not yet reached its declared size.
   */
  @Test
  public void testNewWindowFlushesIncompleteExistingBatch() {
    String json = "{\"x\":\"y\"}";
    byte[] jsonBytes = json.getBytes(StandardCharsets.UTF_8);
    ByteBuf buf = Unpooled.buffer();
    // First window: size=2
    buf.writeByte(Protocol.VERSION_2);
    buf.writeByte(Protocol.CODE_WINDOW_SIZE);
    buf.writeInt(2);
    // One JSON frame (incomplete — window expects 2)
    buf.writeByte(Protocol.VERSION_2);
    buf.writeByte(Protocol.CODE_JSON_FRAME);
    buf.writeInt(1);
    buf.writeInt(jsonBytes.length);
    buf.writeBytes(jsonBytes);
    // Second window header triggers flush of the incomplete first batch.
    buf.writeByte(Protocol.VERSION_2);
    buf.writeByte(Protocol.CODE_WINDOW_SIZE);
    buf.writeInt(1);
    channel.writeInbound(buf);
    // The incomplete first batch (1 of 2 frames) must have been flushed.
    V2Batch first = channel.readInbound();
    assertNotNull("Partial batch should have been flushed on new window", first);
    assertEquals(1, first.size());
    first.release();
    // The new batch has batchSize=0 until another window frame arrives; nothing more emitted.
    assertNull(channel.readInbound());
  }

  /*
   * Tests window size of zero is rejected by bounds check.
   */
  @Test
  public void testWindowSizeZeroRejected() {
    ByteBuf buf = Unpooled.buffer();
    buf.writeByte(Protocol.VERSION_2);
    buf.writeByte(Protocol.CODE_WINDOW_SIZE);
    buf.writeInt(0); // invalid: must be > 0
    try {
      channel.writeInbound(buf);
      fail("Expected exception for zero window size");
    } catch (Exception e) {
      assertTrue(e.getMessage().contains("window size"));
    }
  }

  /*
   * Test window size above MAX_WINDOW_SIZE (16384) is rejected by bounds check.
   */
  @Test
  public void testWindowSizeExceedsMaxRejected() {
    ByteBuf buf = Unpooled.buffer();
    buf.writeByte(Protocol.VERSION_2);
    buf.writeByte(Protocol.CODE_WINDOW_SIZE);
    buf.writeInt(16385); // MAX_WINDOW_SIZE is 16384
    try {
      channel.writeInbound(buf);
      fail("Expected exception for window size exceeding max");
    } catch (Exception e) {
      assertTrue(e.getMessage().contains("window size"));
    }
  }

  // READ_JSON_HEADER / READ_JSON

  /*
   * Tests single JSON frame is decoded and its sequence number is recorded on the batch.
   */
  @Test
  public void testJsonFrameParsed() {
    String json = "{\"message\":\"test\"}";
    ByteBuf buf = buildV2SingleJsonFrame(1, 42, json);
    channel.writeInbound(buf);
    V2Batch batch = channel.readInbound();
    assertNotNull(batch);
    assertEquals(1, batch.size());
    assertEquals(42, batch.getHighestSequence());
    batch.release();
  }

  /*
   * Tests multiple JSON frames accumulate in one batch and are emitted together when the window is full.
   */
  @Test
  public void testMultipleJsonFramesInOneBatch() {
    String json = "{\"k\":\"v\"}";
    byte[] jsonBytes = json.getBytes(StandardCharsets.UTF_8);
    ByteBuf buf = Unpooled.buffer();
    buf.writeByte(Protocol.VERSION_2);
    buf.writeByte(Protocol.CODE_WINDOW_SIZE);
    buf.writeInt(3);
    for (int seq = 1; seq <= 3; seq++) {
      buf.writeByte(Protocol.VERSION_2);
      buf.writeByte(Protocol.CODE_JSON_FRAME);
      buf.writeInt(seq);
      buf.writeInt(jsonBytes.length);
      buf.writeBytes(jsonBytes);
    }
    channel.writeInbound(buf);
    V2Batch batch = channel.readInbound();
    assertNotNull(batch);
    assertEquals(3, batch.size());
    assertEquals(3, batch.getHighestSequence());
    batch.release();
  }

  /*
   * Tests JSON payload length exceeding MAX_JSON_PAYLOAD_SIZE (5MB) is rejected.
   */
  @Test
  public void testJsonPayloadSizeExceedsMaxRejected() {
    ByteBuf buf = Unpooled.buffer();
    buf.writeByte(Protocol.VERSION_2);
    buf.writeByte(Protocol.CODE_WINDOW_SIZE);
    buf.writeInt(1);
    buf.writeByte(Protocol.VERSION_2);
    buf.writeByte(Protocol.CODE_JSON_FRAME);
    buf.writeInt(1);
    buf.writeInt(5 * 1024 * 1024 + 1); // exceeds MAX_JSON_PAYLOAD_SIZE (5MB)
    try {
      channel.writeInbound(buf);
      fail("Expected exception for oversized JSON payload");
    } catch (Exception e) {
      assertTrue(e.getMessage().contains("json length"));
    }
  }

  // READ_COMPRESSED_FRAME_HEADER / READ_COMPRESSED_FRAME

  /*
   * Tests compressed frame is decompressed and inner frames parsed into a valid batch.
   */
  @Test
  public void testCompressedFrameDecompressedAndParsed() throws Exception {
    // Build the inner (uncompressed) content: window + JSON frame
    ByteBuf inner = buildV2SingleJsonFrame(1, 1, "{\"compressed\":\"yes\"}");
    byte[] innerBytes = new byte[inner.readableBytes()];
    inner.readBytes(innerBytes);
    inner.release();
    byte[] compressed = compress(innerBytes);

    ByteBuf buf = Unpooled.buffer();
    // Outer window
    buf.writeByte(Protocol.VERSION_2);
    buf.writeByte(Protocol.CODE_WINDOW_SIZE);
    buf.writeInt(1);
    // Compressed frame
    buf.writeByte(Protocol.VERSION_2);
    buf.writeByte(Protocol.CODE_COMPRESSED_FRAME);
    buf.writeInt(compressed.length);
    buf.writeBytes(compressed);

    channel.writeInbound(buf);
    V2Batch batch = channel.readInbound();
    assertNotNull("Compressed frame should yield a batch", batch);
    assertEquals(1, batch.size());
    batch.release();
  }

  /*
   * Tests compressed frame header declaring size above MAX_COMPRESSED_FRAME_SIZE (10MB) is rejected.
   */
  @Test
  public void testCompressedFrameSizeExceedsMaxRejected() {
    ByteBuf buf = Unpooled.buffer();
    buf.writeByte(Protocol.VERSION_2);
    buf.writeByte(Protocol.CODE_WINDOW_SIZE);
    buf.writeInt(1);
    buf.writeByte(Protocol.VERSION_2);
    buf.writeByte(Protocol.CODE_COMPRESSED_FRAME);
    buf.writeInt(10 * 1024 * 1024 + 1); // exceeds MAX_COMPRESSED_FRAME_SIZE (10MB)
    try {
      channel.writeInbound(buf);
      fail("Expected exception for oversized compressed frame");
    } catch (Exception e) {
      assertTrue(e.getMessage().contains("compressed frame length"));
    }
  }

  /*
   * Tests compressed frame containing another compressed frame is rejected to prevent decompression bomb attacks.
   */
  @Test
  public void testNestedCompressedFrameRejected() throws Exception {
    // Innermost content: a normal window + JSON frame
    ByteBuf innermost = buildV2SingleJsonFrame(1, 1, "{\"x\":\"y\"}");
    byte[] innermostBytes = new byte[innermost.readableBytes()];
    innermost.readBytes(innermostBytes);
    innermost.release();

    // Wrap innermostBytes in a compressed frame
    ByteBuf middleContent = Unpooled.buffer();
    middleContent.writeByte(Protocol.VERSION_2);
    middleContent.writeByte(Protocol.CODE_COMPRESSED_FRAME);
    byte[] innerCompressed = compress(innermostBytes);
    middleContent.writeInt(innerCompressed.length);
    middleContent.writeBytes(innerCompressed);
    byte[] middleBytes = new byte[middleContent.readableBytes()];
    middleContent.readBytes(middleBytes);
    middleContent.release();

    byte[] outerCompressed = compress(middleBytes);

    ByteBuf buf = Unpooled.buffer();
    buf.writeByte(Protocol.VERSION_2);
    buf.writeByte(Protocol.CODE_WINDOW_SIZE);
    buf.writeInt(1);
    buf.writeByte(Protocol.VERSION_2);
    buf.writeByte(Protocol.CODE_COMPRESSED_FRAME);
    buf.writeInt(outerCompressed.length);
    buf.writeBytes(outerCompressed);

    try {
      channel.writeInbound(buf);
      fail("Expected an exception for nested compressed frames");
    } catch (Exception e) {
      assertTrue(e.getMessage().contains("Nested compressed frames"));
    }
  }

  // READ_DATA_FIELDS_HEADER / READ_DATA_FIELDS

  /*
   * Tests v1 key-value data frame is decoded and field values are accessible on the Message.
   */
  @Test
  public void testV1DataFrameKeyValueParsed() {
    String key = "host";
    String value = "myhost.example.com";
    byte[] keyBytes = key.getBytes(StandardCharsets.UTF_8);
    byte[] valBytes = value.getBytes(StandardCharsets.UTF_8);
    ByteBuf buf = Unpooled.buffer();
    buf.writeByte(Protocol.VERSION_1);
    buf.writeByte(Protocol.CODE_WINDOW_SIZE);
    buf.writeInt(1);
    buf.writeByte(Protocol.VERSION_1);
    buf.writeByte(Protocol.CODE_FRAME);
    buf.writeInt(7);      // sequence
    buf.writeInt(1);      // 1 field
    buf.writeInt(keyBytes.length);
    buf.writeBytes(keyBytes);
    buf.writeInt(valBytes.length);
    buf.writeBytes(valBytes);
    channel.writeInbound(buf);
    V1Batch batch = channel.readInbound();
    assertNotNull(batch);
    Message msg = batch.iterator().next();
    assertEquals(7, msg.getSequence());
    assertEquals("myhost.example.com", msg.getData().get("host"));
  }

  /*
   * Tests v1 data frame declaring fieldsCount of zero is rejected by the bounds check.
   */
  @Test
  public void testFieldCountZeroRejected() {
    ByteBuf buf = Unpooled.buffer();
    buf.writeByte(Protocol.VERSION_1);
    buf.writeByte(Protocol.CODE_WINDOW_SIZE);
    buf.writeInt(1);
    buf.writeByte(Protocol.VERSION_1);
    buf.writeByte(Protocol.CODE_FRAME);
    buf.writeInt(1);  // sequence
    buf.writeInt(0);  // fieldsCount = 0, invalid
    try {
      channel.writeInbound(buf);
      fail("Expected exception for zero fields count");
    } catch (Exception e) {
      assertTrue(e.getMessage().contains("number of fields"));
    }
  }

  /*
   * Tests v1 data frame declaring more than MAX_FIELDS_COUNT (1024) fields is rejected.
   */
  @Test
  public void testFieldCountExceedsMaxRejected() {
    ByteBuf buf = Unpooled.buffer();
    buf.writeByte(Protocol.VERSION_1);
    buf.writeByte(Protocol.CODE_WINDOW_SIZE);
    buf.writeInt(1);
    buf.writeByte(Protocol.VERSION_1);
    buf.writeByte(Protocol.CODE_FRAME);
    buf.writeInt(1);       // sequence
    buf.writeInt(1025);    // MAX_FIELDS_COUNT is 1024
    try {
      channel.writeInbound(buf);
      fail("Expected exception for too many fields");
    } catch (Exception e) {
      assertTrue(e.getMessage().contains("number of fields"));
    }
  }

  /*
   * Tests v1 data frame with a field key length exceeding MAX_FIELD_LENGTH (1024) is rejected.
   */
  @Test
  public void testFieldLengthExceedsMaxRejected() {
    ByteBuf buf = Unpooled.buffer();
    buf.writeByte(Protocol.VERSION_1);
    buf.writeByte(Protocol.CODE_WINDOW_SIZE);
    buf.writeInt(1);
    buf.writeByte(Protocol.VERSION_1);
    buf.writeByte(Protocol.CODE_FRAME);
    buf.writeInt(1);     // sequence
    buf.writeInt(1);     // 1 field
    buf.writeInt(1025);  // fieldLength exceeds MAX_FIELD_LENGTH (1024)
    try {
      channel.writeInbound(buf);
      fail("Expected exception for oversized field length");
    } catch (Exception e) {
      assertTrue(e.getMessage().contains("field length"));
    }
  }

  /*
   * Tests v1 data frame with a field value length exceeding MAX_DATA_LENGTH (1MB) is rejected.
   */
  @Test
  public void testDataLengthExceedsMaxRejected() {
    String key = "k";
    byte[] keyBytes = key.getBytes(StandardCharsets.UTF_8);
    ByteBuf buf = Unpooled.buffer();
    buf.writeByte(Protocol.VERSION_1);
    buf.writeByte(Protocol.CODE_WINDOW_SIZE);
    buf.writeInt(1);
    buf.writeByte(Protocol.VERSION_1);
    buf.writeByte(Protocol.CODE_FRAME);
    buf.writeInt(1);                    // sequence
    buf.writeInt(1);                    // 1 field
    buf.writeInt(keyBytes.length);
    buf.writeBytes(keyBytes);
    buf.writeInt(1024 * 1024 + 1);      // dataLength exceeds MAX_DATA_LENGTH (1MB)
    try {
      channel.writeInbound(buf);
      fail("Expected exception for oversized data length");
    } catch (Exception e) {
      assertTrue(e.getMessage().contains("data length"));
    }
  }

  /*
   * Tests decoder emits nothing when bytes available are fewer than the next state requires.
   */
  @Test
  public void testPartialFrameDoesNotEmitBatch() {
    ByteBuf buf = Unpooled.buffer();
    buf.writeByte(Protocol.VERSION_2);
    buf.writeByte(Protocol.CODE_WINDOW_SIZE);
    buf.writeInt(1);
    channel.writeInbound(buf);
    assertNull("No batch should be emitted for incomplete input", channel.readInbound());
  }

  /*
   * Tests frame split across two delivery chunks is reassembled correctly into a complete batch.
   */
  @Test
  public void testChunkedDeliveryReassembled() {
    String json = "{\"chunked\":true}";
    byte[] jsonBytes = json.getBytes(StandardCharsets.UTF_8);

    ByteBuf full = Unpooled.buffer();
    full.writeByte(Protocol.VERSION_2);
    full.writeByte(Protocol.CODE_WINDOW_SIZE);
    full.writeInt(1);
    full.writeByte(Protocol.VERSION_2);
    full.writeByte(Protocol.CODE_JSON_FRAME);
    full.writeInt(1);
    full.writeInt(jsonBytes.length);
    full.writeBytes(jsonBytes);

    // Split into two chunks and deliver separately
    int split = full.readableBytes() / 2;
    ByteBuf chunk1 = full.readSlice(split).retain();
    ByteBuf chunk2 = full.retain();
    full.release();

    channel.writeInbound(chunk1);
    assertNull("No batch yet after first chunk", channel.readInbound());
    channel.writeInbound(chunk2);
    V2Batch batch = channel.readInbound();
    assertNotNull("Batch should be emitted after second chunk", batch);
    batch.release();
  }


  // Helper Methods

  /** Writes a v2 window-size frame followed by one JSON frame. */
  private ByteBuf buildV2SingleJsonFrame(int windowSize, int sequence, String json) {
    byte[] jsonBytes = json.getBytes(StandardCharsets.UTF_8);
    ByteBuf buf = Unpooled.buffer();
    // Window frame: version=2, type='W', size=windowSize
    buf.writeByte(Protocol.VERSION_2);
    buf.writeByte(Protocol.CODE_WINDOW_SIZE);
    buf.writeInt(windowSize);
    // JSON frame: version=2, type='J', sequence, payload length, payload
    buf.writeByte(Protocol.VERSION_2);
    buf.writeByte(Protocol.CODE_JSON_FRAME);
    buf.writeInt(sequence);
    buf.writeInt(jsonBytes.length);
    buf.writeBytes(jsonBytes);
    return buf;
  }

  /** Compresses bytes with ZLIB (java.util.zip deflate). */
  private byte[] compress(byte[] data) throws Exception {
    ByteArrayOutputStream bos = new ByteArrayOutputStream();
    try (DeflaterOutputStream dos = new DeflaterOutputStream(bos)) {
      dos.write(data);
    }
    return bos.toByteArray();
  }
}
