package com.wavefront.agent.channel;

import com.google.common.base.Charsets;

import java.util.List;
import java.util.logging.Logger;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPipeline;
import io.netty.handler.codec.ByteToMessageDecoder;
import io.netty.handler.codec.compression.ZlibCodecFactory;
import io.netty.handler.codec.compression.ZlibWrapper;
import io.netty.handler.codec.http.HttpContentDecompressor;
import io.netty.handler.codec.http.HttpObjectAggregator;
import io.netty.handler.codec.http.HttpRequestDecoder;
import io.netty.handler.codec.http.HttpResponseEncoder;
import io.netty.handler.codec.string.StringDecoder;
import io.netty.handler.codec.string.StringEncoder;

/**
 * This class handles 2 different protocols on a single port.  Supported protocols include HTTP and
 * a plain text protocol.  It dynamically adds the appropriate encoder/decoder based on the detected
 * protocol. This class was largely adapted from the example provided with netty v4.0
 *
 * @see <a href="http://netty.io/4.0/xref/io/netty/example/portunification/PortUnificationServerHandler.html">Netty
 * Port Unification Example</a>
 * @author Mike McLaughlin (mike@wavefront.com)
 */
public final class PlainTextOrHttpFrameDecoder extends ByteToMessageDecoder {

  protected static final Logger logger = Logger.getLogger(
      PlainTextOrHttpFrameDecoder.class.getName());

  /**
   * The object for handling requests of either protocol
   */
  private final ChannelHandler handler;
  private final boolean detectGzip;
  private final int maxLengthPlaintext;
  private final int maxLengthHttp;

  private static final StringDecoder STRING_DECODER = new StringDecoder(Charsets.UTF_8);
  private static final StringEncoder STRING_ENCODER = new StringEncoder(Charsets.UTF_8);

  /**
   * @param handler            the object responsible for handling the incoming messages on
   *                           either protocol.
   * @param maxLengthPlaintext max allowed line length for line-delimiter protocol
   * @param maxLengthHttp      max allowed size for incoming HTTP requests
   */
  public PlainTextOrHttpFrameDecoder(final ChannelHandler handler,
                                     int maxLengthPlaintext,
                                     int maxLengthHttp) {
    this(handler, maxLengthPlaintext, maxLengthHttp, true);
  }

  private PlainTextOrHttpFrameDecoder(final ChannelHandler handler, int maxLengthPlaintext,
                                      int maxLengthHttp, boolean detectGzip) {
    this.handler = handler;
    this.maxLengthPlaintext = maxLengthPlaintext;
    this.maxLengthHttp = maxLengthHttp;
    this.detectGzip = detectGzip;
  }

  /**
   * Dynamically adds the appropriate encoder/decoder(s) to the pipeline based on the detected
   * protocol.
   */
  @Override
  protected void decode(final ChannelHandlerContext ctx, final ByteBuf buffer, List<Object> out) {
    // read the first 2 bytes to use for protocol detection
    if (buffer.readableBytes() < 2) {
      logger.info("Inbound data from " + ctx.channel().remoteAddress() +
          " has less that 2 readable bytes - ignoring");
      return;
    }
    final int firstByte = buffer.getUnsignedByte(buffer.readerIndex());
    final int secondByte = buffer.getUnsignedByte(buffer.readerIndex() + 1);

    // determine the protocol and add the encoder/decoder
    final ChannelPipeline pipeline = ctx.pipeline();

    if (detectGzip && isGzip(firstByte, secondByte)) {
      logger.fine("Inbound gzip stream detected");
      pipeline.
          addLast("gzipdeflater", ZlibCodecFactory.newZlibEncoder(ZlibWrapper.GZIP)).
          addLast("gzipinflater", ZlibCodecFactory.newZlibDecoder(ZlibWrapper.GZIP)).
          addLast("unificationB", new PlainTextOrHttpFrameDecoder(handler, maxLengthPlaintext,
              maxLengthHttp, false));
    } else if (isHttp(firstByte, secondByte)) {
      logger.fine("Switching to HTTP protocol");
      pipeline.
          addLast("decoder", new HttpRequestDecoder()).
          addLast("inflater", new HttpContentDecompressor()).
          addLast("encoder", new HttpResponseEncoder()).
          addLast("aggregator", new StatusTrackingHttpObjectAggregator(maxLengthHttp)).
          addLast("handler", this.handler);
    } else {
      logger.fine("Switching to plaintext TCP protocol");
      pipeline.
          addLast("line", new IncompleteLineDetectingLineBasedFrameDecoder(logger::warning,
              maxLengthPlaintext)).
          addLast("decoder", STRING_DECODER).
          addLast("encoder", STRING_ENCODER).
          addLast("handler", this.handler);
    }
    pipeline.remove(this);
  }

  /**
   * @param magic1 the first byte of the incoming message
   * @param magic2 the second byte of the incoming message
   * @return true if this is an HTTP message; false o/w
   * @see <a href="http://netty.io/4.0/xref/io/netty/example/portunification/PortUnificationServerHandler.html">Netty
   * Port Unification Example</a>
   */
  private static boolean isHttp(int magic1, int magic2) {
    return
        ((magic1 == 'G' && magic2 == 'E') || // GET
            (magic1 == 'P' && magic2 == 'O') || // POST
            (magic1 == 'P' && magic2 == 'U') || // PUT
            (magic1 == 'H' && magic2 == 'E') || // HEAD
            (magic1 == 'O' && magic2 == 'P') || // OPTIONS
            (magic1 == 'P' && magic2 == 'A') || // PATCH
            (magic1 == 'D' && magic2 == 'E') || // DELETE
            (magic1 == 'T' && magic2 == 'R') || // TRACE
            (magic1 == 'C' && magic2 == 'O'));  // CONNECT
  }

  /**
   * @param magic1 the first byte of the incoming message
   * @param magic2 the second byte of the incoming message
   * @return true if this is a GZIP stream; false o/w
   * @see <a href="http://netty.io/4.0/xref/io/netty/example/portunification/PortUnificationServerHandler.html">Netty
   * Port Unification Example</a>
   */
  private static boolean isGzip(int magic1, int magic2) {
    return magic1 == 31 && magic2 == 139;
  }

}
