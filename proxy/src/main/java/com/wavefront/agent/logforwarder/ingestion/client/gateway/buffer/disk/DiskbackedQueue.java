package com.wavefront.agent.logforwarder.ingestion.client.gateway.buffer.disk;

/**
 * @author Manoj Ramakrishnan (rmanoj@vmware.com).
 * @since 9/9/21 11:13 AM
 */

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.logging.Logger;
import com.wavefront.agent.logforwarder.ingestion.client.gateway.buffer.disk.QueueFile.Builder;


public final class DiskbackedQueue<T> {
  public static final long DEFAULT_MAX_FILE_LENGTH_BYTES = 1073741824L;
  public static final String PROPERTY_NAME_MAX_QUEUE_FILE_LENGTH_BYTES = "lemans.queueFile.maxFileLengthBytes";
  public static final String DEFAULT_QUEUE_FILE_NAME = "buffered-requests";
  final QueueFile queueFile;
  private QueueBuffer queueBuffer;
  private final DiskbackedQueue.Converter<T> converter;
  private final long maxFileLengthBytes;
  private static final Logger logger = Logger.getLogger(DiskbackedQueue.class.getSimpleName());

  public DiskbackedQueue(File parentDir, DiskbackedQueue.Converter<T> converter) throws IOException {
    this(parentDir, converter, Long.getLong("lemans.queueFile.maxFileLengthBytes", 1073741824L), "buffered-requests");
  }

  DiskbackedQueue(File parentDir, DiskbackedQueue.Converter<T> converter, long maxFileLengthBytes, String queueFileName) throws IOException {
    validateFileSize(maxFileLengthBytes);
    File f = new File(parentDir, queueFileName);
    this.queueFile = (new Builder(f)).forceLegacy(false).zero(false).build();
    this.maxFileLengthBytes = maxFileLengthBytes;
    this.queueBuffer = new DiskbackedQueue.QueueBuffer(this.maxFileLengthBytes, this.queueFile.usedBytes());
    this.converter = converter;
    logger.info(String.format("queueFileName = %s, Full path: %s", queueFileName, f.getAbsolutePath()));
    logger.info(String.format("maxFileLengthBytes = %s", this.maxFileLengthBytes));
  }

  public boolean add(T entry) throws IOException {
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    this.converter.toBytes(entry, out);
    return this.queueBuffer.add(out.toByteArray());
  }

  public void flushToDisk() throws IOException {
    while(!this.queueBuffer.isEmpty()) {
      byte[] entry = this.queueBuffer.peek();
      this.addToQueueFile(entry);
      this.queueBuffer.remove();
    }

  }

  public List<T> peek(int numElements) {
    List<T> elements = new ArrayList();
    Iterator<byte[]> iterator = this.queueFile.iterator();

    for(int i = 0; i < numElements && iterator.hasNext(); ++i) {
      T elem = this.converter.fromBytes((byte[])iterator.next());
      elements.add(elem);
    }

    return elements;
  }

  public void remove(int numElements) throws IOException {
    this.removeFromQueueFile(numElements);
  }

  public int size() {
    return this.queueFile.size();
  }

  public boolean isEmpty() {
    return this.queueFile.isEmpty();
  }

  public void close() throws IOException {
    this.queueFile.close();
  }

  public int bufferSize() {
    return this.queueBuffer.size();
  }

  private void addToQueueFile(byte[] entry) throws IOException {
    this.queueFile.add(entry);
    this.queueBuffer.setQueueFileBytesUsed(this.queueFile.usedBytes());
  }

  private void removeFromQueueFile(int numElements) throws IOException {
    this.queueFile.remove(numElements);
    this.queueBuffer.setQueueFileBytesUsed(this.queueFile.usedBytes());
  }

  private static void validateFileSize(long fileSize) {
    if (fileSize < 4096L) {
      throw new IllegalArgumentException("Queue file size must be at least 4096");
    } else if ((fileSize & fileSize - 1L) != 0L) {
      throw new IllegalArgumentException(String.format("Queue file size %s must be power of 2", fileSize));
    }
  }

  private static class QueueBuffer {
    private Queue<byte[]> buffer = new LinkedList();
    private long bufferLengthBytes = 0L;
    private long queueFileBytesUsed;
    private final long maxQueueFileLengthBytes;

    QueueBuffer(long maxQueueFileLengthBytes, long bytesUsed) {
      this.maxQueueFileLengthBytes = maxQueueFileLengthBytes;
      this.queueFileBytesUsed = bytesUsed;
    }

    synchronized boolean add(byte[] elem) {
      if (elem == null) {
        throw new IllegalArgumentException("null is not allowed in the queue");
      } else if (!this.canFitElement(elem.length)) {
        return false;
      } else {
        this.bufferLengthBytes += this.getTotalElemLength(elem.length);
        return this.buffer.add(elem);
      }
    }

    synchronized byte[] remove() {
      byte[] elem = (byte[])this.buffer.remove();
      this.bufferLengthBytes -= this.getTotalElemLength(elem.length);
      return elem;
    }

    synchronized byte[] peek() {
      return (byte[])this.buffer.peek();
    }

    synchronized boolean isEmpty() {
      return this.buffer.isEmpty();
    }

    synchronized void setQueueFileBytesUsed(long bytesUsed) {
      this.queueFileBytesUsed = bytesUsed;
    }

    synchronized int size() {
      return this.buffer.size();
    }

    private boolean canFitElement(int elemLength) {
      long actualElemLength = this.getTotalElemLength(elemLength);
      long remainingBytes = this.maxQueueFileLengthBytes - (this.queueFileBytesUsed + this.bufferLengthBytes);
      return actualElemLength < remainingBytes;
    }

    private long getTotalElemLength(int elemLength) {
      return (long)(elemLength + 4);
    }
  }

  public interface Converter<T> {
    T fromBytes(byte[] var1);

    void toBytes(T var1, OutputStream var2) throws IOException;
  }
}
