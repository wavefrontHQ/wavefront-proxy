package com.wavefront.agent.logforwarder.ingestion.client.gateway.buffer.disk;

import java.io.Closeable;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;
import java.util.ConcurrentModificationException;
import java.util.Iterator;
import java.util.NoSuchElementException;
import javax.annotation.Nullable;

/**
 * A File representation of a queue.
 */
public final class QueueFile implements Closeable, Iterable<byte[]> {
    private static final int VERSIONED_HEADER = -2147483647;
    static final int INITIAL_LENGTH = 4096;
    private static final byte[] ZEROES = new byte[4096];
    final RandomAccessFile raf;
    final File file;
    final boolean versioned;
    final int headerLength;
    long fileLength;
    int elementCount;
    private QueueFile.Element first;
    public QueueFile.Element last;
    private final byte[] buffer = new byte[32];
    int modCount = 0;
    private final boolean zero;
    boolean closed;

    static RandomAccessFile initializeFromFile(File file, boolean forceLegacy) throws IOException {
        if (!file.exists()) {
            File tempFile = new File(file.getPath() + ".tmp");
            RandomAccessFile raf = open(tempFile);

            try {
                raf.setLength(4096L);
                raf.seek(0L);
                if (forceLegacy) {
                    raf.writeInt(4096);
                } else {
                    raf.writeInt(-2147483647);
                    raf.writeLong(4096L);
                }
            } finally {
                raf.close();
            }

            if (!tempFile.renameTo(file)) {
                throw new IOException("Rename failed!");
            }
        }

        return open(file);
    }

    private static RandomAccessFile open(File file) throws FileNotFoundException {
        return new RandomAccessFile(file, "rwd");
    }

    QueueFile(File file, RandomAccessFile raf, boolean zero, boolean forceLegacy) throws IOException {
        this.file = file;
        this.raf = raf;
        this.zero = zero;
        raf.seek(0L);
        raf.readFully(this.buffer);
        this.versioned = !forceLegacy && (this.buffer[0] & 128) != 0;
        long firstOffset;
        long lastOffset;
        if (this.versioned) {
            this.headerLength = 32;
            int version = readInt(this.buffer, 0) & 2147483647;
            if (version != 1) {
                throw new IOException("Unable to read version " + version + " format. Supported versions are 1 and legacy.");
            }

            this.fileLength = readLong(this.buffer, 4);
            this.elementCount = readInt(this.buffer, 12);
            firstOffset = readLong(this.buffer, 16);
            lastOffset = readLong(this.buffer, 24);
        } else {
            this.headerLength = 16;
            this.fileLength = (long)readInt(this.buffer, 0);
            this.elementCount = readInt(this.buffer, 4);
            firstOffset = (long)readInt(this.buffer, 8);
            lastOffset = (long)readInt(this.buffer, 12);
        }

        if (this.fileLength > raf.length()) {
            throw new IOException("File is truncated. Expected length: " + this.fileLength + ", Actual length: " + raf.length());
        } else if (this.fileLength <= (long)this.headerLength) {
            throw new IOException("File is corrupt; length stored in header (" + this.fileLength + ") is invalid.");
        } else {
            this.first = this.readElement(firstOffset);
            this.last = this.readElement(lastOffset);
        }
    }

    private static void writeInt(byte[] buffer, int offset, int value) {
        buffer[offset] = (byte)(value >> 24);
        buffer[offset + 1] = (byte)(value >> 16);
        buffer[offset + 2] = (byte)(value >> 8);
        buffer[offset + 3] = (byte)value;
    }

    private static int readInt(byte[] buffer, int offset) {
        return ((buffer[offset] & 255) << 24) + ((buffer[offset + 1] & 255) << 16) + ((buffer[offset + 2] & 255) << 8) + (buffer[offset + 3] & 255);
    }

    private static void writeLong(byte[] buffer, int offset, long value) {
        buffer[offset] = (byte)((int)(value >> 56));
        buffer[offset + 1] = (byte)((int)(value >> 48));
        buffer[offset + 2] = (byte)((int)(value >> 40));
        buffer[offset + 3] = (byte)((int)(value >> 32));
        buffer[offset + 4] = (byte)((int)(value >> 24));
        buffer[offset + 5] = (byte)((int)(value >> 16));
        buffer[offset + 6] = (byte)((int)(value >> 8));
        buffer[offset + 7] = (byte)((int)value);
    }

    private static long readLong(byte[] buffer, int offset) {
        return (((long)buffer[offset] & 255L) << 56) + (((long)buffer[offset + 1] & 255L) << 48) + (((long)buffer[offset + 2] & 255L) << 40) + (((long)buffer[offset + 3] & 255L) << 32) + (((long)buffer[offset + 4] & 255L) << 24) + (((long)buffer[offset + 5] & 255L) << 16) + (((long)buffer[offset + 6] & 255L) << 8) + ((long)buffer[offset + 7] & 255L);
    }

    private void writeHeader(long fileLength, int elementCount, long firstPosition, long lastPosition) throws IOException {
        this.raf.seek(0L);
        if (this.versioned) {
            writeInt(this.buffer, 0, -2147483647);
            writeLong(this.buffer, 4, fileLength);
            writeInt(this.buffer, 12, elementCount);
            writeLong(this.buffer, 16, firstPosition);
            writeLong(this.buffer, 24, lastPosition);
            this.raf.write(this.buffer, 0, 32);
        } else {
            writeInt(this.buffer, 0, (int)fileLength);
            writeInt(this.buffer, 4, elementCount);
            writeInt(this.buffer, 8, (int)firstPosition);
            writeInt(this.buffer, 12, (int)lastPosition);
            this.raf.write(this.buffer, 0, 16);
        }
    }

    QueueFile.Element readElement(long position) throws IOException {
        if (position == 0L) {
            return QueueFile.Element.NULL;
        } else {
            this.ringRead(position, this.buffer, 0, 4);
            int length = readInt(this.buffer, 0);
            return new QueueFile.Element(position, length);
        }
    }

    long wrapPosition(long position) {
        return position < this.fileLength ? position : (long)this.headerLength + position - this.fileLength;
    }

    private void ringWrite(long position, byte[] buffer, int offset, int count) throws IOException {
        position = this.wrapPosition(position);
        if (position + (long)count <= this.fileLength) {
            this.raf.seek(position);
            this.raf.write(buffer, offset, count);
        } else {
            int beforeEof = (int)(this.fileLength - position);
            this.raf.seek(position);
            this.raf.write(buffer, offset, beforeEof);
            this.raf.seek((long)this.headerLength);
            this.raf.write(buffer, offset + beforeEof, count - beforeEof);
        }

    }

    private void ringErase(long position, long length) throws IOException {
        while(length > 0L) {
            int chunk = (int)Math.min(length, (long)ZEROES.length);
            this.ringWrite(position, ZEROES, 0, chunk);
            length -= (long)chunk;
            position += (long)chunk;
        }

    }

    void ringRead(long position, byte[] buffer, int offset, int count) throws IOException {
        position = this.wrapPosition(position);
        if (position + (long)count <= this.fileLength) {
            this.raf.seek(position);
            this.raf.readFully(buffer, offset, count);
        } else {
            int beforeEof = (int)(this.fileLength - position);
            this.raf.seek(position);
            this.raf.readFully(buffer, offset, beforeEof);
            this.raf.seek((long)this.headerLength);
            this.raf.readFully(buffer, offset + beforeEof, count - beforeEof);
        }

    }

    public void add(byte[] data) throws IOException {
        this.add(data, 0, data.length);
    }

    public void add(byte[] data, int offset, int count) throws IOException {
        if (data == null) {
            throw new NullPointerException("data == null");
        } else if ((offset | count) >= 0 && count <= data.length - offset) {
            if (this.closed) {
                throw new IOException("closed");
            } else {
                this.expandIfNecessary((long)count);
                boolean wasEmpty = this.isEmpty();
                long position = wasEmpty ? (long)this.headerLength : this.wrapPosition(this.last.position + 4L + (long)this.last.length);
                QueueFile.Element newLast = new QueueFile.Element(position, count);
                writeInt(this.buffer, 0, count);
                this.ringWrite(newLast.position, this.buffer, 0, 4);
                this.ringWrite(newLast.position + 4L, data, offset, count);
                long firstPosition = wasEmpty ? newLast.position : this.first.position;
                this.writeHeader(this.fileLength, this.elementCount + 1, firstPosition, newLast.position);
                this.last = newLast;
                ++this.elementCount;
                ++this.modCount;
                if (wasEmpty) {
                    this.first = this.last;
                }

            }
        } else {
            throw new IndexOutOfBoundsException();
        }
    }

    public long usedBytes() {
        if (this.elementCount == 0) {
            return (long)this.headerLength;
        } else {
            return this.last.position >= this.first.position ? this.last.position - this.first.position + 4L + (long)this.last.length + (long)this.headerLength : this.last.position + 4L + (long)this.last.length + this.fileLength - this.first.position;
        }
    }

    private long remainingBytes() {
        return this.fileLength - this.usedBytes();
    }

    public boolean isEmpty() {
        return this.elementCount == 0;
    }

    private void expandIfNecessary(long dataLength) throws IOException {
        long elementLength = 4L + dataLength;
        long remainingBytes = this.remainingBytes();
        if (remainingBytes < elementLength) {
            long previousLength = this.fileLength;

            long newLength;
            do {
                remainingBytes += previousLength;
                newLength = previousLength << 1;
                previousLength = newLength;
            } while(remainingBytes < elementLength);

            this.setLength(newLength);
            long endOfLastElement = this.wrapPosition(this.last.position + 4L + (long)this.last.length);
            long count = 0L;
            if (endOfLastElement <= this.first.position) {
                FileChannel channel = this.raf.getChannel();
                channel.position(this.fileLength);
                count = endOfLastElement - (long)this.headerLength;
                if (channel.transferTo((long)this.headerLength, count, channel) != count) {
                    throw new AssertionError("Copied insufficient number of bytes!");
                }
            }

            if (this.last.position < this.first.position) {
                long newLastPosition = this.fileLength + this.last.position - (long)this.headerLength;
                this.writeHeader(newLength, this.elementCount, this.first.position, newLastPosition);
                this.last = new QueueFile.Element(newLastPosition, this.last.length);
            } else {
                this.writeHeader(newLength, this.elementCount, this.first.position, this.last.position);
            }

            this.fileLength = newLength;
            if (this.zero) {
                this.ringErase((long)this.headerLength, count);
            }

        }
    }

    private void setLength(long newLength) throws IOException {
        this.raf.setLength(newLength);
        this.raf.getChannel().force(true);
    }

    @Nullable
    public byte[] peek() throws IOException {
        if (this.closed) {
            throw new IOException("closed");
        } else if (this.isEmpty()) {
            return null;
        } else {
            int length = this.first.length;
            byte[] data = new byte[length];
            this.ringRead(this.first.position + 4L, data, 0, length);
            return data;
        }
    }

    public Iterator<byte[]> iterator() {
        return new QueueFile.ElementIterator();
    }

    public int size() {
        return this.elementCount;
    }

    public void remove() throws IOException {
        this.remove(1);
    }

    public void remove(int n) throws IOException {
        if (n < 0) {
            throw new IllegalArgumentException("Cannot remove negative (" + n + ") number of elements.");
        } else if (n != 0) {
            if (n == this.elementCount) {
                this.clear();
            } else if (this.isEmpty()) {
                throw new NoSuchElementException();
            } else if (n > this.elementCount) {
                throw new IllegalArgumentException("Cannot remove more elements (" + n + ") than present in queue (" + this.elementCount + ").");
            } else {
                long eraseStartPosition = this.first.position;
                long eraseTotalLength = 0L;
                long newFirstPosition = this.first.position;
                int newFirstLength = this.first.length;

                for(int i = 0; i < n; ++i) {
                    eraseTotalLength += (long)(4 + newFirstLength);
                    newFirstPosition = this.wrapPosition(newFirstPosition + 4L + (long)newFirstLength);
                    this.ringRead(newFirstPosition, this.buffer, 0, 4);
                    newFirstLength = readInt(this.buffer, 0);
                }

                this.writeHeader(this.fileLength, this.elementCount - n, newFirstPosition, this.last.position);
                this.elementCount -= n;
                ++this.modCount;
                this.first = new QueueFile.Element(newFirstPosition, newFirstLength);
                if (this.zero) {
                    this.ringErase(eraseStartPosition, eraseTotalLength);
                }

            }
        }
    }

    public void clear() throws IOException {
        if (this.closed) {
            throw new IOException("closed");
        } else {
            this.writeHeader(4096L, 0, 0L, 0L);
            if (this.zero) {
                this.raf.seek((long)this.headerLength);
                this.raf.write(ZEROES, 0, 4096 - this.headerLength);
            }

            this.elementCount = 0;
            this.first = QueueFile.Element.NULL;
            this.last = QueueFile.Element.NULL;
            if (this.fileLength > 4096L) {
                this.setLength(4096L);
            }

            this.fileLength = 4096L;
            ++this.modCount;
        }
    }

    public File file() {
        return this.file;
    }

    public void close() throws IOException {
        this.closed = true;
        this.raf.close();
    }

    public String toString() {
        return "QueueFile{file=" + this.file + ", zero=" + this.zero + ", versioned=" + this.versioned + ", length=" + this.fileLength + ", size=" + this.elementCount + ", first=" + this.first + ", last=" + this.last + '}';
    }

    public static final class Builder {
        final File file;
        boolean zero = true;
        boolean forceLegacy = false;

        public Builder(File file) {
            if (file == null) {
                throw new NullPointerException("file == null");
            } else {
                this.file = file;
            }
        }

        public QueueFile.Builder zero(boolean zero) {
            this.zero = zero;
            return this;
        }

        public QueueFile.Builder forceLegacy(boolean forceLegacy) {
            this.forceLegacy = forceLegacy;
            return this;
        }

        public QueueFile build() throws IOException {
            RandomAccessFile raf = QueueFile.initializeFromFile(this.file, this.forceLegacy);
            return new QueueFile(this.file, raf, this.zero, this.forceLegacy);
        }
    }

    public static class Element {
        static final QueueFile.Element NULL = new QueueFile.Element(0L, 0);
        static final int HEADER_LENGTH = 4;
        final long position;
        final int length;

        Element(long position, int length) {
            this.position = position;
            this.length = length;
        }

        public String toString() {
            return this.getClass().getSimpleName() + "[position=" + this.position + ", length=" + this.length + "]";
        }
    }

    private final class ElementIterator implements Iterator<byte[]> {
        int nextElementIndex = 0;
        private long nextElementPosition;
        int expectedModCount;

        ElementIterator() {
            this.nextElementPosition = QueueFile.this.first.position;
            this.expectedModCount = QueueFile.this.modCount;
        }

        private void checkForComodification() {
            if (QueueFile.this.modCount != this.expectedModCount) {
                throw new ConcurrentModificationException();
            }
        }

        public boolean hasNext() {
            if (QueueFile.this.closed) {
                throw new IllegalStateException("closed");
            } else {
                this.checkForComodification();
                return this.nextElementIndex != QueueFile.this.elementCount;
            }
        }

        public byte[] next() {
            if (QueueFile.this.closed) {
                throw new IllegalStateException("closed");
            } else {
                this.checkForComodification();
                if (QueueFile.this.isEmpty()) {
                    throw new NoSuchElementException();
                } else if (this.nextElementIndex >= QueueFile.this.elementCount) {
                    throw new NoSuchElementException();
                } else {
                    try {
                        QueueFile.Element current = QueueFile.this.readElement(this.nextElementPosition);
                        byte[] buffer = new byte[current.length];
                        this.nextElementPosition = QueueFile.this.wrapPosition(current.position + 4L);
                        QueueFile.this.ringRead(this.nextElementPosition, buffer, 0, current.length);
                        this.nextElementPosition = QueueFile.this.wrapPosition(current.position + 4L + (long)current.length);
                        ++this.nextElementIndex;
                        return buffer;
                    } catch (IOException var3) {
                        throw new RuntimeException("todo: throw a proper error", var3);
                    }
                }
            }
        }

        public void remove() {
            this.checkForComodification();
            if (QueueFile.this.isEmpty()) {
                throw new NoSuchElementException();
            } else if (this.nextElementIndex != 1) {
                throw new UnsupportedOperationException("Removal is only permitted from the head.");
            } else {
                try {
                    QueueFile.this.remove();
                } catch (IOException var2) {
                    throw new RuntimeException("todo: throw a proper error", var2);
                }

                this.expectedModCount = QueueFile.this.modCount;
                --this.nextElementIndex;
            }
        }
    }
}