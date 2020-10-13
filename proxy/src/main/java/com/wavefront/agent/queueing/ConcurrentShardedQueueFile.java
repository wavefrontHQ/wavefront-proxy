package com.wavefront.agent.queueing;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.ConcurrentModificationException;
import java.util.Deque;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang3.ObjectUtils;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterators;
import com.google.errorprone.annotations.CanIgnoreReturnValue;
import com.wavefront.common.Utils;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * A thread-safe {@link QueueFile} implementation, that uses multiple smaller "shard" files
 * instead of one large file. This also improves concurrency - when we have more than one file,
 * we can add and remove tasks at the same time without mutually exclusive locking.
 *
 * @author vasily@wavefront.com
 */
public class ConcurrentShardedQueueFile implements QueueFile {
  private static final int HEADER_SIZE_BYTES = 36;
  private static final int TASK_HEADER_SIZE_BYTES = 4;
  private static final int SUFFIX_DIGITS = 4;

  private final String fileNamePrefix;
  private final String fileNameSuffix;
  private final int shardSizeBytes;
  private final QueueFileFactory queueFileFactory;

  @VisibleForTesting
  final Deque<Shard> shards = new ConcurrentLinkedDeque<>();
  private final ReentrantLock globalLock = new ReentrantLock(true);
  private final ReentrantLock tailLock = new ReentrantLock(true);
  private final ReentrantLock headLock = new ReentrantLock(true);
  private volatile boolean closed = false;
  private volatile byte[] head;
  private final AtomicLong modCount = new AtomicLong();

  /**
   * @param fileNamePrefix   path + file name prefix for shard files
   * @param fileNameSuffix   file name suffix to identify shard files
   * @param shardSizeBytes   target shard size bytes
   * @param queueFileFactory factory for {@link QueueFile} objects
   * @throws IOException if file(s) could not be created or accessed
   */
  public ConcurrentShardedQueueFile(String fileNamePrefix,
                                    String fileNameSuffix,
                                    int shardSizeBytes,
                                    QueueFileFactory queueFileFactory) throws IOException {
    this.fileNamePrefix = fileNamePrefix;
    this.fileNameSuffix = fileNameSuffix;
    this.shardSizeBytes = shardSizeBytes;
    this.queueFileFactory = queueFileFactory;
    //noinspection unchecked
    for (String filename : ObjectUtils.firstNonNull(listFiles(fileNamePrefix, fileNameSuffix),
        ImmutableList.of(getInitialFilename()))) {
      Shard shard = new Shard(filename);
      // don't keep the QueueFile open within the shard object until it's actually needed,
      // as we don't want to keep too many files open.
      shard.close();
      this.shards.add(shard);
    }
  }

  @Nullable
  @Override
  public byte[] peek() throws IOException {
    checkForClosedState();
    headLock.lock();
    try {
      if (this.head == null) {
        globalLock.lock();
        Shard shard = shards.getFirst().updateStats();
        if (shards.size() > 1) {
          globalLock.unlock();
        }
        this.head = Objects.requireNonNull(shard.queueFile).peek();
      }
      return this.head;
    } finally {
      headLock.unlock();
      if (globalLock.isHeldByCurrentThread()) {
        globalLock.unlock();
      }
    }
  }

  @Override
  public void add(byte[] data, int offset, int count) throws IOException {
    checkForClosedState();
    tailLock.lock();
    try {
      globalLock.lock();
      // check whether we need to allocate a new shard
      Shard shard = shards.getLast();
      if (shard.newShardRequired(count)) {
        // allocate new shard unless the task is oversized and current shard is empty
        if (shards.size() > 1) {
          // we don't want to close if that shard was the head
          shard.close();
        }
        String newFileName = incrementFileName(shard.shardFileName, fileNameSuffix);
        shard = new Shard(newFileName);
        shards.addLast(shard);
      }
      shard.updateStats();
      modCount.incrementAndGet();
      if (shards.size() > 2) {
        globalLock.unlock();
      }
      Objects.requireNonNull(shard.queueFile).add(data, offset, count);
      shard.updateStats();
    } finally {
      tailLock.unlock();
      if (globalLock.isHeldByCurrentThread()) {
        globalLock.unlock();
      }
    }
  }

  @Override
  public void remove() throws IOException {
    checkForClosedState();
    headLock.lock();
    try {
      this.head = null;
      Shard shard = shards.getFirst().updateStats();
      if (shards.size() == 1) {
        globalLock.lock();
      }
      modCount.incrementAndGet();
      Objects.requireNonNull(shard.queueFile).remove();
      shard.updateStats();
      // check whether we have removed the last task in a shard
      if (shards.size() > 1 && shard.numTasks == 0) {
        shard.close();
        shards.removeFirst();
        new File(shard.shardFileName).delete();
      }
    } finally {
      headLock.unlock();
      if (globalLock.isHeldByCurrentThread()) {
        globalLock.unlock();
      }
    }
  }

  @Override
  public int size() {
    return shards.stream().mapToInt(shard -> shard.numTasks).sum();
  }

  @Override
  public long storageBytes() {
    return shards.stream().mapToLong(shard -> shard.fileLength).sum();
  }

  @Override
  public long usedBytes() {
    return shards.stream().mapToLong(shard -> shard.usedBytes).sum();
  }

  @Override
  public long availableBytes() {
    Shard shard = shards.getLast();
    return shard.fileLength - shard.usedBytes;
  }

  @Override
  public void close() throws IOException {
    this.closed = true;
    for (Shard shard : shards) {
      shard.close();
    }
  }

  @Override
  public void clear() throws IOException {
    this.headLock.lock();
    this.tailLock.lock();
    try {
      this.head = null;
      for (Shard shard : shards) {
        shard.close();
        new File(shard.shardFileName).delete();
      }
      shards.clear();
      shards.add(new Shard(getInitialFilename()));
      modCount.incrementAndGet();
    } finally {
      this.headLock.unlock();
      this.tailLock.unlock();
    }
  }

  @Nonnull
  @Override
  public Iterator<byte[]> iterator() {
    checkForClosedState();
    return new ShardedIterator();
  }

  private final class ShardedIterator implements Iterator<byte[]> {
    long expectedModCount = modCount.get();
    Iterator<byte[]> currentIterator = Collections.emptyIterator();
    Shard currentShard = null;
    Iterator<Shard> shardIterator = shards.iterator();
    int nextElementIndex = 0;

    ShardedIterator() {
    }

    private void checkForComodification() {
      checkForClosedState();
      if (modCount.get() != expectedModCount) {
        throw new ConcurrentModificationException();
      }
    }

    @Override
    public boolean hasNext() {
      checkForComodification();
      try {
        while (!checkNotNull(currentIterator).hasNext()) {
          if (!shardIterator.hasNext()) {
            return false;
          }
          currentShard = shardIterator.next().updateStats();
          currentIterator = Objects.requireNonNull(currentShard.queueFile).iterator();
        }
      } catch (IOException e) {
        throw Utils.<Error>throwAny(e);
      }
      return true;
    }

    @Override
    public byte[] next() {
      checkForComodification();
      if (hasNext()) {
        nextElementIndex++;
        return currentIterator.next();
      } else {
        throw new NoSuchElementException();
      }
    }

    @Override
    public void remove() {
      checkForComodification();
      if (nextElementIndex > 1) {
        throw new UnsupportedOperationException("Removal is only permitted from the head.");
      }
      try {
        currentIterator.remove();
        currentShard.updateStats();
        nextElementIndex--;
      } catch (IOException e) {
        throw Utils.<Error>throwAny(e);
      }
    }
  }

  private final class Shard {
    private final String shardFileName;
    @Nullable private QueueFile queueFile;
    private long fileLength;
    private Long usedBytes;
    private int numTasks;

    private Shard(String shardFileName) throws IOException {
      this.shardFileName = shardFileName;
      updateStats();
    }

    @CanIgnoreReturnValue
    private Shard updateStats() throws IOException {
      if (this.queueFile == null) {
        this.queueFile = queueFileFactory.get(this.shardFileName);
      }
      if (this.queueFile != null) {
        this.fileLength = this.queueFile.storageBytes();
        this.numTasks = this.queueFile.size();
        this.usedBytes = this.queueFile.usedBytes();
      }
      return this;
    }

    private void close() throws IOException {
      if (this.queueFile != null) {
        this.queueFile.close();
        this.queueFile = null;
      }
    }

    private boolean newShardRequired(int taskSize) {
      return (taskSize > (shardSizeBytes - this.usedBytes - TASK_HEADER_SIZE_BYTES) &&
          (taskSize <= (shardSizeBytes - HEADER_SIZE_BYTES) || this.numTasks > 0));
    }
  }

  private void checkForClosedState() {
    if (closed) {
      throw new IllegalStateException("closed");
    }
  }

  private String getInitialFilename() {
    return new File(fileNamePrefix).exists() ?
        fileNamePrefix :
        incrementFileName(fileNamePrefix, fileNameSuffix);
  }

  @VisibleForTesting
  @Nullable
  static List<String> listFiles(String path, String suffix) {
    String fnPrefix = Iterators.getLast(Splitter.on('/').split(path).iterator());
    Pattern pattern = getSuffixMatchingPattern(suffix);
    File bufferFilePath = new File(path);
    File[] files = bufferFilePath.getParentFile().listFiles((dir, fileName) ->
        (fileName.endsWith(suffix) || pattern.matcher(fileName).matches()) &&
            fileName.startsWith(fnPrefix));
    return (files == null || files.length == 0) ? null :
        Arrays.stream(files).map(File::getAbsolutePath).sorted().collect(Collectors.toList());
  }

  @VisibleForTesting
  static String incrementFileName(String fileName, String suffix) {
    Pattern pattern = getSuffixMatchingPattern(suffix);
    String zeroes = StringUtils.repeat("0", SUFFIX_DIGITS);
    if (pattern.matcher(fileName).matches()) {
      int nextId = Integer.parseInt(StringUtils.right(fileName, SUFFIX_DIGITS), 16) + 1;
      String newHex = StringUtils.right(zeroes + Long.toHexString(nextId), SUFFIX_DIGITS);
      return StringUtils.left(fileName, fileName.length() - SUFFIX_DIGITS) + newHex;
    } else {
      return fileName + "_" + zeroes;
    }
  }

  private static Pattern getSuffixMatchingPattern(String suffix) {
    return Pattern.compile("^.*" + Pattern.quote(suffix) + "_[0-9a-f]{" + SUFFIX_DIGITS + "}$");
  }
}
