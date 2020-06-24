package com.wavefront.agent.queueing;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Splitter;
import com.google.common.collect.Iterators;
import com.wavefront.agent.ProxyConfig;
import com.wavefront.agent.data.DataSubmissionTask;
import com.wavefront.agent.data.EntityPropertiesFactory;
import com.wavefront.agent.data.EventDataSubmissionTask;
import com.wavefront.agent.data.LineDelimitedDataSubmissionTask;
import com.wavefront.agent.data.SourceTagSubmissionTask;
import com.wavefront.agent.handlers.HandlerKey;
import com.wavefront.data.ReportableEntityType;
import com.wavefront.dto.Event;
import org.apache.commons.lang.math.NumberUtils;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * Supports proxy's ability to export data from buffer files.
 *
 * @author vasily@wavefront.com
 */
public class QueueExporter {
  private static final Logger logger = Logger.getLogger(QueueExporter.class.getCanonicalName());
  private static final Pattern FILENAME =
      Pattern.compile("^(.*)\\.(\\w+)\\.(\\w+)\\.(\\w+)\\.(\\w+)$");

  private final ProxyConfig config;
  private final TaskQueueFactory taskQueueFactory;
  private final EntityPropertiesFactory entityPropertiesFactory;

  /**
   * @param config                  Proxy configuration
   * @param taskQueueFactory        Factory for task queues
   * @param entityPropertiesFactory Entity properties factory
   */
  public QueueExporter(ProxyConfig config, TaskQueueFactory taskQueueFactory,
                       EntityPropertiesFactory entityPropertiesFactory) {
    this.config = config;
    this.taskQueueFactory = taskQueueFactory;
    this.entityPropertiesFactory = entityPropertiesFactory;
  }

  /**
   * Starts data exporting process.
   */
  public void export() {
    Set<HandlerKey> handlerKeys = getValidHandlerKeys(listFiles(config.getBufferFile()),
        config.getExportQueuePorts());
    handlerKeys.forEach(this::processHandlerKey);
  }

  @VisibleForTesting
  <T extends DataSubmissionTask<T>> void processHandlerKey(HandlerKey key) {
    logger.info("Processing " + key.getEntityType() + " queue for port " + key.getHandle());
    int threads = entityPropertiesFactory.get(key.getEntityType()).getFlushThreads();
    for (int i = 0; i < threads; i++) {
      TaskQueue<T> taskQueue = taskQueueFactory.getTaskQueue(key, i);
      if (!(taskQueue instanceof TaskQueueStub)) {
        String outputFileName = config.getExportQueueOutputFile() + "." + key.getEntityType() +
            "." + key.getHandle() + "." + i + ".txt";
        logger.info("Exporting data to " + outputFileName);
        try {
          BufferedWriter writer = new BufferedWriter(new FileWriter(outputFileName));
          processQueue(taskQueue, writer);
          writer.close();
          taskQueue.close();
        } catch (IOException e) {
          logger.log(Level.SEVERE, "IO error", e);
        }
      }
    }
  }

  @VisibleForTesting
  <T extends DataSubmissionTask<T>> void processQueue(TaskQueue<T> queue,
                                                      BufferedWriter writer) throws IOException {
    int tasksProcessed = 0;
    int itemsExported = 0;
    Iterator<T> iterator = queue.iterator();
    while (iterator.hasNext()) {
      T task = iterator.next();
      processTask(task, writer);
      if (!config.isExportQueueRetainData()) {
        iterator.remove();
      }
      tasksProcessed++;
      itemsExported += task.weight();
    }
    logger.info(tasksProcessed + " tasks, " + itemsExported + " items exported");
  }

  @VisibleForTesting
  <T extends DataSubmissionTask<T>> void processTask(T task, BufferedWriter writer)
      throws IOException {
    if (task instanceof LineDelimitedDataSubmissionTask) {
      for (String line : ((LineDelimitedDataSubmissionTask) task).payload()) {
        writer.write(line);
        writer.newLine();
      }
    } else if (task instanceof SourceTagSubmissionTask) {
      writer.write(((SourceTagSubmissionTask) task).payload().toString());
      writer.newLine();
    } else if (task instanceof EventDataSubmissionTask) {
      for (Event event : ((EventDataSubmissionTask) task).payload()) {
        writer.write(event.toString());
        writer.newLine();
      }
    }
  }

  @VisibleForTesting
  static Set<HandlerKey> getValidHandlerKeys(List<String> files, String portList) {
    Set<String> ports = new HashSet<>(Splitter.on(",").omitEmptyStrings().trimResults().
        splitToList(portList));
    Set<HandlerKey> out = new HashSet<>();
    files.forEach(x -> {
      Matcher matcher = FILENAME.matcher(x);
      if (matcher.matches()) {
        ReportableEntityType type = ReportableEntityType.fromString(matcher.group(2));
        String handle = matcher.group(3);
        if (type != null && NumberUtils.isDigits(matcher.group(4)) && !handle.startsWith("_") &&
            (portList.equalsIgnoreCase("all") || ports.contains(handle))) {
          out.add(HandlerKey.of(type, handle));
        }
      }
    });
    return out;
  }

  @VisibleForTesting
  static List<String> listFiles(String buffer) {
    String fnPrefix = Iterators.getLast(Splitter.on('/').split(buffer).iterator());
    File bufferFile = new File(buffer);
    File[] files = bufferFile.getParentFile().listFiles((dir, name) ->
        name.endsWith(".spool") && name.startsWith(fnPrefix));
    return files == null ?
        Collections.emptyList() :
        Arrays.stream(files).map(File::getAbsolutePath).collect(Collectors.toList());
  }
}
