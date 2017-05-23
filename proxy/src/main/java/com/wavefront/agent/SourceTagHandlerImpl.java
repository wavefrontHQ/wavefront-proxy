package com.wavefront.agent;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Lists;

import com.wavefront.ingester.SourceTagDecoder;

import java.util.List;
import java.util.Map;
import java.util.logging.Logger;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import sunnylabs.report.ReportSourceTag;

/**
 * This class will read the input from the socket, process it, and call the server API to set the
 * sourceTag/description.
 *
 * @author Suranjan Pramanik (suranjan@wavefront.com).
 */
public class SourceTagHandlerImpl extends SimpleChannelInboundHandler<String> implements
    SourceTagHandler {

  private static final Logger logger = Logger.getLogger(SourceTagHandlerImpl.class
      .getCanonicalName());

  // TODO: Add some metrics

  protected final PostSourceTagTimedTask[] sendDataTasks;

  // decoder which will be used to parse the input
  private static final SourceTagDecoder sourceTagDecoder = new SourceTagDecoder();

  public SourceTagHandlerImpl(PostSourceTagTimedTask[] tasks) {
    sendDataTasks = tasks;
  }

  /**
   * This method will read the input over the socket.
   *
   * @param ctx
   * @param msg
   * @throws Exception
   */
  @Override
  protected void channelRead0(ChannelHandlerContext ctx, String msg) throws Exception {
    if (msg == null || msg.trim().length() == 0) return;
    processSourceTag(msg);
  }

  /**
   * This method is used to process a sourceTag line coming from the client (such as Telegraf)
   *
   * @param msg The line containing the message
   */
  private void processSourceTag(final String msg) {
    String sourceTagLine = msg.trim();

    List<ReportSourceTag> sourceTags = Lists.newArrayListWithExpectedSize(1);
    try {
      sourceTagDecoder.decodeSourceTagLine(sourceTagLine, sourceTags);
    } catch (Exception ex) {
      // TODO: Handle the exception
    }
    reportSourceTags(sourceTags);
  }

  /**
   * This method will call the server-side APIs to set/remove the sourceTag/description.
   *
   * @param sourceTags
   */
  @Override
  public void reportSourceTags(List<ReportSourceTag> sourceTags) {
    for (ReportSourceTag reportSourceTag : sourceTags) {
      if (!annotationKeysAreValid(reportSourceTag)) {
        String errorMessage = "WF-401 : SourceTag annotation key has illegal characters.";
        throw new IllegalArgumentException(errorMessage);
      }
      // TODO: validate the sourceTag and call server API: Currently log it
      logger.info(String.format("Message Type = %s", reportSourceTag.getSourceTagLiteral()));
      logger.info(String.format("Description = %s", reportSourceTag.getDescription()));
      logger.info(String.format("Source = %s", reportSourceTag.getSource()));
      logger.info(String.format("Action = %s", reportSourceTag.getAction()));
      int count = 0;
      if (reportSourceTag.getAnnotations() != null) {
        for (String sourceTag : reportSourceTag.getAnnotations()) {
          logger.info(String.format("Tag[%d] = %s", count++, sourceTag));
        }
      }
      reportSourceTag(reportSourceTag);
    }
  }

  private void reportSourceTag(ReportSourceTag sourceTag) {
    final PostSourceTagTimedTask randomTask = getRandomPostTask();
    randomTask.addSourceTag(sourceTag);
  }

  @VisibleForTesting
  static boolean annotationKeysAreValid(ReportSourceTag sourceTag) {
    if (sourceTag.getAnnotations() != null) {
      for (String key : sourceTag.getAnnotations()) {
        if (!Validation.charactersAreValid(key)) {
          return false;
        }
      }
    }
    return true;
  }

  public PostSourceTagTimedTask getRandomPostTask() {
    // return the task with the lowest number of pending data
    long min = Long.MAX_VALUE;
    PostSourceTagTimedTask randomTask = null;
    PostSourceTagTimedTask firstChoice = null;
    for (int i=0; i < this.sendDataTasks.length; i++) {
      long dataToSend = this.sendDataTasks[i].getNumDataToSend();
      if (dataToSend < min) {
        min = dataToSend;
        randomTask = this.sendDataTasks[i];
        if (!this.sendDataTasks[i].getFlushingToQueueFlag()) {
          firstChoice = this.sendDataTasks[i];
        }
      }
    }
    return firstChoice == null ? randomTask : firstChoice;
  }
}
