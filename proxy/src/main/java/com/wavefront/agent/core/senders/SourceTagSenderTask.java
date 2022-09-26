package com.wavefront.agent.core.senders;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.wavefront.agent.core.buffers.Buffer;
import com.wavefront.agent.core.buffers.BuffersManager;
import com.wavefront.agent.core.queues.QueueInfo;
import com.wavefront.agent.core.queues.QueueStats;
import com.wavefront.agent.data.EntityProperties;
import com.wavefront.api.SourceTagAPI;
import com.wavefront.dto.SourceTag;
import java.util.Iterator;
import java.util.List;
import java.util.logging.Logger;
import javax.ws.rs.core.Response;

public class SourceTagSenderTask extends SenderTask {
  private static final Logger log = Logger.getLogger(SourceTagSenderTask.class.getCanonicalName());

  private final QueueInfo queue;
  private final SourceTagAPI proxyAPI;

  SourceTagSenderTask(
      QueueInfo queue,
      int idx,
      SourceTagAPI proxyAPI,
      EntityProperties properties,
      Buffer buffer,
      QueueStats queueStats) {
    super(queue, idx, properties, buffer, queueStats);
    this.queue = queue;
    this.proxyAPI = proxyAPI;
  }

  @Override
  protected Response submit(List<String> batch) {

    ObjectMapper objectMapper = new ObjectMapper();

    Iterator<String> iterator = batch.iterator();
    while (iterator.hasNext()) {
      String sourceTagStr = iterator.next();
      try {
        SourceTag sourceTag = objectMapper.readValue(sourceTagStr, SourceTag.class);
        Response res = doExecute(sourceTag);
        if ((res.getStatus() / 100) != 2) {
          // if there is a communication problem, we send back the point to the buffer
          BuffersManager.sendMsg(queue, sourceTagStr);
          iterator.forEachRemaining(s -> BuffersManager.sendMsg(queue, s));
        }
      } catch (JsonProcessingException e) {
        log.severe("Error parsing a SourceTag point. " + e);
      }
    }
    return Response.ok().build();
  }

  private Response doExecute(SourceTag sourceTag) {
    switch (sourceTag.getOperation()) {
      case SOURCE_DESCRIPTION:
        switch (sourceTag.getAction()) {
          case DELETE:
            Response resp = proxyAPI.removeDescription(sourceTag.getSource());
            if (resp.getStatus() == 404) {
              log.info(
                  "Attempting to delete description for "
                      + "a non-existent source  "
                      + sourceTag.getSource()
                      + ", ignoring");
              return Response.ok().build();
            }
            return resp;
          case SAVE:
          case ADD:
            return proxyAPI.setDescription(
                sourceTag.getSource(), sourceTag.getAnnotations().get(0));
          default:
            throw new IllegalArgumentException("Invalid acton: " + sourceTag.getAction());
        }
      case SOURCE_TAG:
        switch (sourceTag.getAction()) {
          case ADD:
            String addTag = sourceTag.getAnnotations().get(0);
            Response re = proxyAPI.appendTag(sourceTag.getSource(), addTag);
            if (re.getStatus() == 404) {
              log.info(
                  "Failed to add tag "
                      + addTag
                      + " for source "
                      + sourceTag.getSource()
                      + ", ignoring");
              return Response.ok().build();
            }
            return re;
          case DELETE:
            String tag = sourceTag.getAnnotations().get(0);
            Response resp = proxyAPI.removeTag(sourceTag.getSource(), tag);
            if (resp.getStatus() == 404) {
              log.info(
                  "Attempting to delete non-existing tag "
                      + tag
                      + " for source "
                      + sourceTag.getSource()
                      + ", ignoring");
              return Response.ok().build();
            }
            return resp;
          case SAVE:
            return proxyAPI.setTags(sourceTag.getSource(), sourceTag.getAnnotations());
          default:
            throw new IllegalArgumentException("Invalid acton: " + sourceTag.getAction());
        }
      default:
        throw new IllegalArgumentException(
            "Invalid source tag operation: " + sourceTag.getOperation());
    }
  }
}
