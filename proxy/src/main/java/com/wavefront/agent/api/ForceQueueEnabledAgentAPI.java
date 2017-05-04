package com.wavefront.agent.api;

import com.wavefront.api.AgentAPI;
import com.wavefront.api.agent.ShellOutputDTO;

import java.util.List;
import java.util.UUID;

import javax.ws.rs.core.Response;

/**
 * @author Andrew Kao (andrew@wavefront.com)
 */
public interface ForceQueueEnabledAgentAPI extends AgentAPI {

  Response postWorkUnitResult(UUID agentId,
                              UUID workUnitId,
                              UUID targetId,
                              ShellOutputDTO shellOutputDTO,
                              boolean forceToQueue);

  Response postPushData(UUID agentId,
                        UUID workUnitId,
                        Long currentMillis,
                        String format,
                        String pushData,
                        boolean forceToQueue);

  Response removeTag(String id, String tagValue, boolean forceToQueue);

  Response setTags(String id, List<String> tagsValuesToSet, boolean forceToQueue);

  Response removeDescription(String id, boolean forceToQueue);

  Response setDescription(String id, String desc, boolean forceToQueue);
}
