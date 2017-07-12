package com.wavefront.agent.api;

import com.wavefront.api.WavefrontAPI;
import com.wavefront.api.agent.ShellOutputDTO;

import java.util.List;
import java.util.UUID;

import javax.ws.rs.core.Response;

/**
 * @author Andrew Kao (andrew@wavefront.com)
 */
public interface ForceQueueEnabledAgentAPI extends WavefrontAPI {

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

  Response removeTag(String id, String token, String tagValue, boolean forceToQueue);

  Response setTags(String id, String token, List<String> tagsValuesToSet, boolean forceToQueue);

  Response removeDescription(String id, String token, boolean forceToQueue);

  Response setDescription(String id, String token, String desc, boolean forceToQueue);
}
