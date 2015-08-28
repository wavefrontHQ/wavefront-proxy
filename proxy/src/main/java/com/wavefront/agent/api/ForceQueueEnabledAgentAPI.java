package com.wavefront.agent.api;

import com.wavefront.api.AgentAPI;
import com.wavefront.api.agent.ShellOutputDTO;

import javax.ws.rs.core.Response;
import java.util.UUID;

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
}
