package com.wavefront.agent;

import com.squareup.tape.Task;
import com.wavefront.api.AgentAPI;

import java.util.List;
import java.util.UUID;

/**
 * A task for resubmission.
 *
 * @author Clement Pang (clement@wavefront.com).
 */
public abstract class ResubmissionTask<T extends ResubmissionTask<T>> implements Task {

    /**
     * To be injected. Should be null when serialized.
     */
    protected AgentAPI service = null;

    /**
     * To be injected. Should be null when serialized.
     */
    protected UUID currentAgentId = null;

    public abstract List<T> splitTask();
}
