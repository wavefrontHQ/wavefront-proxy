package com.wavefront.agent;

import org.apache.commons.daemon.Daemon;
import org.apache.commons.daemon.DaemonContext;

/**
 * @author Mori Bellamy (mori@wavefront.com)
 */
public class WavefrontProxyService implements Daemon {

    private PushAgent agent;
    private DaemonContext daemonContext;

    @Override
    public void init(DaemonContext daemonContext) {
        this.daemonContext = daemonContext;
        agent = new PushAgent();
    }

    @Override
    public void start() {
        agent.start(daemonContext.getArguments());
    }

    @Override
    public void stop() {
        agent.shutdown();
    }

    @Override
    public void destroy() {
    }
}
