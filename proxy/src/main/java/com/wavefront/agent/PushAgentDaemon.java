package com.wavefront.agent;

import org.apache.commons.daemon.Daemon;
import org.apache.commons.daemon.DaemonContext;
import org.apache.commons.daemon.DaemonInitException;

import java.util.logging.Logger;

/**
 * @author Mori Bellamy (mori@wavefront.com)
 */
public class PushAgentDaemon implements Daemon {

    protected static final Logger logger = Logger.getLogger("agent");

    private PushAgent agent;
    private DaemonContext daemonContext;

    @Override
    public void init(DaemonContext daemonContext) throws DaemonInitException, Exception {
        this.daemonContext = daemonContext;
        agent = new PushAgent();
    }

    @Override
    public void start() throws Exception {
        agent.start(daemonContext.getArguments());
    }

    @Override
    public void stop() throws Exception {
        agent.shutdown();
    }

    @Override
    public void destroy() {

    }
}
