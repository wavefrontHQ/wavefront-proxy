package com.wavefront.agent;

import com.wavefront.agent.api.APIContainer;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class ProxySendConfigScheduler {
  private static final Logger logger =
      LogManager.getLogger(ProxySendConfigScheduler.class.getCanonicalName());
  private boolean successful = false;
  private final ScheduledExecutorService executor;
  private final Runnable task;

  public ProxySendConfigScheduler(
      APIContainer apiContainer, UUID proxyId, ProxyConfig proxyConfig) {
    executor = Executors.newScheduledThreadPool(1);
    task =
        () -> {
          try {
            // TODO: review
//            apiContainer
//                .getProxyV2APIForTenant(APIContainer.CENTRAL_TENANT_NAME)
//                .proxySaveConfig(proxyId, proxyConfig.getJsonConfig());
            successful = true;
            logger.info("Configuration sent to the server successfully.");
          } catch (javax.ws.rs.NotFoundException ex) {
            logger.debug("'proxySaveConfig' api end point not found", ex);
            successful = true;
          } catch (Throwable e) {
            logger.warn(
                "Can't send the Proxy configuration to the server, retrying in 60 seconds. "
                    + e.getMessage());
            logger.log(Level.DEBUG, "Exception: ", e);
          }

          if (successful) {
            executor.shutdown();
          }
        };
  }

  public void start() {
    executor.scheduleAtFixedRate(task, 0, 60, TimeUnit.SECONDS);
  }
}
