package com.wavefront.agent;

import static com.wavefront.agent.ProxyConfig.ProxyAuthMethod.*;

import com.google.common.collect.Maps;
import java.util.Map;
import java.util.logging.Logger;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import org.apache.commons.lang.StringUtils;

/**
 * The class to construct {@link TenantInfo} objects and provide them.
 *
 * @author Norayr Chaparyan(nchaparyan@vmware.com).
 */
public class TenantInfoManager {
  private static final Logger logger = Logger.getLogger(TenantInfoManager.class.getCanonicalName());
  private final Map<String, TenantInfo> multicastingTenantList = Maps.newHashMap();

  /**
   * Helper function to construct tenant info {@link TenantInfo} object based on input parameters.
   *
   * @param appId the CSP OAuth server to server app id.
   * @param appSecret the CSP OAuth server to server app secret.
   * @param cspOrgId the CSP organisation id.
   * @param cspAPIToken the CSP API token.
   * @param token the Wavefront API token.
   * @param server the server url.
   * @param tenantName the name of the tenant.
   * @throws IllegalArgumentException for invalid arguments.
   */
  public void constructTenantInfoObject(
      @Nullable final String appId,
      @Nullable final String appSecret,
      @Nullable final String cspOrgId,
      @Nullable final String cspAPIToken,
      @Nonnull final String token,
      @Nonnull final String server,
      @Nonnull final String tenantName) {

    boolean oauthApp =
        StringUtils.isNotBlank(appId)
            && StringUtils.isNotBlank(appSecret)
            && StringUtils.isNotBlank(cspOrgId);

    if ((oauthApp && StringUtils.isNotBlank(cspAPIToken))
        || (oauthApp && StringUtils.isNotBlank(token))
        || (StringUtils.isNotBlank(cspAPIToken) && StringUtils.isNotBlank(token))) {
      throw new IllegalArgumentException(
          "Proxy failed to select an authentication method for the tenant name " + tenantName);
    }

    TenantInfo tenantInfo;
    if (oauthApp) {
      tenantInfo = new TenantInfo(appId, appSecret, cspOrgId, server, CSP_CLIENT_CREDENTIALS);
      tenantInfo.run();
      logger.info(
          "The proxy selected the CSP OAuth server to server app credentials for further authentication. For the server "
              + server);
    } else if (StringUtils.isNotBlank(cspAPIToken)) {
      tenantInfo = new TenantInfo(cspAPIToken, server, CSP_API_TOKEN);
      tenantInfo.run();
      logger.info(
          "The proxy selected the CSP api token for further authentication. For the server "
              + server);
    } else {
      tenantInfo = new TenantInfo(token, server, WAVEFRONT_API_TOKEN);
      logger.info(
          "The proxy selected the Wavefront api token for further authentication. For the server "
              + server);
    }

    multicastingTenantList.put(tenantName, tenantInfo);
  }

  public Map<String, TenantInfo> getMulticastingTenantList() {
    return multicastingTenantList;
  }
}
