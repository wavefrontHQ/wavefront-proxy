package com.wavefront.agent;

import static com.wavefront.agent.ProxyConfig.ProxyAuthMethod.*;
import static com.wavefront.agent.api.APIContainer.CENTRAL_TENANT_NAME;

import com.google.common.collect.Maps;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
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
  private List<TenantInfo> tenantsInfo = new ArrayList();

  /**
   * Helper function to construct tenant info {@link TenantInfo} object based on input parameters.
   *
   * @param appId the CSP OAuth server to server app id.
   * @param appSecret the CSP OAuth server to server app secret.
   * @param cspOrgId the CSP organisation id.
   * @param cspAPIToken the CSP API wfToken.
   * @param wfToken the Wavefront API wfToken.
   * @param server the server url.
   * @param tenantName the name of the tenant.
   * @throws IllegalArgumentException for invalid arguments.
   */
  public void constructTenantInfoObject(
      @Nullable final String appId,
      @Nullable final String appSecret,
      @Nullable final String cspOrgId,
      @Nullable final String cspAPIToken,
      @Nonnull final String wfToken,
      @Nonnull final String server,
      @Nonnull final String tenantName) {

    final String BAD_CONFIG =
        "incorrect configuration, one (and only one) of this options are required: `token`, `cspAPIToken` or `cspAppId, cspAppSecret, cspOrgId`"
            + (CENTRAL_TENANT_NAME.equals(tenantName) ? "" : " for tenant `" + tenantName + "`");

    boolean isOAuthApp =
        StringUtils.isNotBlank(appId)
            || StringUtils.isNotBlank(appSecret)
            || StringUtils.isNotBlank(cspOrgId);
    boolean isCPSAPIToken = StringUtils.isNotBlank(cspAPIToken);
    boolean isWFToken = StringUtils.isNotBlank(wfToken);

    long authMethods =
        Arrays.asList(isOAuthApp, isCPSAPIToken, isWFToken).stream().filter(auth -> auth).count();
    if (authMethods != 1) {
      throw new IllegalArgumentException(BAD_CONFIG);
    }

    TenantInfo tenantInfo;
    if (isOAuthApp) {
      if (StringUtils.isNotBlank(appId)
          && StringUtils.isNotBlank(appSecret)
          && StringUtils.isNotBlank(cspOrgId)) {
        logger.info(
            "TCSP OAuth server to server app credentials for further authentication. For the server "
                + server);
        tenantInfo = new TenantInfo(appId, appSecret, cspOrgId, server, CSP_CLIENT_CREDENTIALS);
        tenantsInfo.add(tenantInfo);
      } else {
        throw new IllegalArgumentException(BAD_CONFIG);
      }
    } else if (isCPSAPIToken) {
      logger.info("CSP api token for further authentication. For the server " + server);
      tenantInfo = new TenantInfo(cspAPIToken, server, CSP_API_TOKEN);
      tenantsInfo.add(tenantInfo);
    } else { // isWFToken
      logger.info("Wavefront api token for further authentication. For the server " + server);
      tenantInfo = new TenantInfo(wfToken, server, WAVEFRONT_API_TOKEN);
    }

    multicastingTenantList.put(tenantName, tenantInfo);
  }

  public void start() {
    tenantsInfo.forEach(tenantInfo -> tenantInfo.run());
  }

  public Map<String, TenantInfo> getMulticastingTenantList() {
    return multicastingTenantList;
  }
}
