package com.wavefront.agent;

import static org.easymock.EasyMock.expect;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import com.wavefront.agent.auth.CSPAuthConnector;
import javax.ws.rs.core.Response;
import org.easymock.EasyMock;
import org.junit.Test;

/**
 * Unit tests for {@link TenantInfo}.
 *
 * @author Norayr Chaparyan(nchaparyan@vmware.com).
 */
public class TenantInfoTest {
  private static final String wfServer = "wfServer";
  private TenantInfo tenantInfo;
  private final CSPAuthConnector mockCSPAuthConnector = EasyMock.createMock(CSPAuthConnector.class);
  private final Response mockResponse = EasyMock.createMock(Response.class);
  private final Response.StatusType statusTypeMock = EasyMock.createMock(Response.StatusType.class);
  private final TokenExchangeResponseDTO mockTokenExchangeResponseDTO =
      EasyMock.createMock(TokenExchangeResponseDTO.class);

  private void replayAll() {
    EasyMock.replay(
        mockCSPAuthConnector, mockResponse, statusTypeMock, mockTokenExchangeResponseDTO);
  }

  private void verifyAll() {
    EasyMock.verify(
        mockCSPAuthConnector, mockResponse, statusTypeMock, mockTokenExchangeResponseDTO);
  }

  @Test
  public void testRun_SuccessfulResponseWavefrontAPIToken() {
    final String wfToken = "wavefront-api-token";
    // Replay mocks
    replayAll();

    tenantInfo = new TenantInfo(wfToken, wfServer, ProxyConfig.ProxyAuthMethod.WAVEFRONT_API_TOKEN);

    // Verify the results
    assertEquals(wfToken, tenantInfo.getBearerToken());
    assertEquals(wfServer, tenantInfo.getWFServer());
    verifyAll();
  }

  @Test
  public void testRun_SuccessfulResponseUsingOAuthApp() {
    tenantInfo =
        new TenantInfo(
            "appId",
            "appSecret",
            "orgId",
            wfServer,
            ProxyConfig.ProxyAuthMethod.CSP_CLIENT_CREDENTIALS);
    tenantInfo.setCSPAuthConnector(mockCSPAuthConnector);

    // Set up expectations
    expect(mockCSPAuthConnector.loadAccessTokenByClientCredentials()).andReturn(mockResponse);
    expect(mockResponse.getStatusInfo()).andReturn(statusTypeMock).times(2);
    expect(statusTypeMock.getStatusCode()).andReturn(200);
    expect(statusTypeMock.getFamily()).andReturn(Response.Status.Family.SUCCESSFUL);
    expect(mockResponse.readEntity(TokenExchangeResponseDTO.class))
        .andReturn(mockTokenExchangeResponseDTO);
    expect(mockTokenExchangeResponseDTO.getAccessToken()).andReturn("newAccessToken");
    expect(mockTokenExchangeResponseDTO.getExpiresIn()).andReturn(600);

    // Replay mocks
    replayAll();

    tenantInfo.run();

    // Verify the results
    assertEquals("newAccessToken", tenantInfo.getBearerToken());
    assertEquals(wfServer, tenantInfo.getWFServer());
    verifyAll();
  }

  @Test
  public void testRun_UnsuccessfulResponseUsingOAuthApp() {
    tenantInfo =
        new TenantInfo(
            "appId",
            "appSecret",
            "orgId",
            wfServer,
            ProxyConfig.ProxyAuthMethod.CSP_CLIENT_CREDENTIALS);
    tenantInfo.setCSPAuthConnector(mockCSPAuthConnector);

    // Set up expectations
    expect(mockCSPAuthConnector.loadAccessTokenByClientCredentials()).andReturn(mockResponse);
    expect(mockResponse.getStatusInfo()).andReturn(statusTypeMock).times(3);
    expect(statusTypeMock.getStatusCode()).andReturn(400).times(2);
    expect(statusTypeMock.getFamily()).andReturn(Response.Status.Family.SERVER_ERROR);

    // Replay mocks
    replayAll();

    tenantInfo.run();

    // Verify the results
    assertNull(tenantInfo.getBearerToken());
    assertEquals(wfServer, tenantInfo.getWFServer());
    verifyAll();
  }

  @Test
  public void testRun_SuccessfulResponseUsingCSPAPIToken() {
    tenantInfo =
        new TenantInfo("csp-api-token", wfServer, ProxyConfig.ProxyAuthMethod.CSP_API_TOKEN);
    tenantInfo.setCSPAuthConnector(mockCSPAuthConnector);

    // Set up expectations
    expect(mockCSPAuthConnector.loadAccessTokenByAPIToken()).andReturn(mockResponse);
    expect(mockResponse.getStatusInfo()).andReturn(statusTypeMock).times(2);
    expect(statusTypeMock.getStatusCode()).andReturn(200);
    expect(statusTypeMock.getFamily()).andReturn(Response.Status.Family.SUCCESSFUL);
    expect(mockResponse.readEntity(TokenExchangeResponseDTO.class))
        .andReturn(mockTokenExchangeResponseDTO);
    expect(mockTokenExchangeResponseDTO.getAccessToken()).andReturn("newAccessToken");
    expect(mockTokenExchangeResponseDTO.getExpiresIn()).andReturn(600);

    // Replay mocks
    replayAll();

    tenantInfo.run();

    // Verify the results
    assertEquals("newAccessToken", tenantInfo.getBearerToken());
    assertEquals(wfServer, tenantInfo.getWFServer());
    verifyAll();
  }

  @Test
  public void testRun_UnsuccessfulResponseUsingCSPAPIToken() {
    tenantInfo =
        new TenantInfo("csp-api-token", wfServer, ProxyConfig.ProxyAuthMethod.CSP_API_TOKEN);
    tenantInfo.setCSPAuthConnector(mockCSPAuthConnector);

    // Set up expectations
    expect(mockCSPAuthConnector.loadAccessTokenByAPIToken()).andReturn(mockResponse);
    expect(mockResponse.getStatusInfo()).andReturn(statusTypeMock).times(3);
    expect(statusTypeMock.getStatusCode()).andReturn(400).times(2);
    expect(statusTypeMock.getFamily()).andReturn(Response.Status.Family.SERVER_ERROR);

    // Replay mocks
    replayAll();

    tenantInfo.run();

    // Verify the results
    assertNull(tenantInfo.getBearerToken());
    assertEquals(wfServer, tenantInfo.getWFServer());
    verifyAll();
  }
}
