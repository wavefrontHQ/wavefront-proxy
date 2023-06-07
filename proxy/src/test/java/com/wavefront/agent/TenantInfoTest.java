package com.wavefront.agent;

import static com.wavefront.agent.api.APIContainer.CENTRAL_TENANT_NAME;
import static org.easymock.EasyMock.*;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import com.wavefront.agent.api.APIContainer;
import com.wavefront.agent.api.CSPAPI;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Properties;
import java.util.UUID;
import javax.ws.rs.core.Response;
import org.easymock.EasyMock;
import org.junit.After;
import org.junit.Test;

/**
 * Unit tests for {@link TokenWorkerCSP}.
 *
 * @author Norayr Chaparyan(nchaparyan@vmware.com).
 */
public class TenantInfoTest {
  private static final String wfServer = "wfServer";
  private final CSPAPI cpsApi = EasyMock.createMock(CSPAPI.class);
  private final Response mockResponse = EasyMock.createMock(Response.class);
  private final Response.StatusType statusTypeMock = EasyMock.createMock(Response.StatusType.class);
  private final TokenExchangeResponseDTO mockTokenExchangeResponseDTO =
      EasyMock.createMock(TokenExchangeResponseDTO.class);

  @After
  public void cleanup() {
    TokenManager.reset();
    reset(mockResponse, statusTypeMock, mockTokenExchangeResponseDTO, cpsApi);
  }

  private void replayAll() {
    replay(mockResponse, statusTypeMock, mockTokenExchangeResponseDTO, cpsApi);
  }

  private void verifyAll() {
    verify(mockResponse, statusTypeMock, mockTokenExchangeResponseDTO, cpsApi);
  }

  @Test
  public void testRun_SuccessfulResponseWavefrontAPIToken() {
    final String wfToken = "wavefront-api-token";
    // Replay mocks
    replayAll();

    TenantInfo tokenWorkerCSP = new TokenWorkerWF(wfToken, wfServer);
    TokenManager.addTenant(CENTRAL_TENANT_NAME, tokenWorkerCSP);
    TokenManager.start(new APIContainer(null, null, null, null, cpsApi));

    // Verify the results
    assertEquals(wfToken, tokenWorkerCSP.getBearerToken());
    assertEquals(wfServer, tokenWorkerCSP.getWFServer());
    verifyAll();
  }

  @Test
  public void testRun_SuccessfulResponseUsingOAuthApp() {
    TokenWorkerCSP tokenWorkerCSP = new TokenWorkerCSP("appId", "appSecret", "orgId", wfServer);

    // Set up expectations
    expect(mockResponse.getStatusInfo()).andReturn(statusTypeMock).times(2);
    expect(statusTypeMock.getStatusCode()).andReturn(200);
    expect(statusTypeMock.getFamily()).andReturn(Response.Status.Family.SUCCESSFUL);
    expect(mockResponse.readEntity(TokenExchangeResponseDTO.class))
        .andReturn(mockTokenExchangeResponseDTO);
    expect(mockTokenExchangeResponseDTO.getAccessToken()).andReturn("newAccessToken");
    expect(mockTokenExchangeResponseDTO.getExpiresIn()).andReturn(600);
    expect(
            cpsApi.getTokenByClientCredentials(
                "Basic YXBwSWQ6YXBwU2VjcmV0", "client_credentials", "orgId"))
        .andReturn(mockResponse)
        .times(1);
    replayAll();

    TokenManager.addTenant(CENTRAL_TENANT_NAME, tokenWorkerCSP);
    TokenManager.start(new APIContainer(null, null, null, null, cpsApi));

    // Verify the results
    assertEquals("newAccessToken", tokenWorkerCSP.getBearerToken());
    assertEquals(wfServer, tokenWorkerCSP.getWFServer());
    verifyAll();
  }

  @Test
  public void testRun_UnsuccessfulResponseUsingOAuthApp() {
    TokenWorkerCSP tokenWorkerCSP = new TokenWorkerCSP("appId", "appSecret", "orgId", wfServer);

    // Set up expectations
    expect(mockResponse.getStatusInfo()).andReturn(statusTypeMock).times(3);
    expect(statusTypeMock.getStatusCode()).andReturn(400).times(2);
    expect(statusTypeMock.getFamily()).andReturn(Response.Status.Family.SERVER_ERROR);
    expect(
            cpsApi.getTokenByClientCredentials(
                "Basic YXBwSWQ6YXBwU2VjcmV0", "client_credentials", "orgId"))
        .andReturn(mockResponse)
        .times(1);
    replayAll();

    TokenManager.addTenant(CENTRAL_TENANT_NAME, tokenWorkerCSP);
    TokenManager.start(new APIContainer(null, null, null, null, cpsApi));

    // Verify the results
    assertNull(tokenWorkerCSP.getBearerToken());
    assertEquals(wfServer, tokenWorkerCSP.getWFServer());
    verifyAll();
  }

  @Test
  public void testRun_SuccessfulResponseUsingCSPAPIToken() {
    TokenWorkerCSP tokenWorkerCSP = new TokenWorkerCSP("csp-api-token", wfServer);

    // Set up expectations
    expect(mockResponse.getStatusInfo()).andReturn(statusTypeMock).times(2);
    expect(statusTypeMock.getStatusCode()).andReturn(200);
    expect(statusTypeMock.getFamily()).andReturn(Response.Status.Family.SUCCESSFUL);
    expect(mockResponse.readEntity(TokenExchangeResponseDTO.class))
        .andReturn(mockTokenExchangeResponseDTO);
    expect(mockTokenExchangeResponseDTO.getAccessToken()).andReturn("newAccessToken");
    expect(mockTokenExchangeResponseDTO.getExpiresIn()).andReturn(600);
    expect(cpsApi.getTokenByAPIToken("api_token", "csp-api-token"))
        .andReturn(mockResponse)
        .times(1);
    replayAll();

    TokenManager.addTenant(CENTRAL_TENANT_NAME, tokenWorkerCSP);
    TokenManager.start(new APIContainer(null, null, null, null, cpsApi));

    // Verify the results
    assertEquals("newAccessToken", tokenWorkerCSP.getBearerToken());
    assertEquals(wfServer, tokenWorkerCSP.getWFServer());
    verifyAll();
  }

  @Test
  public void testRun_UnsuccessfulResponseUsingCSPAPIToken() {
    TokenWorkerCSP tokenWorkerCSP = new TokenWorkerCSP("csp-api-token", wfServer);

    // Set up expectations
    expect(mockResponse.getStatusInfo()).andReturn(statusTypeMock).times(3);
    expect(statusTypeMock.getStatusCode()).andReturn(400).times(2);
    expect(statusTypeMock.getFamily()).andReturn(Response.Status.Family.SERVER_ERROR);
    expect(cpsApi.getTokenByAPIToken("api_token", "csp-api-token"))
        .andReturn(mockResponse)
        .times(1);
    replayAll();

    TokenManager.addTenant(CENTRAL_TENANT_NAME, tokenWorkerCSP);
    TokenManager.start(new APIContainer(null, null, null, null, cpsApi));

    // Verify the results
    assertNull(tokenWorkerCSP.getBearerToken());
    assertEquals(wfServer, tokenWorkerCSP.getWFServer());
    verifyAll();
  }

  @Test
  public void full_test() throws IOException {
    File cfgFile = File.createTempFile("proxy", ".cfg");
    cfgFile.deleteOnExit();

    Properties props = new Properties();
    props.setProperty("pushListenerPorts", "1234");

    props.setProperty("multicastingTenants", "2");

    props.setProperty("multicastingTenantName_1", "name1");
    props.setProperty("multicastingServer_1", "server1");
    props.setProperty("multicastingCSPAPIToken_1", "csp-api-token1");

    props.setProperty("multicastingTenantName_2", "name2");
    props.setProperty("multicastingServer_2", "server2");
    props.setProperty("multicastingCSPAppId_2", "app-id2");
    props.setProperty("multicastingCSPAppSecret_2", "app-secret2");
    props.setProperty("multicastingCSPOrgId_2", "org-id2");

    FileOutputStream out = new FileOutputStream(cfgFile);
    props.store(out, "");
    out.close();

    String token = UUID.randomUUID().toString();
    String[] args =
        new String[] {
          "-f",
          cfgFile.getAbsolutePath(),
          "--pushListenerPorts",
          "4321",
          "--proxyname",
          "proxyname",
          "--token",
          token,
        };

    ProxyConfig proxyConfig = new ProxyConfig();
    proxyConfig.parseArguments(args, "test");

    CSPAPI cpsApi = EasyMock.createMock(CSPAPI.class);
    Response mockResponse = EasyMock.createMock(Response.class);
    Response.StatusType statusTypeMock = EasyMock.createMock(Response.StatusType.class);
    TokenExchangeResponseDTO mockTokenExchangeResponseDTO =
        EasyMock.createMock(TokenExchangeResponseDTO.class);

    expect(mockResponse.getStatusInfo()).andReturn(statusTypeMock).times(4);
    expect(statusTypeMock.getStatusCode()).andReturn(200).times(2);
    expect(statusTypeMock.getFamily()).andReturn(Response.Status.Family.SUCCESSFUL).times(2);
    expect(mockResponse.readEntity(TokenExchangeResponseDTO.class))
        .andReturn(mockTokenExchangeResponseDTO)
        .times(2);
    expect(mockTokenExchangeResponseDTO.getAccessToken()).andReturn("newAccessToken").times(2);
    expect(mockTokenExchangeResponseDTO.getExpiresIn()).andReturn(600).times(2);
    expect(
            cpsApi.getTokenByClientCredentials(
                "Basic YXBwLWlkMjphcHAtc2VjcmV0Mg==", "client_credentials", "org-id2"))
        .andReturn(mockResponse)
        .times(1);
    expect(cpsApi.getTokenByAPIToken("api_token", "csp-api-token1"))
        .andReturn(mockResponse)
        .times(1);

    replay(mockResponse, statusTypeMock, mockTokenExchangeResponseDTO, cpsApi);
    TokenManager.start(new APIContainer(null, null, null, null, cpsApi));
    verify(mockResponse, statusTypeMock, mockTokenExchangeResponseDTO, cpsApi);
  }
}
