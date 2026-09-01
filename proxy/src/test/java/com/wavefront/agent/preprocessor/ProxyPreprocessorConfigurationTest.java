package com.wavefront.agent.preprocessor;

import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.expectLastCall;
import static org.easymock.EasyMock.replay;
import static org.easymock.EasyMock.verify;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertNotNull;


import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.io.File;
import java.io.IOException;
import java.io.FileNotFoundException;
import java.nio.file.Files;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.function.Supplier;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import com.wavefront.agent.ProxyCheckInScheduler;
import com.wavefront.agent.TokenManager;
import com.wavefront.agent.TokenWorkerWF;
import com.wavefront.agent.api.APIContainer;
import com.wavefront.api.agent.preprocessor.ReportPointAddPrefixTransformer;
import org.easymock.EasyMock;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import wavefront.report.ReportPoint;

public class ProxyPreprocessorConfigurationTest {
    final String tempRules = "'3000':\n  - rule    : drop-ts-tag\n    action  : dropTag\n    tag   : ts\n    match  : ts.*";

    /** YAML snippet that defines a forward rule targeting {@code "sidecar"} on port 2878. */
    private static final String FORWARD_RULE_SIDECAR =
        "'2878':\n"
            + "  - rule: test-forward-sidecar\n"
            + "    action: forward\n"
            + "    scope: metricName\n"
            + "    match: '.*'\n"
            + "    tenants:\n"
            + "      - sidecar\n";

    /** YAML snippet that defines a forward rule targeting {@code "MyAlias"} on port 2878. */
    private static final String FORWARD_RULE_ALIAS =
        "'2878':\n"
            + "  - rule: test-forward-alias\n"
            + "    action: forward\n"
            + "    scope: metricName\n"
            + "    match: '.*'\n"
            + "    tenants:\n"
            + "      - MyAlias\n";

    /** YAML snippet that defines a forward rule targeting an unknown/unregistered tenant. */
    private static final String FORWARD_RULE_UNKNOWN =
        "'2878':\n"
            + "  - rule: test-forward-unknown\n"
            + "    action: forward\n"
            + "    scope: metricName\n"
            + "    match: '.*'\n"
            + "    tenants:\n"
            + "      - unknownTenant\n";

    @Before
    public void setUp() {
        TokenManager.reset();
        ProxyPreprocessorConfigManager.clearProxyConfigRules();
    }

    @After
    public void tearDown() {
        TokenManager.reset();
        ProxyPreprocessorConfigManager.clearProxyConfigRules();
        ProxyCheckInScheduler.preprocessorRulesNeedUpdate.set(false);
        ProxyCheckInScheduler.isRulesSetInFE.set(false);
    }

    private static File createTempFile(String content) throws IOException {
        File tempFile = File.createTempFile("preprocessor", ".yaml");
        Files.write(tempFile.toPath(), content.getBytes(StandardCharsets.UTF_8));
        tempFile.deleteOnExit(); // Clean up on JVM exit
        return tempFile;
    }

    /**
     * Test that system rules applied before user rules
     */
    @Test
    public void testPreprocessorRulesOrder() {
        InputStream stream =
                ProxyPreprocessorConfigurationTest.class.getResourceAsStream("preprocessor_rules_order_test.yaml");
        ProxyPreprocessorConfigManager config = new ProxyPreprocessorConfigManager();
        config.loadFromStream(stream);
        config
                .getSystemPreprocessor("2878")
                .forReportPoint()
                .addTransformer(new ReportPointAddPrefixTransformer("methodPrefix"));
        ReportPoint point =
                new ReportPoint(
                        "testMetric", System.currentTimeMillis(), 10, "host", "table", new HashMap<>());
        config.get("2878").get().forReportPoint().transform(point);
        assertEquals("methodPrefix.testMetric", point.getMetric());
    }

    @Test
    public void testMultiPortPreprocessorRules() {
        // test that preprocessor rules take priority over local rules
        InputStream stream =
                ProxyPreprocessorConfigurationTest.class.getResourceAsStream("preprocessor_rules_multiport.yaml");
        ProxyPreprocessorConfigManager config = new ProxyPreprocessorConfigManager();
        config.loadFromStream(stream);
        ReportPoint point =
                new ReportPoint(
                        "metric1", System.currentTimeMillis(), 4, "host", "table", new HashMap<>());
        config.get("2879").get().forReportPoint().transform(point);
        assertEquals("metric1", point.getMetric());
        assertEquals(1, point.getAnnotations().size());
        assertEquals("multiTagVal", point.getAnnotations().get("multiPortTagKey"));

        ReportPoint point1 =
                new ReportPoint(
                        "metric2", System.currentTimeMillis(), 4, "host", "table", new HashMap<>());
        config.get("1111").get().forReportPoint().transform(point1);
        assertEquals("metric2", point1.getMetric());
        assertEquals(1, point1.getAnnotations().size());
        assertEquals("multiTagVal", point1.getAnnotations().get("multiPortTagKey"));
    }

    @Test
    public void testEmptyRules() {
        InputStream stream = new ByteArrayInputStream("".getBytes());
        ProxyPreprocessorConfigManager config = new ProxyPreprocessorConfigManager();
        config.loadFromStream(stream);
    }

    @Test
    public void testLoadFERulesAndGetProxyConfigRules() {
        ProxyPreprocessorConfigManager config = new ProxyPreprocessorConfigManager();
        String testRules = tempRules;

        // Get initial state of the flag and reset
        boolean initialIsRulesSetInFE = ProxyCheckInScheduler.isRulesSetInFE.get();
        boolean initialPreprocessorRulesNeedUpdate = ProxyCheckInScheduler.preprocessorRulesNeedUpdate.get();
        ProxyCheckInScheduler.isRulesSetInFE.set(false); // Ensure FE rules are processed

        config.loadFERules(testRules);

        assertNotNull(config.get("3000").get());
        assertEquals(testRules, ProxyPreprocessorConfigManager.getProxyConfigRules());
        assertTrue(ProxyCheckInScheduler.preprocessorRulesNeedUpdate.get());

        // cleanup
        ProxyCheckInScheduler.isRulesSetInFE.set(initialIsRulesSetInFE);
        ProxyCheckInScheduler.preprocessorRulesNeedUpdate.set(initialPreprocessorRulesNeedUpdate);

        ProxyPreprocessorConfigManager.clearProxyConfigRules();
    }

    @Test
    public void testGetFileRulesReadsFileContent() throws IOException {
        String fileContent = "key: value\nlist:\n  - item1\n  - item2\n";
        File tempFile = createTempFile(fileContent);
        String readContent = ProxyPreprocessorConfigManager.getFileRules(tempFile.getAbsolutePath());

        assertEquals(fileContent, readContent);
    }

    /**
     * Test that getFileRules throws a RuntimeException when given a non-existent file path.
     */
    @Test(expected = RuntimeException.class)
    public void testGetFileRulesNonExistentFileThrowsRuntimeException() {
        ProxyPreprocessorConfigManager.getFileRules("/path/to/nonexistent/file.yaml");
    }

    /**
     * Test that getFileRules returns null for empty or null file names.
     */
    @Test
    public void testGetFileRulesEmptyFileNameReturnsNull() {
        assertNull(ProxyPreprocessorConfigManager.getFileRules(""));
        assertNull(ProxyPreprocessorConfigManager.getFileRules(null));
    }

    /**
     * Test loadFileIfModified skips loading if isRulesSetInFE is true.
     */
    @Test
    public void testLoadFileIfModifiedSkipsIfFERulesSet() throws IOException {
        File tempFile = createTempFile("tempRules");

        ProxyPreprocessorConfigManager config = EasyMock.partialMockBuilder(ProxyPreprocessorConfigManager.class)
                .addMockedMethod("loadFile", String.class)
                .withConstructor().createMock();

        replay(config);

        boolean initialIsRulesSetInFE = ProxyCheckInScheduler.isRulesSetInFE.get();
        ProxyCheckInScheduler.isRulesSetInFE.set(true);

        config.loadFileIfModified(tempFile.getAbsolutePath());

        // verify loadFile was not called
        verify(config);

        // cleanup
        ProxyCheckInScheduler.isRulesSetInFE.set(initialIsRulesSetInFE);
    }

    /**
     * Test that loadFileIfModified reloads rules and updates timestamp when file is updated.
     */
    @Test
    public void testLoadFileIfModifiedReloadsAndUpdatesTimestamp() throws IOException, FileNotFoundException {
        String expectedRules = tempRules;
        File tempFile = createTempFile(expectedRules); // Create a file with content

        Supplier<Long> mockTimeSupplier = EasyMock.mock(Supplier.class);

        expect(mockTimeSupplier.get()).andReturn(1000L).once();
        expect(mockTimeSupplier.get()).andReturn(tempFile.lastModified() + 100L).anyTimes(); // loadFileIfModified checks updated lastModified
        replay(mockTimeSupplier);

        ProxyPreprocessorConfigManager config = new ProxyPreprocessorConfigManager(mockTimeSupplier);

        config.loadFileIfModified(tempFile.getAbsolutePath());

        verify(mockTimeSupplier);

        // test loadFile results in updated proxyConfigRules
        assertEquals(expectedRules, ProxyPreprocessorConfigManager.getProxyConfigRules());
        assertTrue(ProxyCheckInScheduler.preprocessorRulesNeedUpdate.get());

        // cleanup
        ProxyCheckInScheduler.preprocessorRulesNeedUpdate.set(false);
    }

    /**
     * Test that loadFileIfModified does not reload if the file's last modified timestamp is not updated.
     */
    @Test
    public void testLoadFileIfModifiedDoesNotReloadIfFileNotNewer() throws IOException {
        File tempFile = createTempFile(tempRules);

        Supplier<Long> mockTimeSupplier = EasyMock.mock(Supplier.class);
        expect(mockTimeSupplier.get()).andReturn(tempFile.lastModified()).once();
        // mock same timestamp to indicate no time has passed
        expect(mockTimeSupplier.get()).andReturn(tempFile.lastModified()).anyTimes();
        replay(mockTimeSupplier);

        ProxyPreprocessorConfigManager config = new ProxyPreprocessorConfigManager(mockTimeSupplier);
        config.loadFileIfModified(tempFile.getAbsolutePath());

        // Verify mocks (will fail if loadFile or getFileRules were unexpectedly called)
        verify(mockTimeSupplier);

        // Assert that proxyConfigRules was not updated
        assertNull(ProxyPreprocessorConfigManager.getProxyConfigRules());
    }

    @Test
    public void testLoadFileIfModifiedHandlesExceptionInLoadFile() throws IOException, FileNotFoundException {
        File tempFile = createTempFile("rules that will cause exception");

        Supplier<Long> mockTimeSupplier = EasyMock.mock(Supplier.class);
        expect(mockTimeSupplier.get()).andReturn(1000L).once(); // For constructor
        expect(mockTimeSupplier.get()).andReturn(tempFile.lastModified() + 100L).anyTimes(); // Simulate newer file
        replay(mockTimeSupplier);

        ProxyPreprocessorConfigManager config = EasyMock.partialMockBuilder(ProxyPreprocessorConfigManager.class)
                .addMockedMethod("loadFile", String.class)
                .withConstructor(mockTimeSupplier)
                .createMock();

        config.loadFile(tempFile.getAbsolutePath());
        expectLastCall().andThrow(new FileNotFoundException("Simulated error")).once();

        replay(config);

        config.loadFileIfModified(tempFile.getAbsolutePath());

        verify(mockTimeSupplier, config);
    }

    /**
     * A forward rule that references a registered multicasting tenant must be accepted without
     * any exception during loadFERules.
     */
    @Test
    public void testLoadFERulesAcceptsForwardRuleTargetingRegisteredTenant() {
        TokenManager.addTenant("sidecar",
            new TokenWorkerWF("sidecar-token", "https://sidecar.corp/api/"));

        ProxyPreprocessorConfigManager config = new ProxyPreprocessorConfigManager();
        // Should not throw — "sidecar" is in the registered tenant set.
        config.loadFERules(FORWARD_RULE_SIDECAR);
        assertNotNull(config.get("2878").get());
    }

    /**
     * When {@code setDefaultTenant("MyAlias")} is called, a forward rule that references the alias
     * must be accepted as valid (the alias is added to the valid-tenant set).
     */
    @Test
    public void testSetDefaultTenantAliasIsAcceptedAsForwardTarget() {
        // No multicasting tenants — only "central" and the alias are valid.
        ProxyPreprocessorConfigManager config = new ProxyPreprocessorConfigManager();
        config.setDefaultTenant("MyAlias");

        // Should not throw — the alias is now part of the valid set.
        config.loadFERules(FORWARD_RULE_ALIAS);
        assertNotNull(config.get("2878").get());
    }

    /**
     * When {@code setDefaultTenant(null)}, the alias is not added to the valid-tenant set, so a
     * forward rule targeting an alias name must be rejected.
     */
    @Test(expected = IllegalArgumentException.class)
    public void testSetDefaultTenantNullDoesNotAddAliasToValidSet() {
        ProxyPreprocessorConfigManager config = new ProxyPreprocessorConfigManager();
        config.setDefaultTenant(null);

        // "MyAlias" is not in the valid set → must throw.
        config.loadFERules(FORWARD_RULE_ALIAS);
    }

    /**
     * A forward rule that references a tenant not registered in TokenManager must be rejected with
     * an IllegalArgumentException when loaded via loadFERules.
     */
    @Test(expected = IllegalArgumentException.class)
    public void testLoadFERulesRejectsUnregisteredForwardTarget() {
        ProxyPreprocessorConfigManager config = new ProxyPreprocessorConfigManager();
        // "unknownTenant" is not registered — must throw.
        config.loadFERules(FORWARD_RULE_UNKNOWN);
    }

    /**
     * When {@code defaultTenant} is NOT configured (alias is null) and a forward rule references
     * a name that would have been the alias, the error message must include a hint pointing the
     * operator to the {@code defaultTenant} config field.
     */
    @Test
    public void testMissingDefaultTenantHintInErrorMessage() {
        ProxyPreprocessorConfigManager config = new ProxyPreprocessorConfigManager();
        // defaultTenant is not set → alias is null.
        try {
            config.loadFERules(FORWARD_RULE_ALIAS); // targets "MyAlias"
            Assert.fail("Expected IllegalArgumentException");
        } catch (IllegalArgumentException ex) {
            assertTrue("Error message must mention the unknown tenant name",
                ex.getMessage().contains("MyAlias"));
            assertTrue("Error message must hint at the defaultTenant config field",
                ex.getMessage().contains("defaultTenant"));
        }
    }

    /**
     * Exact scenario: defaultTenant IS configured, NO multicastingTenants, forward rule targets
     * a tenant that would need to be a multicastingTenant.
     * The error must name the unknown tenant, must NOT mention defaultTenant (that's not the issue),
     * and MUST hint that multicastingTenants are not configured.
     */
    @Test
    public void testDefaultTenantSetButNoMulticastingTenantsForwardRuleHints() {
        // Mirrors: config.ini has defaultTenant=Localdev but no multicastingTenants.
        // preprocessor-rules.yaml has forward rule targeting "Master".
        ProxyPreprocessorConfigManager config = new ProxyPreprocessorConfigManager();
        config.setDefaultTenant("Localdev"); // alias IS set, but no multicastingTenants registered

        try {
            config.loadFERules(FORWARD_RULE_SIDECAR); // "sidecar" is not a multicastingTenant
            Assert.fail("Expected IllegalArgumentException");
        } catch (IllegalArgumentException ex) {
            assertTrue("Error must name the unknown tenant",
                ex.getMessage().contains("sidecar"));
            assertFalse("Error must NOT mention defaultTenant (that config is fine)",
                ex.getMessage().contains("defaultTenant"));
            assertTrue("Error must hint at missing multicastingTenants config",
                ex.getMessage().contains("multicastingTenants"));
            assertTrue("Error must show the config fields needed",
                ex.getMessage().contains("multicastingTenantName_X"));
        }
    }

    /**
     * When multicastingTenants ARE configured and the forward rule targets a genuinely wrong
     * tenant name, the plain error is thrown without any hint (operator knows what to fix).
     */
    @Test
    public void testMultiTenantModeWithWrongTenantNameGivesPlainError() {
        TokenManager.addTenant("sidecar",
            new TokenWorkerWF("sidecar-token", "https://sidecar.corp/api/"));
        TokenManager.addTenant("extra",
            new TokenWorkerWF("extra-token", "https://extra.corp/api/"));

        ProxyPreprocessorConfigManager config = new ProxyPreprocessorConfigManager();
        config.setDefaultTenant("MyAlias"); // alias IS set, multi-tenant mode active (size > 1)

        try {
            // "unknownTenant" is not among the registered multicastingTenants.
            config.loadFERules(FORWARD_RULE_UNKNOWN);
            Assert.fail("Expected IllegalArgumentException");
        } catch (IllegalArgumentException ex) {
            assertTrue("Error must name the unknown tenant",
                ex.getMessage().contains("unknownTenant"));
            assertFalse("Error must NOT mention defaultTenant",
                ex.getMessage().contains("defaultTenant"));
            assertFalse("Error must NOT hint at missing multicastingTenants (they ARE configured)",
                ex.getMessage().contains("Multi-tenant routing is not configured"));
        }
    }

    // -------------------------------------------------------------------------
    // Tests: defaultTenant is required when forward rules exist in multi-tenant mode
    // -------------------------------------------------------------------------

    /**
     * Multi-tenant mode (multicastingTenants registered) + forward rules + no defaultTenant
     * must fail at startup with a clear error message.
     * This is the exact scenario reported: config.ini has multicastingTenants but no defaultTenant,
     * and preprocessor-rules.yaml has forward rules.
     */
    @Test
    public void testMultiTenantWithForwardRulesRequiresDefaultTenant() {
        // Register multicasting tenants (simulates config.ini multicastingTenants=2)
        TokenManager.addTenant("Master",
            new TokenWorkerWF("master-token", "https://master.corp/api/"));
        TokenManager.addTenant("wfdev",
            new TokenWorkerWF("wfdev-token", "https://wfdev.wavefront.com/api/"));

        ProxyPreprocessorConfigManager config = new ProxyPreprocessorConfigManager();
        // defaultTenant is NOT set — alias is null.
        try {
            config.loadFERules(FORWARD_RULE_SIDECAR.replace("sidecar", "Master"));
            Assert.fail("Expected IllegalArgumentException: defaultTenant is required");
        } catch (IllegalArgumentException ex) {
            assertTrue("Error must mention 'defaultTenant'", ex.getMessage().contains("defaultTenant"));
            assertTrue("Error must suggest an example fix", ex.getMessage().contains("config.ini"));
            assertTrue("Error must list registered tenants",
                ex.getMessage().contains("Master") && ex.getMessage().contains("wfdev"));
        }
    }

    /**
     * Multi-tenant mode + forward rules + defaultTenant IS set → no error.
     */
    @Test
    public void testMultiTenantWithForwardRulesAndDefaultTenantSucceeds() {
        TokenManager.addTenant("sidecar",
            new TokenWorkerWF("sidecar-token", "https://sidecar.corp/api/"));

        ProxyPreprocessorConfigManager config = new ProxyPreprocessorConfigManager();
        config.setDefaultTenant("MyCluster"); // alias IS set

        // Must not throw: sidecar is registered, defaultTenant is configured.
        config.loadFERules(FORWARD_RULE_SIDECAR);
        assertNotNull("Rules must be loaded when defaultTenant is configured", config.get("2878").get());
    }

    /**
     * Multi-tenant mode (2+ aliases registered) + ANY preprocessor rules + no defaultTenant
     * → error, regardless of whether forward rules are present.
     * This enforces the simpler invariant: multicastingTenants implies forwarding intent,
     * so defaultTenant is always required when a preprocessor file is loaded in multi-tenant mode.
     */
    @Test
    public void testMultiTenantTransformOnlyRulesAlsoRequireDefaultTenant() {
        // Simulate "central" + "Master" registered (multi-tenant mode).
        TokenManager.addTenant(APIContainer.CENTRAL_TENANT_NAME,
            new TokenWorkerWF("token", "https://central.corp/api/"));
        TokenManager.addTenant("Master",
            new TokenWorkerWF("master-token", "https://master.corp/api/"));

        ProxyPreprocessorConfigManager config = new ProxyPreprocessorConfigManager();
        // defaultTenant is NOT set.

        String transformOnlyRules = "'2878':\n"
            + "  - rule: add-env-tag\n"
            + "    action: addTag\n"
            + "    tag: env\n"
            + "    value: production\n";
        try {
            config.loadFERules(transformOnlyRules);
            Assert.fail("Expected IllegalArgumentException: defaultTenant required in multi-tenant mode");
        } catch (IllegalArgumentException ex) {
            assertTrue("Error must mention defaultTenant", ex.getMessage().contains("defaultTenant"));
        }
    }

    /**
     * Multi-tenant mode + transform-only rules + defaultTenant IS set → no error.
     */
    @Test
    public void testMultiTenantTransformOnlyRulesSucceedWithDefaultTenant() {
        TokenManager.addTenant(APIContainer.CENTRAL_TENANT_NAME,
            new TokenWorkerWF("token", "https://central.corp/api/"));
        TokenManager.addTenant("Master",
            new TokenWorkerWF("master-token", "https://master.corp/api/"));

        ProxyPreprocessorConfigManager config = new ProxyPreprocessorConfigManager();
        config.setDefaultTenant(APIContainer.CENTRAL_TENANT_NAME); // required in multi-tenant mode

        String transformOnlyRules = "'2878':\n"
            + "  - rule: add-env-tag\n"
            + "    action: addTag\n"
            + "    tag: env\n"
            + "    value: production\n";
        config.loadFERules(transformOnlyRules); // must NOT throw
        assertNotNull("Transform-only rules must load when defaultTenant is configured",
            config.get("2878").get());
    }

    /**
     * Single-tenant mode (no multicastingTenants) + forward rules + no defaultTenant → no error.
     * The strict check only applies in multi-tenant mode.
     */
    @Test
    public void testSingleTenantWithForwardRulesDoesNotRequireDefaultTenant() {
        // Only "central" is registered (simulates single-tenant proxy).
        // No multicasting tenants added to TokenManager.

        ProxyPreprocessorConfigManager config = new ProxyPreprocessorConfigManager();
        config.setDefaultTenant(null); // explicitly no alias

        // Forward rule targeting "central" is always valid (central is always in the valid set).
        String forwardToCentral = "'2878':\n"
            + "  - rule: route-to-central\n"
            + "    action: forward\n"
            + "    scope: metricName\n"
            + "    match: '.*'\n"
            + "    tenants:\n"
            + "      - central\n";
        config.loadFERules(forwardToCentral); // must NOT throw
        assertNotNull("Forward-to-central must load in single-tenant mode without defaultTenant",
            config.get("2878").get());
    }

    /**
     * A forward rule that references a tenant not registered in TokenManager must be rejected with
     * an IllegalArgumentException when loaded via loadFile.
     */
    @Test(expected = IllegalArgumentException.class)
    public void testLoadFileRejectsUnregisteredForwardTarget() throws Exception {
        File tempFile = createTempFile(FORWARD_RULE_UNKNOWN);
        ProxyPreprocessorConfigManager config = new ProxyPreprocessorConfigManager();
        // "unknownTenant" is not registered — must throw.
        config.loadFile(tempFile.getAbsolutePath());
    }

    /**
     * Calling loadFile directly (not via loadFileIfModified) must populate
     * proxyConfigRules with the file content and set preprocessorRulesNeedUpdate to true.
     */
    @Test
    public void testLoadFileDirectlySetsProxyConfigRulesAndFlag() throws Exception {
        TokenManager.addTenant(APIContainer.CENTRAL_TENANT_NAME,
            new TokenWorkerWF("token", "https://central.corp/api/"));
        TokenManager.addTenant("sidecar",
            new TokenWorkerWF("sidecar-token", "https://sidecar.corp/api/"));

        File tempFile = createTempFile(FORWARD_RULE_SIDECAR);
        ProxyPreprocessorConfigManager config = new ProxyPreprocessorConfigManager();
        // Multi-tenant (central + sidecar) with forward rules requires defaultTenant.
        config.setDefaultTenant(APIContainer.CENTRAL_TENANT_NAME);

        ProxyCheckInScheduler.preprocessorRulesNeedUpdate.set(false);
        config.loadFile(tempFile.getAbsolutePath());

        assertEquals(FORWARD_RULE_SIDECAR, ProxyPreprocessorConfigManager.getProxyConfigRules());
        assertTrue(ProxyCheckInScheduler.preprocessorRulesNeedUpdate.get());
    }

    /**
     * Test setUpConfigFileMonitoring schedules a TimerTask that periodically calls loadFileIfModified.
     */
    @Test
    public void testSetUpConfigFileMonitoring() throws InterruptedException {
        final CountDownLatch latch = new CountDownLatch(1);

        ProxyPreprocessorConfigManager config = EasyMock.partialMockBuilder(ProxyPreprocessorConfigManager.class)
            .addMockedMethod("loadFileIfModified", String.class)
            .withConstructor()
            .createMock();

        String dummyFileName = "test.yaml";
        int checkInterval = 100; // 100ms

        config.loadFileIfModified(dummyFileName);
        // countdown once loadFileIfModified is called by timer to mock time passing
        expectLastCall().andAnswer(() -> {
            latch.countDown();
            return null;
        }).atLeastOnce();

        replay(config);

        config.setUpConfigFileMonitoring(dummyFileName, checkInterval);

        assertTrue("loadFileIfModified was not called within the timeout.", latch.await(500, TimeUnit.MILLISECONDS));
        verify(config);
    }
}
