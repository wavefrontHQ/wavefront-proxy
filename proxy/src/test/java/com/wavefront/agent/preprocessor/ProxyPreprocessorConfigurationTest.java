package com.wavefront.agent.preprocessor;

import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.expectLastCall;
import static org.easymock.EasyMock.replay;
import static org.easymock.EasyMock.verify;
import static org.junit.Assert.assertEquals;
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
import com.wavefront.api.agent.preprocessor.ReportPointAddPrefixTransformer;
import org.easymock.EasyMock;
import org.junit.Assert;
import org.junit.Test;
import wavefront.report.ReportPoint;

public class ProxyPreprocessorConfigurationTest {
    final String tempRules = "'3000':\n  - rule    : drop-ts-tag\n    action  : dropTag\n    tag   : ts\n    match  : ts.*";

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
