package com.wavefront.agent.preprocessor;

import com.wavefront.agent.formatter.DataFormat;
import com.wavefront.api.agent.preprocessor.ReportableEntityPreprocessor;
import com.wavefront.api.agent.preprocessor.Preprocessor;
import com.wavefront.data.ReportableEntityType;
import org.easymock.EasyMock;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.io.PrintStream;
import java.util.Collections;
import java.util.function.Supplier;

import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.replay;
import static org.easymock.EasyMock.verify;
import static org.junit.Assert.*;

public class InteractivePreprocessorTesterTest {

    private final ByteArrayOutputStream outContent = new ByteArrayOutputStream();
    private final ByteArrayOutputStream errContent = new ByteArrayOutputStream();
    private final PrintStream originalOut = System.out;
    private final PrintStream originalErr = System.err;
    private final InputStream originalIn = System.in;

    @Before
    public void setUp() {
        System.setOut(new PrintStream(outContent));
        System.setErr(new PrintStream(errContent));
        // System.in will be set per test method
    }

    @After
    public void tearDown() {
        System.setOut(originalOut);
        System.setErr(originalErr);
        System.setIn(originalIn);
    }

    private void setSystemIn(String input) {
        System.setIn(new ByteArrayInputStream(input.getBytes()));
    }

    private String getOutput() {
        return outContent.toString().trim();
    }

    @Test
    public void testInteractiveTest_TraceEntity_ReportsSpan() {
        setSystemIn("test-span\n");
        Supplier<ReportableEntityPreprocessor> mockPreprocessorSupplier = EasyMock.mock(Supplier.class);
        ReportableEntityPreprocessor mockPreprocessor = EasyMock.mock(ReportableEntityPreprocessor.class);
        expect(mockPreprocessorSupplier.get()).andReturn(mockPreprocessor).anyTimes();

        // Preprocessor does not block or modify for this test, so no specific expectations on its methods
        expect(mockPreprocessor.forSpan()).andReturn(new Preprocessor() {}).anyTimes();
        expect(mockPreprocessor.forPointLine()).andReturn(new Preprocessor<>()).anyTimes();

        replay(mockPreprocessorSupplier, mockPreprocessor);

        InteractivePreprocessorTester tester = new InteractivePreprocessorTester(
                mockPreprocessorSupplier,
                ReportableEntityType.TRACE,
                "2878",
                Collections.emptyList()
        );

        boolean hasNext = tester.interactiveTest();

        assertFalse(hasNext);
        // Verify System.out contains the serialized span. The actual spanId/traceId are random, so use regex.
        String expectedOutputRegex = "Rejected: test-span";
        assertTrue("Output should contain serialized span matching regex: " + getOutput(), getOutput().matches(expectedOutputRegex));

        verify(mockPreprocessorSupplier, mockPreprocessor);
    }

    @Test
    public void testInteractiveTest_PointEntity_ReportsPoint() {
        setSystemIn("some.metric 10.0 source=test\n");
        Supplier<ReportableEntityPreprocessor> mockPreprocessorSupplier = EasyMock.mock(Supplier.class);
        ReportableEntityPreprocessor mockPreprocessor = EasyMock.mock(ReportableEntityPreprocessor.class);
        expect(mockPreprocessorSupplier.get()).andReturn(mockPreprocessor).anyTimes();
        expect(mockPreprocessor.forReportPoint()).andReturn(new Preprocessor() {}).anyTimes();
        expect(mockPreprocessor.forPointLine()).andReturn(new Preprocessor<>()).anyTimes();

        replay(mockPreprocessorSupplier, mockPreprocessor);

        InteractivePreprocessorTester tester = new InteractivePreprocessorTester(
                mockPreprocessorSupplier,
                ReportableEntityType.POINT,
                "2878",
                Collections.emptyList()
        );

        boolean hasNext = tester.interactiveTest();

        assertFalse("Should indicate more input, as 'some.metric...' was followed by a newline", hasNext);

        String actualOutput = outContent.toString().trim();
        assertTrue("Output should contain serialized report point", actualOutput.startsWith("\"some.metric\" 10.0"));
        assertTrue("Output should contain serialized report point", actualOutput.endsWith("source=\"test\""));

        verify(mockPreprocessorSupplier, mockPreprocessor);
    }
}
