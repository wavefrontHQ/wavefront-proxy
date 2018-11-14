package com.wavefront.agent;

import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.StatusLine;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpRequestBase;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.message.BasicHeader;
import org.easymock.Capture;
import org.easymock.EasyMock;
import org.easymock.IArgumentMatcher;

import java.io.ByteArrayInputStream;

public class TestUtils {
  public static <T extends HttpRequestBase> T httpEq(HttpRequestBase request) {
    EasyMock.reportMatcher(new IArgumentMatcher() {
      @Override
      public boolean matches(Object o) {
        return o instanceof HttpRequestBase &&
            o.getClass().getCanonicalName().equals(request.getClass().getCanonicalName()) &&
            ((HttpRequestBase) o).getMethod().equals(request.getMethod()) &&
            ((HttpRequestBase) o).getProtocolVersion().equals(request.getProtocolVersion()) &&
            ((HttpRequestBase) o).getURI().equals(request.getURI());
      }

      @Override
      public void appendTo(StringBuffer stringBuffer) {
        stringBuffer.append("httpEq(");
        stringBuffer.append(request.toString());
        stringBuffer.append(")");
      }
    });
    return null;
  }

  public static void expectHttpResponse(HttpClient httpClient, HttpRequestBase req,
                                        byte[] content, int httpStatus) throws Exception {
    HttpResponse response = EasyMock.createMock(HttpResponse.class);
    HttpEntity entity = EasyMock.createMock(HttpEntity.class);
    StatusLine line = EasyMock.createMock(StatusLine.class);

    EasyMock.expect(response.getStatusLine()).andReturn(line).anyTimes();
    EasyMock.expect(response.getEntity()).andReturn(entity).anyTimes();
    EasyMock.expect(line.getStatusCode()).andReturn(httpStatus).anyTimes();
    EasyMock.expect(line.getReasonPhrase()).andReturn("OK").anyTimes();
    EasyMock.expect(entity.getContent()).andReturn(new ByteArrayInputStream(content)).anyTimes();
    EasyMock.expect(entity.getContentLength()).andReturn((long) content.length).atLeastOnce();
    EasyMock.expect(entity.getContentType()).andReturn(new BasicHeader("Content-Type", "application/json")).anyTimes();

    EasyMock.expect(httpClient.execute(httpEq(req))).andReturn(response).once();

    EasyMock.replay(httpClient, response, entity, line);
  }
}
