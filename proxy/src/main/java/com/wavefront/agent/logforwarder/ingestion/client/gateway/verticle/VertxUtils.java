/*
 * Copyright (c) 2019 VMware, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License.  You may obtain a copy of
 * the License at http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, without warranties or
 * conditions of any kind, EITHER EXPRESS OR IMPLIED.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package com.wavefront.agent.logforwarder.ingestion.client.gateway.verticle;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.util.zip.GZIPInputStream;

import io.vertx.core.buffer.Buffer;
import io.vertx.ext.web.RoutingContext;

public class VertxUtils {

    private static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
    private static int DEFAULT_LOG_FORWARDER_VERTX_PORT = 8101;
    private static final int BUFFER_SIZE = 1024;

    public static int getLogForwarderVertxPort() {
        return DEFAULT_LOG_FORWARDER_VERTX_PORT;
    }

    public static String getLogForwarderVertxHost() {
        return "http://localhost:" + getLogForwarderVertxPort();
    }

    public static int getVertxRestApiPort(int port) {
        return port + 101;
    }

    public static int getBodySize(RoutingContext context) {
        return context.getBody() != null ? context.getBody().length() : 0;
    }

    /**
     * Decompresses the buffer with gzip and returns decompressed buffer object
     * @param buffer
     * @return
     */
    public static Buffer decompressGzip(Buffer buffer) {
        try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
             GZIPInputStream gzipInputStream = new GZIPInputStream(new ByteArrayInputStream(buffer.getBytes()))) {
            byte[] byteBuffer = new byte[BUFFER_SIZE];
            int bytesRead;
            while ((bytesRead = gzipInputStream.read(byteBuffer)) > 0) {
                baos.write(byteBuffer, 0, bytesRead);
            }
            return Buffer.buffer(baos.toByteArray());
        } catch (IOException ioe) {
            logger.error("Unable to decompress gzip", ioe);
            return null;
        }
    }

    // For testing purpose.
    public static void setLogForwarderVertxPort(int port) {
        DEFAULT_LOG_FORWARDER_VERTX_PORT = port;
    }
}
