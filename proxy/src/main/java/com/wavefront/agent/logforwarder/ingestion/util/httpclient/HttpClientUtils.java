///*
// * Copyright (c) 2019 VMware, Inc. All Rights Reserved.
// *
// * Licensed under the Apache License, Version 2.0 (the "License"); you may not
// * use this file except in compliance with the License.  You may obtain a copy of
// * the License at http://www.apache.org/licenses/LICENSE-2.0
// *
// * Unless required by applicable law or agreed to in writing, software distributed
// * under the License is distributed on an "AS IS" BASIS, without warranties or
// * conditions of any kind, EITHER EXPRESS OR IMPLIED.  See the License for the
// * specific language governing permissions and limitations under the License.
// */
//
//package com.wavefront.agent.logforwarder.ingestion.util.httpclient;
//
//import com.vmware.log.forwarder.host.LogForwarderConfigProperties;
//import com.vmware.log.forwarder.utils.JsonUtils.CustomByteArrayOutputStream;
//import com.wavefront.agent.logforwarder.config.LogForwarderConfigProperties;
//import com.wavefront.agent.logforwarder.ingestion.processors.util.JsonUtils;
//
//import org.apache.http.HttpHost;
//import org.apache.http.HttpResponse;
//import org.apache.http.auth.AuthScope;
//import org.apache.http.auth.UsernamePasswordCredentials;
//import org.apache.http.client.CredentialsProvider;
//import org.apache.http.client.config.RequestConfig;
//import org.apache.http.config.Registry;
//import org.apache.http.config.RegistryBuilder;
//import org.apache.http.conn.ssl.SSLConnectionSocketFactory;
//import org.apache.http.impl.client.BasicCredentialsProvider;
//import org.apache.http.impl.nio.client.CloseableHttpAsyncClient;
//import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
//import org.apache.http.impl.nio.client.HttpAsyncClients;
//import org.apache.http.impl.nio.conn.PoolingNHttpClientConnectionManager;
//import org.apache.http.impl.nio.reactor.DefaultConnectingIOReactor;
//import org.apache.http.nio.conn.NoopIOSessionStrategy;
//import org.apache.http.nio.conn.SchemeIOSessionStrategy;
//import org.apache.http.nio.conn.ssl.SSLIOSessionStrategy;
//import org.apache.http.nio.reactor.ConnectingIOReactor;
//import org.apache.http.ssl.TrustStrategy;
//import org.slf4j.Logger;
//import org.slf4j.LoggerFactory;
//
//import java.io.BufferedReader;
//import java.io.ByteArrayOutputStream;
//import java.io.IOException;
//import java.io.InputStreamReader;
//import java.lang.invoke.MethodHandles;
//import java.nio.charset.StandardCharsets;
//import java.security.cert.X509Certificate;
//import java.util.Map;
//import java.util.concurrent.ConcurrentHashMap;
//import java.util.concurrent.Executors;
//import java.util.concurrent.ScheduledExecutorService;
//import java.util.concurrent.TimeUnit;
//import java.util.zip.Deflater;
//import java.util.zip.GZIPOutputStream;
//
//import javax.net.ssl.SSLContext;
//
//public class HttpClientUtils {
//
//    private static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
//
//    private static int MAX_CONN_TOTAL = 1_000;
//    private static int MAX_CONN_PER_ROUTE = 1_000;
//    public static int BUFFER_SIZE = 10 * 1024; // 10KB
//    private static Map<String, CloseableHttpAsyncClient> identifierVsHttpClient = new ConcurrentHashMap<>();
//
//    //Proxy Env variables
//    public static final String PROPERTY_PROXY_SERVER = "http_proxy_server";
//    public static final String PROPERTY_PROXY_PORT = "http_proxy_port";
//    public static final String PROPERTY_PROXY_USERNAME = "http_proxy_username";
//    public static final String PROPERTY_PROXY_PASSWORD = "http_proxy_password";
//
//    /**
//     * get http client for the identifier
//     *
//     * @param identifier identifier name, not null
//     * @return http client for the identifier
//     */
//    public static CloseableHttpAsyncClient getHttpClient(String identifier) {
//        return identifierVsHttpClient.get(identifier);
//    }
//
//    /**
//     * remove http client for the identifier.
//     *
//     * @param identifier identifier name, not null
//     * @return http client for the identifier
//     */
//    public static CloseableHttpAsyncClient removeHttpClient(String identifier) {
//        return identifierVsHttpClient.remove(identifier);
//    }
//
//    /**
//     * create async http client for the identifier
//     *
//     * @param identifier    identifier name, not null
//     * @param timeoutMillis timeout in millis
//     */
//    public static synchronized void createAsyncHttpClient(String identifier, int timeoutMillis,
//                                                          Boolean useNetworkProxy) {
//        if (! identifierVsHttpClient.containsKey(identifier)) {
//            logger.info("Creating Async HTTP Client");
//            CloseableHttpAsyncClient httpAsyncClient = createAsyncHttpClient(timeoutMillis, useNetworkProxy);
//            identifierVsHttpClient.put(identifier, httpAsyncClient);
//        }
//    }
//
//    /**
//     * read the response from the http response object
//     *
//     * @param response http response, not null
//     * @return response in string
//     * @throws IOException
//     */
//    public static String readResponse(HttpResponse response) throws IOException {
//        BufferedReader br =
//                new BufferedReader(new InputStreamReader((response.getEntity().getContent())));
//        StringBuilder output = new StringBuilder();
//        String line;
//        while ((line = br.readLine()) != null) {
//            output.append(line);
//        }
//        return output.toString();
//    }
//
//    /**
//     * create async http client
//     *
//     * @param timeoutMillis timeout in millis
//     * @return http client created
//     */
//    private static CloseableHttpAsyncClient createAsyncHttpClient(int timeoutMillis, Boolean useNetworkProxy) {
//        ConnectingIOReactor ioReactor = null;
//        try {
//            TrustStrategy acceptingTrustStrategy = (X509Certificate[] chain, String authType) -> true;
//            SSLContext sslContext = org.apache.http.ssl.SSLContexts.custom()
//                    .loadTrustMaterial(null, acceptingTrustStrategy)
//                    .build();
//
//            Registry<SchemeIOSessionStrategy> registry = RegistryBuilder.<SchemeIOSessionStrategy>create()
//                    .register("http", NoopIOSessionStrategy.INSTANCE)
//                    .register("https", new SSLIOSessionStrategy(
//                            sslContext, null, null, SSLConnectionSocketFactory.ALLOW_ALL_HOSTNAME_VERIFIER)).build();
//
//            /**
//             connectionRequestTimeout -- timeout to get connection from http pool -- timeoutMillis / 2
//             connectTimeut -- time to establish connection -- timeoutMillis / 2
//             socketTimeout -- inactivity time to get data  -- passed as parameter
//             */
//            RequestConfig.Builder configBuilder =
//                    RequestConfig.custom().setConnectionRequestTimeout(timeoutMillis / 2)
//                            .setConnectTimeout(timeoutMillis / 2)
//                            .setSocketTimeout(timeoutMillis);
//
//            CredentialsProvider credsProvider = null;
//
//            if (Boolean.TRUE == useNetworkProxy) {
//                //Configure proxy server if host and port are set
//                ProxyDetails proxyDetails = getProxyDetails();
//                logger.info("Proxy details: " + proxyDetails.toString());
//                if (proxyDetails.host != null) {
//                    logger.warn(String.format("Configuring Network proxy with host and port %s:%s",
//                            proxyDetails.host, proxyDetails.port));
//                    HttpHost proxy = new HttpHost(proxyDetails.host, proxyDetails.port);
//                    configBuilder.setProxy(proxy);
//                } else {
//                    logger.info("Network proxy is not configured");
//                }
//                //Configure proxy credentials if required
//                if (proxyDetails.host != null && proxyDetails.username != null) {
//                    logger.warn(String.format("Configuring Network proxy with username %s", proxyDetails.username));
//                    credsProvider = new BasicCredentialsProvider();
//                    credsProvider.setCredentials(
//                            new AuthScope(proxyDetails.host, proxyDetails.port),
//                            new UsernamePasswordCredentials(proxyDetails.username, proxyDetails.password));
//                }
//            }
//
//            RequestConfig config = configBuilder.build();
//
//            ioReactor = new DefaultConnectingIOReactor();
//            PoolingNHttpClientConnectionManager cmAsync =
//                    new PoolingNHttpClientConnectionManager(ioReactor, registry);
//            cmAsync.setMaxTotal(MAX_CONN_TOTAL);
//            cmAsync.setDefaultMaxPerRoute(MAX_CONN_PER_ROUTE);
//
//            HttpAsyncClientBuilder asyncClientBuilder = HttpAsyncClients.custom()
//                    .setDefaultRequestConfig(config)
//                    .setSSLHostnameVerifier(SSLConnectionSocketFactory.ALLOW_ALL_HOSTNAME_VERIFIER)
//                    .setConnectionManager(cmAsync);
//
//            if (credsProvider != null) {
//                asyncClientBuilder.setDefaultCredentialsProvider(credsProvider);
//            }
//
//            CloseableHttpAsyncClient asynClient = asyncClientBuilder.build();
//            asynClient.start();
//
//            // Starting idle connection evictor in a loop to get rid of idle and expired connections
//            ScheduledExecutorService executor = Executors.newScheduledThreadPool(1);
//            executor.scheduleAtFixedRate(() -> {
//                cmAsync.closeExpiredConnections();
//                cmAsync.closeIdleConnections(timeoutMillis, TimeUnit.MILLISECONDS);
//            }, 10, 10, TimeUnit.SECONDS);
//
//            return asynClient;
//        } catch (Exception e) {
//            throw new RuntimeException("Error creating AsyncHttpClient", e);
//        }
//    }
//
//    static class ProxyDetails {
//        public String host;
//        public int port;
//        public String username;
//        public String password;
//
//        @Override
//        public String toString() {
//            return "ProxyDetails{" +
//                    "host='" + host + '\'' +
//                    ", port=" + port +
//                    ", username='" + username + '\'' +
//                    '}';
//        }
//    }
//
//    private static ProxyDetails getProxyDetails() {
//        ProxyDetails proxyDetails = new ProxyDetails();
//
//        if (System.getenv(PROPERTY_PROXY_SERVER) == null) {
//            return proxyDetails;
//        }
//
//        try {
//            proxyDetails.host = System.getenv(PROPERTY_PROXY_SERVER);
//            proxyDetails.port = Integer.parseInt(System.getenv(PROPERTY_PROXY_PORT));
//            proxyDetails.username = System.getenv(PROPERTY_PROXY_USERNAME);
//            proxyDetails.password = System.getenv(PROPERTY_PROXY_PASSWORD);
//        } catch (NumberFormatException nfe) {
//            String msg = "Error configuring network proxy while parsing port";
//            logger.error(msg, nfe);
//            throw new RuntimeException(msg, nfe);
//        }
//
//        return proxyDetails;
//    }
//
//    /**
//     * compress the JSON into byte-array
//     *
//     * @param json input json, not null
//     * @return compressed json as byte array
//     */
//    public static byte[] compress(String json) {
//        ByteArrayOutputStream baos = new ByteArrayOutputStream(BUFFER_SIZE);
//        try (GZIPOutputStream gzos = new GZIPOutputStream(baos, BUFFER_SIZE) {
//            {
//                if (useBestSpeed()) {
//                    def.setLevel(Deflater.BEST_SPEED);
//                } else {
//                    def.setLevel(Deflater.BEST_COMPRESSION);
//                }
//            }
//        }) {
//            gzos.write(json.getBytes(StandardCharsets.UTF_8));
//        } catch (Exception e) {
//            throw new RuntimeException(e.getMessage(), e);
//        }
//        return baos.toByteArray();
//    }
//
//    public static JsonUtils.CustomByteArrayOutputStream compress(byte[] b, int off, int len, CustomByteArrayOutputStream baos) {
//        try (GZIPOutputStream gzos = new GZIPOutputStream(baos, BUFFER_SIZE) {
//            {
//                if (useBestSpeed()) {
//                    def.setLevel(Deflater.BEST_SPEED);
//                } else {
//                    def.setLevel(Deflater.BEST_COMPRESSION);
//                }
//            }}
//        ) {
//            gzos.write(b, off, len);
//        } catch (Exception e) {
//            throw new RuntimeException(e.getMessage(), e);
//        }
//        return baos;
//    }
//
//    private static boolean useBestSpeed() {
//        return LogForwarderConfigProperties.logForwarderArgs != null &&
//                Boolean.TRUE.equals(LogForwarderConfigProperties.logForwarderArgs.useBestSpeedCompression);
//    }
//}
