package com.wavefront.agent.logforwarder.config;

import com.vmware.xenon.common.ServiceHost;//TODO Remove xenon and port this

/**
 * @author Manoj Ramakrishnan (rmanoj@vmware.com).
 * @since 9/15/21 1:24 PM
 */
public class LogForwarderArgs extends ServiceHost.Arguments {

  public String addFwderIdInEvent;

  public String sddcId;
  public String orgId;
  public String region;
  public String orgType;
  public String sddcEnv;

  public String dimensionMspMasterOrgId;

  /**
   * this is the proxy id (rdc id or agent id) that will be passed by command channel when it starts log-forwarder
   */
  public String proxyId;
  /**
   * similar to proxyId but some legacy RDC's will send hostId instead of proxyId
   */
  public String hostId;

  /**
   * below properties are sent in the RDC use-case
   */
  public String cspOrgId;
  public String hostName;
  public String proxyName;

  /* used in dev-test env */
  public String tenantId;

  /** used in vmc-log-forwarders deployed for dimension */
  public String profile;

  /**
   * the below config is used in vmc-use-cases
   */
  public String logIqUrl;
  public String logIqAccessKey;

  /**
   * the below config is used in RDC use cases
   */
  public String lemansServerUrl;
  public String lemansAccessKey;

  public String lemansClientDiskBackedQueueLocation;
  public boolean disableBidirectionalCommunication = false;
  public String autoInitializeConfig;

  /**
   * the below config is used in CSP Audit Log Processor use-case
   */
  public String cspGazUri;
  public String cspGazAuthId;
  public String cspGazAuthSecret;

  /**
   * used in starate cloud proxy running behind a proxy
   */
  public String httpProxyServer;
  public Integer httpProxyPort;

  public Integer noOfWorkerThreads;
  public Integer minSyslogMsgsInBlob;
  public Boolean skipLeMansClient;
  public Boolean compressPayload;
  public Boolean customSerialization;
  public Boolean fakePost;
  public Boolean fakeProcess;
  public Boolean fakeParse;
  public Boolean fakeQueue;
  public Boolean queueLessExecution;
  public Boolean useBestSpeedCompression;
  public Integer syslogQueueScale;
  public Integer liAgentsStatusBatchSize;
  public Integer restApiQueueScale;

  //CSp authentication details
  public String cspTokenUri;
  public String cspTokenContext;

  // Configs for Arctic use cases
  public String csgwId;
  public String vcId;
  public String sreOrgId;

  @Override
  public String toString() {
    return "LogForwarderArgs{" +
        ", addFwderIdInEvent='" + addFwderIdInEvent + '\'' +
        ", sddcId='" + sddcId + '\'' +
        ", orgId='" + orgId + '\'' +
        ", region='" + region + '\'' +
        ", orgType='" + orgType + '\'' +
        ", sddcEnv='" + sddcEnv + '\'' +
        ", hostId='" + hostId + '\'' +
        ", proxyId='" + proxyId + '\'' +
        ", cspOrgId='" + cspOrgId + '\'' +
        ", hostName='" + hostName + '\'' +
        ", proxyName='" + proxyName + '\'' +
        ", tenantId='" + tenantId + '\'' +
        ", logIqUrl='" + logIqUrl + '\'' +
        ", logIqAccessKey='*****'" +
        ", lemansServerUrl='" + lemansServerUrl + '\'' +
        ", lemansAccessKey='*****'" +
        ", lemansClientDiskBackedQueueLocation='" + lemansClientDiskBackedQueueLocation + '\'' +
        ", disableBidirectionalCommunication=" + disableBidirectionalCommunication +
        ", autoInitializeConfig='" + autoInitializeConfig + '\'' +
        ", cspGazUri='" + cspGazUri + '\'' +
        ", cspGazAuthId='" + cspGazAuthId + '\'' +
        ", cspGazAuthSecret='" + cspGazAuthSecret + '\'' +
        ", profile='" + profile + '\'' +
        ", httpProxyServer='" + httpProxyServer + '\'' +
        ", httpProxyPort='" + httpProxyPort + '\'' +
        ", noOfWorkerThreads='" + noOfWorkerThreads + '\'' +
        ", skipLeMansClient='" + skipLeMansClient + '\'' +
        ", compressPayload='" + compressPayload + '\'' +
        ", customSerialization='" + customSerialization + '\'' +
        ", fakePost='" + fakePost + '\'' +
        ", fakeProcess='" + fakeProcess + '\'' +
        ", fakeParse='" + fakeParse + '\'' +
        ", fakeQueue='" + fakeQueue + '\'' +
        ", minSyslogMsgsInBlob='" + minSyslogMsgsInBlob + '\'' +
        ", syslogQueueScale='" + syslogQueueScale + '\'' +
        ", liAgentsStatusBatchSize='" + liAgentsStatusBatchSize + '\'' +
        ", restApiQueueScale='" + restApiQueueScale + '\'' +
        ", cspTokenUri='*****'" +
        ", cspTokenContext='" + cspTokenContext + '\'' +
        '}';
  }
}
