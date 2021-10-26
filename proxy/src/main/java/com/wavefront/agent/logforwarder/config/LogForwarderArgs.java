package com.wavefront.agent.logforwarder.config;



/**
 * @author Manoj Ramakrishnan (rmanoj@vmware.com).
 * @since 9/15/21 1:24 PM
 */
public class LogForwarderArgs  {

  public String addFwderIdInEvent;

  public String orgId;
  public String region;
  public String orgType;
  public String sddcEnv;

  /**
   * this is the proxy id (wavefront agentId)
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

  /**
   * the below config is used in vmc-use-cases
   */
  public String logIqUrl;
  public String logIqAccessKey;

  /* used in dev-test env */
  public String tenantId;

  /**
   * the below config is used in RDC use cases
   */
  public String lemansServerUrl;
  public String lemansAccessKey;
  public String lemansClientDiskBackedQueueLocation;// location in local disk where buffers are
  // written
  public boolean disableBidirectionalCommunication = true;

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


  // Configs for Arctic use cases
  public String csgwId;
  public String vcId;
  public String sreOrgId;

  @Override
  public String toString() {
    return "LogForwarderArgs{" +
        ", addFwderIdInEvent='" + addFwderIdInEvent + '\'' +
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
        ", logIqAccessKey='*****'" +
        ", lemansServerUrl='" + lemansServerUrl + '\'' +
        ", lemansAccessKey='*****'" +
        ", lemansClientDiskBackedQueueLocation='" + lemansClientDiskBackedQueueLocation + '\'' +
        ", disableBidirectionalCommunication=" + disableBidirectionalCommunication +
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
        '}';
  }
}
