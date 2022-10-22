package com.wavefront.agent.listeners.grpc;

import com.wavefront.grpc.Batch;
import com.wavefront.grpc.Response;
import com.wavefront.grpc.WFProxyGrpc;
import java.util.logging.Logger;

public class GrpcHandler extends WFProxyGrpc.WFProxyImplBase {
  private static final Logger logger = Logger.getLogger(GrpcHandler.class.getName());

  @Override
  public void reportPoints(
          Batch request,
      io.grpc.stub.StreamObserver<com.wavefront.grpc.Response> responseObserver) {
    logger.info("Batch received:\n" + request);
    responseObserver.onNext(Response.getDefaultInstance());
    responseObserver.onCompleted();
  }
}
