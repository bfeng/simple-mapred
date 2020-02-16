package io.github.bfeng.simplemapred.app;

import io.github.bfeng.simplemapred.resource.*;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;

import java.util.logging.Logger;

public abstract class SimpleMapReduce {
    private static final Logger logger = Logger.getLogger(SimpleMapReduce.class.getName());
    int clusterId;

    public void initCluster(String masterIP, int masterPort) {
        logger.info("Starting MapReduce job cluster");
        InitClusterRequest initClusterRequest = buildInitRequest();
        ManagedChannel channel = ManagedChannelBuilder.forAddress(masterIP, masterPort)
                .usePlaintext()
                .build();
        MasterServiceGrpc.MasterServiceBlockingStub stub
                = MasterServiceGrpc.newBlockingStub(channel);
        InitClusterResponse response = stub.initCluster(initClusterRequest);
        clusterId = response.getClusterId();
        logger.info(String.format("Cluster:%d started", clusterId));
        channel.shutdown();
    }

    public void destroyCluster(String masterIP, int masterPort) {
        logger.info("MapReduce job done");
        DestroyClusterRequest destroyClusterRequest = DestroyClusterRequest.newBuilder()
                .setClusterId(clusterId).build();
        ManagedChannel channel = ManagedChannelBuilder.forAddress(masterIP, masterPort)
                .usePlaintext()
                .build();
        MasterServiceGrpc.MasterServiceBlockingStub stub
                = MasterServiceGrpc.newBlockingStub(channel);
        DestroyClusterResponse response = stub.destroyCluster(destroyClusterRequest);
        int status = response.getStatus();
        logger.info("Cluster shutdown: " + status);
        channel.shutdown();
    }

    public abstract void runMapReduce(String masterIP, int masterPort);

    protected abstract InitClusterRequest buildInitRequest();
}
