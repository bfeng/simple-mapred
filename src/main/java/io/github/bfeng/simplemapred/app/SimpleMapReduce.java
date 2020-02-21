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
        channel.shutdown();
        logger.info(String.format("Cluster:%d started", clusterId));
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
        channel.shutdown();
        logger.info(String.format("Cluster:%d shutdown: %d", clusterId, status));
    }

    public void runMapReduce(String masterIP, int masterPort) {
        logger.info(String.format("Cluster:%d Run MapReduce job", clusterId));
        RunMapReduceRequest request = buildRunMapReduceRequest();
        ManagedChannel channel = ManagedChannelBuilder.forAddress(masterIP, masterPort)
                .usePlaintext()
                .build();
        MasterServiceGrpc.MasterServiceBlockingStub stub
                = MasterServiceGrpc.newBlockingStub(channel);
        RunMapReduceResponse response = stub.runMapReduce(request);
        int status = response.getStatus();
        channel.shutdown();
        logger.info(String.format("Cluster:%d MapReduce job done: %d", clusterId, status));
    }

    protected abstract InitClusterRequest buildInitRequest();

    protected abstract RunMapReduceRequest buildRunMapReduceRequest();
}
