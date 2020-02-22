package io.github.bfeng.simplemapred.app;

import io.github.bfeng.simplemapred.resource.*;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;

import java.util.Arrays;
import java.util.List;
import java.util.logging.Logger;

public abstract class SimpleMapReduce {
    private static final Logger logger = Logger.getLogger(SimpleMapReduce.class.getName());
    protected String masterHost;
    protected int masterPort;
    protected List<String> inputFiles;
    protected List<String> outputFiles;
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

    public void runMapReduce(String masterIP, int masterPort, String className) {
        logger.info(String.format("Cluster:%d Run MapReduce job", clusterId));
        RunMapReduceRequest request = buildRunMapReduceRequest(className);
        ManagedChannel channel = ManagedChannelBuilder.forAddress(masterIP, masterPort)
                .usePlaintext()
                .build();
        MasterServiceGrpc.MasterServiceBlockingStub stub
                = MasterServiceGrpc.newBlockingStub(channel);
        RunMapReduceResponse response = stub.runMapReduce(request);
        int status = response.getStatus();
        channel.shutdown();
        for (String output : outputFiles) {
            logger.info("Output: " + output);
        }
        logger.info(String.format("Cluster:%d MapReduce job done: %d", clusterId, status));
    }

    protected void parseArgs(String[] args) {
        assert args.length == 3;
        String[] masterConf = args[0].split(":");
        this.masterHost = masterConf[0];
        this.masterPort = Integer.parseInt(masterConf[1]);
        String[] inputConf = args[1].split(",");
        inputFiles = Arrays.asList(inputConf);
        String[] outputConf = args[2].split(",");
        outputFiles = Arrays.asList(outputConf);
    }

    /**
     * The maps and reduces match input files and output files respectively.
     * But it is not a hard requirement in the framework.
     */
    protected InitClusterRequest buildInitRequest() {
        int mappers = this.inputFiles.size();
        int reducers = this.outputFiles.size();
        return InitClusterRequest
                .newBuilder()
                .setNumberOfMappers(mappers)
                .setNumberOfReducers(reducers)
                .build();
    }

    protected RunMapReduceRequest buildRunMapReduceRequest(String className) {
        List<String> inputFiles = this.inputFiles;
        List<String> outputFiles = this.outputFiles;
        return RunMapReduceRequest.newBuilder()
                .setClusterId(clusterId)
                .addAllInputFiles(inputFiles)
                .addAllOutputFiles(outputFiles)
                .setMapReduceClass(className)
                .build();
    }
}
