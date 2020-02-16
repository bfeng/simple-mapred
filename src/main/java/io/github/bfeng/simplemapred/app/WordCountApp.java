package io.github.bfeng.simplemapred.app;

import io.github.bfeng.simplemapred.resource.InitClusterRequest;

public class WordCountApp extends SimpleMapReduce {

    @Override
    public void runMapReduce(String masterIP, int masterPort) {

    }

    @Override
    protected InitClusterRequest buildInitRequest() {
        return InitClusterRequest
                .newBuilder()
                .setNumberOfMappers(2)
                .setNumberOfReducers(2)
                .build();
    }

    public static void main(String[] args) {
        WordCountApp app = new WordCountApp();
        String masterIP = "localhost";
        int port = 12345;
        app.initCluster(masterIP, port);
        app.runMapReduce(masterIP, port);
        app.destroyCluster(masterIP, port);
    }
}
