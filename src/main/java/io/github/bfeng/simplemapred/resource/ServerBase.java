package io.github.bfeng.simplemapred.resource;

import io.grpc.Server;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

abstract class ServerBase {
    private MachineConf masterConf;
    private List<MachineConf> workerConf;

    protected Server server;

    protected ServerBase(String configuration) throws IOException {
        workerConf = new ArrayList<>();
        parse(configuration);
    }

    private void parse(String configuration) throws IOException {
        List<String> allLines = Files.readAllLines(Paths.get(configuration));
        for (String line : allLines) {
            String[] conf = line.split(":");
            switch (conf[0]) {
                case "master":
                    masterConf = new MachineConf(conf[1], conf[2], conf[3], MachineType.master);
                    break;
                case "worker":
                    workerConf.add(new MachineConf(conf[1], conf[2], conf[3], MachineType.worker));
                    break;
                default:
                    System.err.println("Unknown conf: " + line);
            }
        }
    }

    protected abstract void start() throws IOException;

    protected void stop() throws InterruptedException {
        if (server != null) {
            try {
                server.shutdown().awaitTermination(30, TimeUnit.SECONDS);
            } catch (NoClassDefFoundError e) {
                // ignore unknown bug belongs to grpc or netty
            }
        }
    }

    public static void awaitTerminationAfterShutdown(ExecutorService threadPool) {
        threadPool.shutdown();
        try {
            if (!threadPool.awaitTermination(30, TimeUnit.MINUTES)) {
                threadPool.shutdownNow();
            }
        } catch (InterruptedException ex) {
            threadPool.shutdownNow();
            Thread.currentThread().interrupt();
        }
    }

    /**
     * Await termination on the main thread since the grpc library uses daemon threads.
     */
    protected void blockUntilShutdown() throws InterruptedException {
        if (server != null) {
            server.awaitTermination();
        }
    }

    public MachineConf getMasterConf() {
        return this.masterConf;
    }

    public List<MachineConf> getWorkerConf() {
        return this.workerConf;
    }

    enum MachineType {
        master,
        worker
    }

    static class MachineConf {
        final String ip;
        final int port;
        final String home;
        final MachineType type;

        public MachineConf(String ip, String port, String home, MachineType type) {
            this.ip = ip;
            this.port = Integer.parseInt(port);
            this.home = home;
            this.type = type;
        }
    }
}
