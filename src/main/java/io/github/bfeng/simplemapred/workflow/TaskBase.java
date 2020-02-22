package io.github.bfeng.simplemapred.workflow;

import io.grpc.Server;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

public abstract class TaskBase {
    protected TaskMeta meta;
    protected Server server;

    protected TaskBase(TaskMeta meta) {
        this.meta = meta;
    }

    protected abstract void start() throws IOException;

    protected void stop() throws InterruptedException {
        if (server != null) {
            server.shutdown().awaitTermination(1, TimeUnit.SECONDS);
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

    public static int exec(Class clazz, List<String> jvmArgs, List<String> args) throws IOException, InterruptedException {
        String javaHome = System.getProperty("java.home");
        String javaBin = javaHome + File.separator + "bin" + File.separator + "java";
        String classpath = System.getProperty("java.class.path");
        String className = clazz.getName();

        List<String> command = new ArrayList<>();
        command.add(javaBin);
        command.addAll(jvmArgs);
        command.add("-cp");
        command.add(classpath);
        command.add(className);
        command.addAll(args);

        ProcessBuilder builder = new ProcessBuilder(command);
        Process process = builder.inheritIO().start();
//        process.waitFor();
//        return process.exitValue();
        Thread.sleep(1000);
        return 0;
    }
}
