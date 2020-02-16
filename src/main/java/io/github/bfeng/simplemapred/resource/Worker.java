package io.github.bfeng.simplemapred.resource;

import io.grpc.ServerBuilder;

import java.io.IOException;
import java.util.logging.Logger;

public class Worker extends ServerBase {
    private static final Logger logger = Logger.getLogger(Worker.class.getName());
    private final int workerId;
    private final int port;

    public Worker(String configuration, int workerId) throws IOException {
        super(configuration);
        this.workerId = workerId;
        this.port = getWorkerConf().get(workerId).port;
    }

    @Override
    protected void start() throws IOException {
        /* The port on which the server should run */
        server = ServerBuilder.forPort(port)
                .addService(new WorkerService())
                .build()
                .start();
        logger.info(String.format("Worker{%d} started, listening on %d", workerId, port));
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            // Use stderr here since the logger may have been reset by its JVM shutdown hook.
            System.err.println("*** shutting down gRPC server since JVM is shutting down");
            try {
                Worker.this.stop();
            } catch (InterruptedException e) {
                e.printStackTrace(System.err);
            }
            System.err.println("*** server shut down");
        }));
    }

    static class WorkerService extends WorkerServiceGrpc.WorkerServiceImplBase {

    }
}
