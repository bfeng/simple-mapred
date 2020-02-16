package io.github.bfeng.simplemapred.resource;

import io.grpc.ServerBuilder;
import io.grpc.stub.StreamObserver;

import java.io.IOException;
import java.util.logging.Logger;

public class Master extends ServerBase {
    private static final Logger logger = Logger.getLogger(Master.class.getName());

    private final int port;

    public Master(String configuration) throws IOException {
        super(configuration);
        this.port = getMasterConf().port;
    }

    @Override
    protected void start() throws IOException {
        /* The port on which the server should run */
        server = ServerBuilder.forPort(port)
                .addService(new MasterService())
                .build()
                .start();
        logger.info("Master started, listening on " + port);
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            // Use stderr here since the logger may have been reset by its JVM shutdown hook.
            System.err.println("*** shutting down gRPC server since JVM is shutting down");
            try {
                Master.this.stop();
            } catch (InterruptedException e) {
                e.printStackTrace(System.err);
            }
            System.err.println("*** server shut down");
        }));
    }

    /**
     * Main launches the server from the command line.
     */
    public static void main(String[] args) throws IOException, InterruptedException {
        final Master server = new Master(args[0]);
        server.start();
        server.blockUntilShutdown();
    }

    static class MasterService extends MasterServiceGrpc.MasterServiceImplBase {
        @Override
        public void initCluster(InitClusterRequest request, StreamObserver<InitClusterResponse> responseObserver) {
            InitClusterResponse response = InitClusterResponse.newBuilder().setClusterId(0).build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        }

        @Override
        public void destroyCluster(DestroyClusterRequest request, StreamObserver<DestroyClusterResponse> responseObserver) {
            DestroyClusterResponse response = DestroyClusterResponse.newBuilder().setStatus(0).build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        }
    }
}