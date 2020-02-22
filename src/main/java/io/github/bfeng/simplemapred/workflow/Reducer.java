package io.github.bfeng.simplemapred.workflow;

import com.google.protobuf.Any;
import com.google.protobuf.Message;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.ServerBuilder;
import io.grpc.stub.StreamObserver;

import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

public class Reducer extends TaskBase {
    private static final Logger logger = Logger.getLogger(Reducer.class.getName());

    protected Reducer(int id, String host, int port) {
        super(new TaskMeta(TaskMeta.TaskType.reducer, id, host, port));
    }

    @Override
    protected void start() throws IOException {
        /* The port on which the server should run */
        server = ServerBuilder.forPort(meta.port)
                .addService(new ReducerService(this))
                .build()
                .start();
        logger.info(String.format("Reducer[%d] started, listening on %d", meta.id, meta.port));
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            // Use stderr here since the logger may have been reset by its JVM shutdown hook.
            System.err.println("*** shutting down gRPC server since JVM is shutting down");
            try {
                this.stop();
            } catch (InterruptedException e) {
                e.printStackTrace(System.err);
            }
            System.err.println("*** Reducer shut down");
        }));
    }

    public int runReducerFn(String outputFile, String reduceClass, int totalReducers, List<TaskConf> mapperMetas) {
        logger.info(reduceClass + " runs output: " + outputFile + " -> about to talk to " + mapperMetas.size() + " mappers");
        ReducerEmitter<Message, Message> emitter = new ReducerEmitter<>(outputFile);
        emitter.open();
        for (TaskConf conf : mapperMetas) {
            logger.info(String.format("Reducer[%d] talks to Mapper[%d:%s:%d]", meta.id, conf.getId(), conf.getHost(), conf.getPort()));
            ManagedChannel channel = ManagedChannelBuilder.forAddress(conf.getHost(), conf.getPort())
                    .usePlaintext()
                    .build();
            MapperServiceGrpc.MapperServiceBlockingStub stub = MapperServiceGrpc.newBlockingStub(channel);
            ReadKeyValueRequest request = ReadKeyValueRequest.newBuilder()
                    .setReduceId(meta.id)
                    .setTotalReducers(totalReducers)
                    .build();
            Iterator<CombinedKeyValuePairs> iterator;
            try {
                iterator = stub.readCombinedKeyValues(request);
                while (iterator.hasNext()) {
                    CombinedKeyValuePairs keyValuePairs = iterator.next();
                    Any key = keyValuePairs.getKey();
                    List<Any> values = keyValuePairs.getValuesList();
                    ReflectionUtils.runReduceFn(reduceClass, key, values, emitter);
                }
            } catch (Exception e) {
                logger.log(Level.WARNING, e.getMessage(), e.getCause());
            }
            channel.shutdown();
        }
        emitter.close();
        return 0;
    }

    public static void main(String[] args) {
        logger.info("Reducer task: " + Arrays.deepToString(args));
        int id = Integer.parseInt(args[0]);
        String host = args[1];
        int port = Integer.parseInt(args[2]);
        try {
            final Reducer server = new Reducer(id, host, port);
            server.start();
            server.blockUntilShutdown();
        } catch (IOException | InterruptedException e) {
            logger.fine(e.getMessage());
        }
    }

    private static class ReducerService extends ReducerServiceGrpc.ReducerServiceImplBase {
        private final Reducer reducer;

        public ReducerService(Reducer reducer) {
            this.reducer = reducer;
        }

        @Override
        public void runReducer(RunReducerRequest request, StreamObserver<RunReducerResponse> responseObserver) {
            int status = 0;
            try {
                String outputFile = request.getOutputFile();
                String className = request.getReduceClass();
                int totalReducers = request.getTotalReducers();
                List<TaskConf> mapperMetas = request.getMappersList();
                status = reducer.runReducerFn(outputFile, className, totalReducers, mapperMetas);
            } catch (Exception e) {
                logger.info(e.getMessage());
                status = -1;
            } finally {
                RunReducerResponse response = RunReducerResponse.newBuilder().setStatus(status).build();
                responseObserver.onNext(response);
                responseObserver.onCompleted();
            }
        }

        @Override
        public void stopReducer(StopLocalReducerRequest request, StreamObserver<StopLocalReducerResponse> responseObserver) {
            try {
                reducer.stop();
            } catch (InterruptedException e) {
                logger.info(e.getMessage());
            } finally {
                StopLocalReducerResponse response = StopLocalReducerResponse.newBuilder().setStatus(0).build();
                responseObserver.onNext(response);
                responseObserver.onCompleted();
            }
        }
    }
}
