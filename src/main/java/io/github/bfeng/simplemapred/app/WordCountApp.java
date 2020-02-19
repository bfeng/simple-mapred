package io.github.bfeng.simplemapred.app;

import io.github.bfeng.simplemapred.resource.InitClusterRequest;
import io.github.bfeng.simplemapred.workflow.GenericMapReduce;
import io.github.bfeng.simplemapred.workflow.MapperEmitter;
import io.github.bfeng.simplemapred.workflow.ReducerEmitter;
import io.github.bfeng.simplemapred.workflow.types.IntMsg;
import io.github.bfeng.simplemapred.workflow.types.TextMsg;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.StringTokenizer;

public class WordCountApp extends SimpleMapReduce {

    static class MapReduce implements GenericMapReduce<TextMsg, IntMsg, TextMsg, IntMsg> {

        @Override
        public void map(String inputFile, MapperEmitter<TextMsg, IntMsg> emitter) {
            try {
                BufferedReader reader = new BufferedReader(new FileReader(inputFile));
                String line = null;
                while ((line = reader.readLine()) != null) {
                    StringTokenizer st = new StringTokenizer(line);
                    while (st.hasMoreTokens()) {
                        String key = st.nextToken();
                        int value = 1;
                        TextMsg outKey = TextMsg.newBuilder().setContent(key).build();
                        IntMsg outValue = IntMsg.newBuilder().setContent(value).build();
                        emitter.write(outKey, outValue);
                    }
                }
                reader.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        @Override
        public void reduce(TextMsg textMsg, Iterable<IntMsg> intMessages, ReducerEmitter<TextMsg, IntMsg> emitter) {
            int sum = 0;
            for (IntMsg intMessage : intMessages) {
                sum += intMessage.getContent();
            }
            IntMsg out = IntMsg.newBuilder().setContent(sum).build();
            emitter.write(textMsg, out);
        }
    }

    @Override
    public void runMapReduce(String masterIP, int masterPort) {
        System.out.println(MapReduce.class);
    }

    @Override
    protected InitClusterRequest buildInitRequest() {
        // The number of mappers must match the number of input files
        // This framework doesn't support input splits.
        return InitClusterRequest
                .newBuilder()
                .setNumberOfMappers(2)
                .setNumberOfReducers(1)
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
