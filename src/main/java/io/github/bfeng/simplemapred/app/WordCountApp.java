package io.github.bfeng.simplemapred.app;

import io.github.bfeng.simplemapred.workflow.GenericMapReduce;
import io.github.bfeng.simplemapred.workflow.MapperEmitter;
import io.github.bfeng.simplemapred.workflow.ReducerEmitter;
import io.github.bfeng.simplemapred.workflow.types.IntMsg;
import io.github.bfeng.simplemapred.workflow.types.TextMsg;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.StringTokenizer;

public class WordCountApp extends SimpleMapReduce {

    public static class MapReduce implements GenericMapReduce<TextMsg, IntMsg, TextMsg, IntMsg> {

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
                        emitter.collect(outKey, outValue);
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
            if (emitter.contains(textMsg)) {
                List<IntMsg> values = emitter.getValues(textMsg);
                if (values.size() > 0) {
                    sum += values.get(0).getContent();
                    IntMsg out = IntMsg.newBuilder().setContent(sum).build();
                    emitter.put(textMsg, Collections.singletonList(out));
                }
            } else {
                IntMsg out = IntMsg.newBuilder().setContent(sum).build();
                emitter.collect(textMsg, out);
            }
        }
    }

    public static void main(String[] args) {
        WordCountApp app = new WordCountApp();
        app.parseArgs(args);
        String masterIP = app.masterHost;
        int port = app.masterPort;
        app.initCluster(masterIP, port);
        app.runMapReduce(masterIP, port, MapReduce.class.getName());
        app.destroyCluster(masterIP, port);
    }
}
