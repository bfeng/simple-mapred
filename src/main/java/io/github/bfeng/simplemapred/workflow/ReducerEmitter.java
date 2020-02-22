package io.github.bfeng.simplemapred.workflow;

import com.google.protobuf.Any;
import com.google.protobuf.Message;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;

public class ReducerEmitter<ReducerOutKey extends Message, ReducerOutValue extends Message> {

    private String outputFile;

    private BufferedWriter writer;

    public ReducerEmitter() {
    }

    public ReducerEmitter(String outputFile) {
        this.outputFile = outputFile;
    }

    public void open() {
        try {
            writer = new BufferedWriter(new FileWriter(outputFile));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void close() {
        if (writer != null) {
            try {
                writer.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    public void write(ReducerOutKey outKey, ReducerOutValue outValue) {
        System.out.println(String.format("Reducer out: key={%s},value={%s}", outKey, outValue));
        if (writer != null) {
            try {
                writer.write(outKey.toString());
                writer.write(outValue.toString());
                writer.newLine();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }
}
