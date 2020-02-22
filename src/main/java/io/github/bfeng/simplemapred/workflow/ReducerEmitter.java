package io.github.bfeng.simplemapred.workflow;

import com.google.protobuf.Descriptors;
import com.google.protobuf.Message;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class ReducerEmitter<ReducerOutKey extends Message, ReducerOutValue extends Message> {

    private String outputFile;

    private BufferedWriter writer;

    private Map<ReducerOutKey, List<ReducerOutValue>> data = new HashMap<>();

    public ReducerEmitter() {
    }

    public ReducerEmitter(String outputFile) {
        this.outputFile = outputFile;
    }

    private void collect(ReducerOutKey outKey, ReducerOutValue outValue) {
        List<ReducerOutValue> list = data.computeIfAbsent(outKey, k -> new ArrayList<>());
        list.add(outValue);
    }

    private List<String> convert(List<ReducerOutValue> outValueList) {
        return outValueList.stream().map((ReducerOutValue outValue) -> {
            Descriptors.FieldDescriptor contentDesc = outValue.getDescriptorForType().findFieldByName("content");
            return outValue.getField(contentDesc).toString();
        }).collect(Collectors.toList());
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
                for (Map.Entry<ReducerOutKey, List<ReducerOutValue>> entry : data.entrySet()) {
                    ReducerOutKey outKey = entry.getKey();
                    Descriptors.FieldDescriptor contentDesc = outKey.getDescriptorForType().findFieldByName("content");
                    String realKey = outKey.getField(contentDesc).toString();
                    List<String> realValues = convert(entry.getValue());
                    writer.write(realKey + "\t");
                    for (int i = 0; i < realValues.size(); i++) {
                        String value = realValues.get(i);
                        writer.write(value);
                        if (i < realValues.size() - 1) {
                            writer.write(",");
                        }
                    }
                    writer.newLine();
                }
                writer.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    public void write(ReducerOutKey outKey, ReducerOutValue outValue) {
        System.out.println(String.format("Reducer out: key={%s},value={%s}", outKey, outValue));
        collect(outKey, outValue);
    }

    public void put(ReducerOutKey outKey, List<ReducerOutValue> outValues) {
        data.put(outKey, outValues);
    }

    public boolean contains(ReducerOutKey outKey) {
        return data.containsKey(outKey);
    }

    public List<ReducerOutValue> getValues(ReducerOutKey outKey) {
        return data.get(outKey);
    }
}
