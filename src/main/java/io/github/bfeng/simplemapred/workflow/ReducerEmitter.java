package io.github.bfeng.simplemapred.workflow;

import com.google.protobuf.Message;

public class ReducerEmitter<ReducerOutKey extends Message, ReducerOutValue extends Message> {
    public void write(ReducerOutKey outKey, ReducerOutValue outValue) {
        System.out.println(String.format("Reducer out: key={%s},value={%s}", outKey, outValue));
    }
}
