package io.github.bfeng.simplemapred.workflow;

import com.google.protobuf.Any;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Message;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;

class ReflectionUtils {
    static Logger logger = Logger.getLogger(ReflectionUtils.class.getName());

    static List<Any> packList(List<Message> input) {
        return input.stream().map(Any::pack).collect(Collectors.toList());
    }

    static Iterable<Message> unpackList(List<Any> input, Class<Message> cls) {
        return input.stream().map((Any any) -> {
            try {
                return any.unpack(cls);
            } catch (InvalidProtocolBufferException e) {
                logger.log(Level.WARNING, e.getMessage(), e.getCause());
                return null;
            }
        }).collect(Collectors.toList());
    }

    static void runMapFn(
            String className,
            String inputFile,
            MapperEmitter<?, ?> emitter) {
        Class<?> mapperClass = null;
        try {
            mapperClass = Class.forName(className);
            Object mapper = mapperClass.newInstance();
            Method mapFn = mapperClass.getDeclaredMethod("map", String.class, MapperEmitter.class);
            mapFn.invoke(mapper, inputFile, emitter);
        } catch (ClassNotFoundException | IllegalAccessException | InvocationTargetException | InstantiationException | NoSuchMethodException e) {
            logger.info(e.getMessage());
        }
    }

    @SuppressWarnings("unchecked")
    static void runReduceFn(
            String className,
            Any key,
            List<Any> values,
            ReducerEmitter<?, ?> reducerEmitter) {
        Class<?> reduceClass = null;
        Class<?> mapOutKeyClass = null;
        Class<Message> reduceOutKeyClass = null;
        Class<Message> reduceOutValueClass = null;
        try {
            reduceClass = Class.forName(className);
            Type superclassType = reduceClass.getGenericInterfaces()[0];
            Type[] typeArgs = ((ParameterizedType) superclassType).getActualTypeArguments();
            String mapOutKeyClassName = typeArgs[0].getTypeName();
            String reduceOutKeyClassName = typeArgs[2].getTypeName();
            String reduceOutValueClassName = typeArgs[3].getTypeName();
            mapOutKeyClass = Class.forName(mapOutKeyClassName);
            reduceOutKeyClass = (Class<Message>) Class.forName(reduceOutKeyClassName);
            reduceOutValueClass = (Class<Message>) Class.forName(reduceOutValueClassName);
            Object reducer = reduceClass.newInstance();
            Method reduceFn = reduceClass.getDeclaredMethod("reduce", mapOutKeyClass, Iterable.class, ReducerEmitter.class);
            Message keyMsg = key.unpack(reduceOutKeyClass);
            Iterable<Message> valueMsg = unpackList(values, reduceOutValueClass);
            reduceFn.invoke(reducer, keyMsg, valueMsg, reducerEmitter);
        } catch (ClassNotFoundException | IllegalAccessException | InvocationTargetException | InstantiationException | NoSuchMethodException | InvalidProtocolBufferException e) {
            logger.info(e.getMessage());
        }
    }
}
