package io.github.bfeng.simplemapred.workflow;

import com.google.protobuf.Message;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.logging.Logger;

class ReflectionUtils {

    static void runMapFn(
            Logger logger,
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

    static void runReduceFn(
            Logger logger,
            String className,
            Object key,
            Iterable<Message> values,
            ReducerEmitter<?, ?> reducerEmitter) {
        Class<?> reduceClass = null;
        Class<?> mapOutKeyClass = null;
        try {
            reduceClass = Class.forName(className);
            Type superclassType = reduceClass.getGenericInterfaces()[0];
            Type[] typeArgs = ((ParameterizedType) superclassType).getActualTypeArguments();
            String mapOutKeyClassName = typeArgs[0].getTypeName();
            mapOutKeyClass = Class.forName(mapOutKeyClassName);
            Object reducer = reduceClass.newInstance();
            Method reduceFn = reduceClass.getDeclaredMethod("reduce", mapOutKeyClass, Iterable.class, ReducerEmitter.class);
            reduceFn.invoke(reducer, key, values, reducerEmitter);
        } catch (ClassNotFoundException | IllegalAccessException | InvocationTargetException | InstantiationException | NoSuchMethodException e) {
            logger.info(e.getMessage());
        }
    }
}
