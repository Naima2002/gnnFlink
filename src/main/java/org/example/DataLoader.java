//package org.example;
//
//import org.apache.flink.api.common.state.ListState;
//import org.apache.flink.api.common.state.ListStateDescriptor;
//import org.apache.flink.api.common.state.ValueState;
//import org.apache.flink.api.common.state.ValueStateDescriptor;
//import org.apache.flink.configuration.Configuration;
//import org.apache.flink.streaming.api.CheckpointingMode;
//import org.apache.flink.streaming.api.datastream.DataStream;
//import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
//import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
//import org.apache.flink.streaming.api.functions.source.SourceFunction;
//import org.apache.flink.util.Collector;
//import org.apache.flink.api.common.typeinfo.Types;
//import org.apache.flink.api.java.tuple.Tuple4;
//
//public class DataLoader {
//
//    public static void main(String[] args) throws Exception {
//        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//
//        // Enable checkpointing
//        env.enableCheckpointing(10000, CheckpointingMode.EXACTLY_ONCE);
//
//        DataStream<String> input = env.readTextFile("/Users/naima/Desktop/summer24/reddit/reddit-final-smaller.csv");
//
//        DataStream<Tuple4<String, String, String, String>> parsed = input
//                .map(line -> {
//                    String[] parts = line.split(",", 4);
//                    if (parts.length != 4) {
//                        throw new IllegalArgumentException("Invalid record: " + line);
//                    }
//                    return new Tuple4<>(parts[0], parts[1], parts[2], parts[3]);
//                })
//                .returns(Types.TUPLE(Types.STRING, Types.STRING, Types.STRING, Types.STRING));
//
//        parsed
//                .keyBy(value -> value.f0)
//                .process(new NeighborStateFunction());
//
//        // Add a continuous source to keep the job running indefinitely
//        env.addSource(new KeepAliveSource()).setParallelism(1);
//
//        env.execute("DataLoader");
//    }
//
//    public static class NeighborStateFunction extends KeyedProcessFunction<String, Tuple4<String, String, String, String>, String> {
//        private transient ValueState<String> featuresState;
//        private transient ListState<String> neighborsState;
//
//        @Override
//        public void open(Configuration parameters) {
//            featuresState = getRuntimeContext().getState(new ValueStateDescriptor<>("features", String.class));
//            neighborsState = getRuntimeContext().getListState(new ListStateDescriptor<>("neighbors", String.class));
//        }
//
//        @Override
//        public void processElement(Tuple4<String, String, String, String> value, Context ctx, Collector<String> out) throws Exception {
//            featuresState.update(value.f1);
//            neighborsState.add(value.f2);
//        }
//    }
//
//    public static class KeepAliveSource implements SourceFunction<String> {
//        private volatile boolean isRunning = true;
//
//        @Override
//        public void run(SourceContext<String> ctx) throws Exception {
//            while (isRunning) {
//                synchronized (ctx.getCheckpointLock()) {
//                    ctx.collect("KeepAlive");
//                }
//                Thread.sleep(1000); // Sleep for 1 second
//            }
//        }
//
//        @Override
//        public void cancel() {
//            isRunning = false;
//        }
//    }
//}
//
//package org.example;
//
//import org.apache.flink.api.common.state.ListState;
//import org.apache.flink.api.common.state.ListStateDescriptor;
//import org.apache.flink.api.common.state.ValueState;
//import org.apache.flink.api.common.state.ValueStateDescriptor;
//import org.apache.flink.configuration.Configuration;
//import org.apache.flink.streaming.api.datastream.DataStream;
//import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
//import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
//import org.apache.flink.streaming.api.functions.source.SourceFunction;
//import org.apache.flink.util.Collector;
//import org.apache.flink.api.common.typeinfo.Types;
//import org.apache.flink.api.java.tuple.Tuple4;
//
//public class DataLoader {
//
//    public static void main(String[] args) throws Exception {
//        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//
//        DataStream<String> input = env.readTextFile("/Users/naima/Desktop/summer24/reddit/reddit-final-smaller.csv");
//
//        DataStream<Tuple4<String, String, String, String>> parsed = input
//                .map(line -> {
//                    String[] parts = line.split(",", 4);
//                    if (parts.length != 4) {
//                        throw new IllegalArgumentException("Invalid record: " + line);
//                    }
//                    return new Tuple4<>(parts[0], parts[1], parts[2], parts[3]);
//                })
//                .returns(Types.TUPLE(Types.STRING, Types.STRING, Types.STRING, Types.STRING));
//
//        parsed
//                .keyBy(value -> value.f0)
//                .process(new NeighborStateFunction());
//
//        // Add a continuous source to keep the job running indefinitely
//        env.addSource(new InfiniteSource()).setParallelism(1);
//
//        env.execute("DataLoader");
//    }
//
//    public static class NeighborStateFunction extends KeyedProcessFunction<String, Tuple4<String, String, String, String>, String> {
//        private transient ValueState<String> featuresState;
//        private transient ListState<String> neighborsState;
//
//        @Override
//        public void open(Configuration parameters) {
//            featuresState = getRuntimeContext().getState(new ValueStateDescriptor<>("features", String.class));
//            neighborsState = getRuntimeContext().getListState(new ListStateDescriptor<>("neighbors", String.class));
//        }
//
//        @Override
//        public void processElement(Tuple4<String, String, String, String> value, Context ctx, Collector<String> out) throws Exception {
//            featuresState.update(value.f1);
//            neighborsState.add(value.f2);
//        }
//    }
//
//    public static class InfiniteSource implements SourceFunction<String> {
//        private volatile boolean isRunning = true;
//
//        @Override
//        public void run(SourceContext<String> ctx) throws Exception {
//            while (isRunning) {
//                synchronized (ctx.getCheckpointLock()) {
//                    ctx.collect("keep-alive");
//                }
//                Thread.sleep(1000); // Sleep for a while to simulate work
//            }
//        }
//
//        @Override
//        public void cancel() {
//            isRunning = false;
//        }
//    }
//}



//this code has the printsinkfunction to ensure the inf src is part of the pipeline
package org.example;

import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.sink.PrintSinkFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple4;

public class DataLoader {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();


        DataStream<String> input = env.readTextFile("/Users/naima/Desktop/summer24/reddit/reddit-final-smaller.csv");

        //parse the input data to tuples
        DataStream<Tuple4<String, String, String, String>> parsed = input
                .map(line -> {
                    String[] parts = line.split(",", 4);
                    if (parts.length != 4) {
                        throw new IllegalArgumentException("invalid record: " + line);
                    }
                    return new Tuple4<>(parts[0], parts[1], parts[2], parts[3]);
                })
                .returns(Types.TUPLE(Types.STRING, Types.STRING, Types.STRING, Types.STRING));

        //process the parsed data to maintain nbr state
        parsed
                .keyBy(value -> value.f0)
                .process(new NeighborStateFunction());

        //add an inf source
        DataStream<String> keepAliveSource = env.addSource(new InfiniteSource()).setParallelism(1);

        //to ensure the source is active
        keepAliveSource.addSink(new PrintSinkFunction<>());

        env.execute("DataLoader");
    }

    //maintain state for each key
    public static class NeighborStateFunction extends KeyedProcessFunction<String, Tuple4<String, String, String, String>, String> {
        private transient ValueState<String> featuresState;
        private transient ListState<String> neighborsState;

        @Override
        public void open(Configuration parameters) {
            featuresState = getRuntimeContext().getState(new ValueStateDescriptor<>("features", String.class));
            neighborsState = getRuntimeContext().getListState(new ListStateDescriptor<>("neighbors", String.class));
        }

        @Override
        public void processElement(Tuple4<String, String, String, String> value, Context ctx, Collector<String> out) throws Exception {
            //udpate state with the incoming node
            featuresState.update(value.f1);
            neighborsState.add(value.f2);
        }
    }

    // SourceFunction to emit alive msgs indefinitely
    public static class InfiniteSource implements SourceFunction<String> {
        private volatile boolean isRunning = true;

        @Override
        public void run(SourceContext<String> ctx) throws Exception {
            while (isRunning) {
                synchronized (ctx.getCheckpointLock()) {
                    ctx.collect("alive");
                }
                Thread.sleep(1000); //sleep bw msgs
            }
        }

        @Override
        public void cancel() {
            isRunning = false; //no more msgs
        }
    }
}
