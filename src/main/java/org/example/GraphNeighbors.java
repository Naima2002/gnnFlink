//package org.example;
//
//import org.apache.flink.api.common.state.ListState;
//import org.apache.flink.api.common.state.ListStateDescriptor;
//import org.apache.flink.configuration.Configuration;
//import org.apache.flink.streaming.api.datastream.DataStream;
//import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
//import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
//import org.apache.flink.streaming.api.functions.sink.PrintSinkFunction;
//import org.apache.flink.util.Collector;
//import org.apache.flink.api.java.tuple.Tuple2;
//
//public class GraphNeighbors {
//    public static void main(String[] args) throws Exception {
//
//        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//
//        //hard coded target node for now
//        String targetNode = "2gv2qt";
//
//        //stream with the target node for processing
//        DataStream<String> targetNodeStream = env.fromElements(targetNode);
//
//        //getting 2 hop nbrs. the second stream key bys the 1st hop neighbors
//        DataStream<Tuple2<String, String>> firstHopNeighbors = targetNodeStream
//                .keyBy(node -> node)
//                .process(new FirstHopNeighborProcessor());
//
//        DataStream<String> resultStream = firstHopNeighbors
//                .keyBy(tuple -> tuple.f1)
//                .process(new SecondHopNeighborProcessor());
//        //print reuslts
//        resultStream.addSink(new PrintSinkFunction<>());
//
//        env.execute("GraphNeighbors");
//    }
//
//    public static class FirstHopNeighborProcessor extends KeyedProcessFunction<String, String, Tuple2<String, String>> {
//        private transient ListState<String> neighborsState;
//
//        @Override
//        public void open(Configuration parameters) {
//            neighborsState = getRuntimeContext().getListState(new ListStateDescriptor<>("neighbors", String.class));
//        }
//        @Override
//        public void processElement(String targetNode, Context ctx, Collector<Tuple2<String, String>> out) throws Exception {
//            for (String neighbor : neighborsState.get()) {
//                out.collect(new Tuple2<>(targetNode, neighbor));
//            }
//        }
//    }
//
//    public static class SecondHopNeighborProcessor extends KeyedProcessFunction<String, Tuple2<String, String>, String> {
//        private transient ListState<String> neighborsState;
//
//        @Override
//        public void open(Configuration parameters) {
//            neighborsState = getRuntimeContext().getListState(new ListStateDescriptor<>("neighbors", String.class));
//        }
//
//        @Override
//        public void processElement(Tuple2<String, String> value, Context ctx, Collector<String> out) throws Exception {
//            String firstHopNeighbor = value.f1;
//            for (String secondHopNeighbor : neighborsState.get()) {
//                out.collect("target Node: " + value.f0 + ", First Hop nbr: " + firstHopNeighbor + ", Second Hop nbr: " + secondHopNeighbor);
//            }
//        }
//    }
//}

package org.example;

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.PrintSinkFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.api.java.tuple.Tuple2;

public class GraphNeighbors {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //a hardcoded target node
        String targetNode = "2gv2qt";

        //stream with the target node
        DataStream<String> targetNodeStream = env.fromElements(targetNode);

        //getting 2 hop nbrs. the second stream key bys the 1st hop neighbors
        DataStream<Tuple2<String, String>> firstHopNeighbors = targetNodeStream
                .keyBy(node -> node)
                .flatMap(new FirstHopNeighborProcessor());

        DataStream<String> resultStream = firstHopNeighbors
                .keyBy(tuple -> tuple.f1)
                .flatMap(new SecondHopNeighborProcessor());

        //orint results
        resultStream.addSink(new PrintSinkFunction<>());

        env.execute("GraphNeighborsQuery");
    }

    public static class FirstHopNeighborProcessor extends RichFlatMapFunction<String, Tuple2<String, String>> {
        private transient ListState<String> neighborsState;

        @Override
        public void open(Configuration parameters) {
            neighborsState = getRuntimeContext().getListState(new ListStateDescriptor<>("neighbors", String.class));
        }

        @Override
        public void flatMap(String targetNode, Collector<Tuple2<String, String>> out) throws Exception {
            for (String neighbor : neighborsState.get()) {
                out.collect(new Tuple2<>(targetNode, neighbor));
            }
        }
    }

    public static class SecondHopNeighborProcessor extends RichFlatMapFunction<Tuple2<String, String>, String> {
        private transient ListState<String> neighborsState;

        @Override
        public void open(Configuration parameters) {
            neighborsState = getRuntimeContext().getListState(new ListStateDescriptor<>("neighbors", String.class));
        }

        @Override
        public void flatMap(Tuple2<String, String> value, Collector<String> out) throws Exception {
            String firstHopNeighbor = value.f1;
            for (String secondHopNeighbor : neighborsState.get()) {
                out.collect("target node: " + value.f0 + ", First Hop nbr: " + firstHopNeighbor + ", Second Hop nbr: " + secondHopNeighbor);
            }
        }
    }
}
