//package org.example;
//
//import org.apache.flink.api.common.state.ListState;
//import org.apache.flink.api.common.state.ListStateDescriptor;
//import org.apache.flink.api.common.typeinfo.Types;
//import org.apache.flink.api.java.tuple.Tuple4;
//import org.apache.flink.streaming.api.datastream.DataStream;
//import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
//import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
//import org.apache.flink.util.Collector;
//
//public class GraphNeighbors {
//
//    public static void main(String[] args) throws Exception {
//        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//
//        DataStream<Tuple4<String, String, String, String>> edges = env.readTextFile("/Users/naima/Desktop/summer24/reddit/reddit-final-smaller.csv")
//                .filter(line -> !line.startsWith("Src node")) // Skip the header row
//                .map(line -> {
//                    String[] fields = line.split(",", 4);
//                    return Tuple4.of(
//                            fields[0].trim(),
//                            fields[1].trim(),
//                            fields[2].trim(),
//                            fields[3].trim()
//                    );
//                })
//                .returns(Types.TUPLE(Types.STRING, Types.STRING, Types.STRING, Types.STRING));
//
//        DataStream<String> neighbors = edges.keyBy(edge -> edge.f0)
//                .process(new NeighborExtractor("2gv2qt")); // specify the node ID for which you want to get neighbors
//
//        neighbors.print();
//
//        env.execute("Graph Neighbors");
//    }
//
//    public static class NeighborExtractor extends KeyedProcessFunction<String, Tuple4<String, String, String, String>, String> {
//        private final String targetNode;
//        private ListState<Tuple4<String, String, String, String>> neighborState;
//
//        public NeighborExtractor(String targetNode) {
//            this.targetNode = targetNode;
//        }
//
//        @Override
//        public void open(org.apache.flink.configuration.Configuration parameters) throws Exception {
//            ListStateDescriptor<Tuple4<String, String, String, String>> descriptor =
//                    new ListStateDescriptor<>(
//                            "neighbors",
//                            Types.TUPLE(Types.STRING, Types.STRING, Types.STRING, Types.STRING));
//            neighborState = getRuntimeContext().getListState(descriptor);
//        }
//
//        @Override
//        public void processElement(
//                Tuple4<String, String, String, String> value,
//                Context ctx,
//                Collector<String> out) throws Exception {
//
//            if (value.f0.equals(targetNode)) {
//                neighborState.add(value);
//                out.collect("Neighbor: " + value.f2 + ", Features: " + value.f3);
//            }
//        }
//    }
//}





package org.example;

import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

public class GraphNeighbors {

    public static class QueryFunction extends KeyedProcessFunction<String, String, String> {
        private transient ValueState<String> featuresState;
        private transient ListState<String> neighborsState;

        @Override
        public void open(Configuration parameters) {
            featuresState = getRuntimeContext().getState(new ValueStateDescriptor<>("features", String.class));
            neighborsState = getRuntimeContext().getListState(new ListStateDescriptor<>("neighbors", String.class));
        }

        @Override
        public void processElement(String targetNode, Context ctx, Collector<String> out) throws Exception {
            Iterable<String> neighbors = neighborsState.get();
            for (String neighbor : neighbors) {
                out.collect("tagrt node " + targetNode + ", Features: " + featuresState.value() + ", nbr: " + neighbor);
            }
        }
    }

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //state has been populated by the datalaoder

        DataStream<String> queryStream = env.fromElements("2gv2qt");

        queryStream
                .keyBy(value -> value)
                .process(new QueryFunction())
                .print();

        env.execute("graph nbrs");
    }
}
