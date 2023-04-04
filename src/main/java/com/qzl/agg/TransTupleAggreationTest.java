package com.qzl.agg;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.stream.Stream;

public class TransTupleAggreationTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env=StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<Tuple2<String,Integer>> stream= env.fromElements(Tuple2.of("aa",1)
                ,Tuple2.of("bb",6),Tuple2.of("bb",3),Tuple2.of("cc",4),Tuple2.of("cc",2));

       // stream.keyBy(v->v.f0).sum(1).print();
        stream.keyBy(v->v.f0).min("f1").print();
        //stream.keyBy(v->v.f0).min("f1").print();

        //stream.print();
        env.execute();

    }
}
