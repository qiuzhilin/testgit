package com.qzl.agg;

import com.qzl.source.ClickSource;
import com.qzl.vo.Event;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class TransReduceTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env=StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
       // DataStream<Event> stream=
                env.addSource(new ClickSource())
                        //将event 转化为二元组，一个用户，一个计数
                .map(new MapFunction<Event, Tuple2<String,Long>>() {
                    @Override
                    public Tuple2<String, Long> map(Event event) throws Exception {
                        return Tuple2.of(event.user,1L);
                    }
                })
                        //根据名字分区分流
                .keyBy(r->r.f0)
                        //进行规约处理，将流值相加
                .reduce(new ReduceFunction<Tuple2<String, Long>>() {
                    @Override
                    public Tuple2<String, Long> reduce(Tuple2<String, Long> stringLongTuple2, Tuple2<String, Long> t1) throws Exception {
                       //每来一条数据就进行相加
                        return Tuple2.of(stringLongTuple2.f0,stringLongTuple2.f1+t1.f1);
                    }
                })
                        //将聚合结果发到一条流中
                .keyBy(r->true).reduce(new ReduceFunction<Tuple2<String, Long>>() {
                    @Override
                    public Tuple2<String, Long> reduce(Tuple2<String, Long> stringLongTuple2, Tuple2<String, Long> t1) throws Exception {
                        return stringLongTuple2.f1 > t1.f1 ? stringLongTuple2 : t1;
                    }
                }).print()

                ;
        //stream.print();

        env.execute();
    }
}
