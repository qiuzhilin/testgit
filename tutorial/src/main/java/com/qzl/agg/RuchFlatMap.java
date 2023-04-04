package com.qzl.agg;

import com.qzl.vo.Event;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class RuchFlatMap {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env=StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(3);
        // DataStream<Event> stream=
        DataStreamSource<Event> clicks = env.fromElements( new Event("Mary", "./home", 1000L),
                new Event("Bob", "./cart", 2000L),
                new Event("Alice", "./prod?id=1", 5 * 1000L), new Event("Cary", "./home", 60 * 1000L)
        );

        clicks.flatMap(new RichFlatMapFunction<Event, Long>() {
            @Override
            public void open(Configuration parameters) throws Exception { super.open(parameters);
                System.out.println("	索	引	为	"	+
                        getRuntimeContext().getIndexOfThisSubtask() + " 的任务开始");
            }
            @Override
            public void flatMap(Event event, Collector<Long> collector) throws Exception {
                collector.collect(1L);
            }
            @Override
            public void close() throws Exception {
                System.out.println("	索	引	为	"	+
                        getRuntimeContext().getIndexOfThisSubtask() + " 的任务end");
                super.close();
            }
        }).shuffle().print().setParallelism(5);

        env.execute();
    }
}
