package com.qzl.wc;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.AggregateOperator;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.FlatMapOperator;
import org.apache.flink.api.java.operators.UnsortedGrouping;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;


public class BatchWordCount {
    public static void main(String[] args) throws Exception {
        // create execute enviroment
        ExecutionEnvironment env=ExecutionEnvironment.getExecutionEnvironment();
        //read datas from file
        DataSource<String> source=env.readTextFile("input\\word.txt");

        //tranceform data format

        FlatMapOperator<String, Tuple2<String,Long>> lamdaFlatMapData=source.flatMap((String line, Collector<Tuple2<String, Long>> out)->{
            String[] words = line.split(" ");
            for (String word : words) {
                out.collect(Tuple2.of(word, 1L));
            }
        }).returns(Types.TUPLE(Types.STRING, Types.LONG));;


        //group by word
        UnsortedGrouping<Tuple2<String,Long>> unsortedGrouping= lamdaFlatMapData.groupBy(0);
        AggregateOperator<Tuple2<String,Long>> min=unsortedGrouping.min(1);
        AggregateOperator<Tuple2<String,Long>> aggregateOperator=unsortedGrouping.sum(1);
        // aggregate in group

        //out put
        min.print();
        aggregateOperator.print();

       // source.print();
    }



}
