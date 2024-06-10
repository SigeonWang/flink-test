package com.wxj.steaming.combine;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @Author: xingjian wang
 * @Date: 2024/5/29 22:47
 * @Description: TODO
 */
public class ConnectDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);

        DataStreamSource<Integer> ds1 = env.fromElements(1, 2, 3, 4, 5);
        DataStreamSource<String> ds2 = env.fromElements("-1", "-2", "-3", "-4", "-5");

        // TODO 合流 connect() （用的较多！）
        // 要点：
        //     1、内部还是独立的流，只是形式上的统一，后续处理还需要单独处理每条流
        //     2、一次只能合并一条流
        ConnectedStreams<Integer, String> connectDs = ds1.connect(ds2);
        // new CoMapFunction<Integer, String, Object>()
        //     参数1：流1的数据类型
        //     参数2：流2的数据类型
        //     参数3：输出类型
        SingleOutputStreamOperator<String> mapDs = connectDs.map(new CoMapFunction<Integer, String, String>() {
            // 流1的处理逻辑
            @Override
            public String map1(Integer value) throws Exception {
                return value.toString();
            }

            // 流2的处理逻辑
            @Override
            public String map2(String value) throws Exception {
                return value;
            }
        });

        /**
         * TODO 连接两条流，输出能根据id匹配上的数据（类似inner join效果）
         */
        DataStreamSource<Tuple2<Integer, String>> ds3 = env.fromElements(
                Tuple2.of(1, "a1"),
                Tuple2.of(1, "a2"),
                Tuple2.of(2, "b"),
                Tuple2.of(3, "c")

        );
        DataStreamSource<Tuple3<Integer, String, Integer>> ds4 = env.fromElements(
                Tuple3.of(1, "aa1", 1),
                Tuple3.of(1, "aa2", 2),
                Tuple3.of(2, "bb", 1),
                Tuple3.of(3, "cc", 1)

        );
        ConnectedStreams<Tuple2<Integer, String>, Tuple3<Integer, String, Integer>> connectDs2 = ds3.connect(ds4);
        // 多并行度下，需要根据 "联条件" 进行keyby，才能保证key相同的数据到一起去，才能匹配上。
        // keyBy()
        //      KeySelector1 / KeySelector2
        // process()
        //      CoProcessFunction: processElement1() / processElement2()
        SingleOutputStreamOperator<Object> res = connectDs2.keyBy(v1 -> v1.f0, v2 -> v2.f0)
                .process(new CoProcessFunction<Tuple2<Integer, String>, Tuple3<Integer, String, Integer>, Object>() {
                    // 缓存两条流的数据（后面可以通过flink的状态来存储！这里先通过HashMap存储）
                    Map<Integer, List<Tuple2<Integer, String>>> ds1Cache = new HashMap<>();
                    Map<Integer, List<Tuple3<Integer, String, Integer>>> ds2Cache = new HashMap<>();

                    /**
                     * 流1的处理逻辑
                     *
                     * @param value 流1的数据
                     * @param ctx   上下文
                     * @param out   采集器（输出）
                     * @throws Exception
                     */
                    @Override
                    public void processElement1(Tuple2<Integer, String> value, Context ctx, Collector<Object> out) throws Exception {
                        Integer id = value.f0;
                        // TODO 1、ds1的数据来了，缓存到ds1Cache
                        if (!ds1Cache.containsKey(id)) {  // key不存在，创建list并添加value
                            List<Tuple2<Integer, String>> ds1List = new ArrayList<>();
                            ds1List.add(value);
                            ds1Cache.put(id, ds1List);
                        } else {  // key存在，取出list并添加value
                            ds1Cache.get(id).add(value);
                        }

                        // TODO 2、去ds2Cache中查看是否能匹配上key
                        if (ds2Cache.containsKey(id)) {
                            List<Tuple3<Integer, String, Integer>> ds2list = ds2Cache.get(id);
                            for (Tuple3<Integer, String, Integer> value2 : ds2list) {
                                out.collect("s1:" + value + "<===>" + "s2" + value2);
                            }
                        }
                    }

                    /**
                     * 流2的处理逻辑
                     */
                    @Override
                    public void processElement2(Tuple3<Integer, String, Integer> value, Context ctx, Collector<Object> out) throws Exception {
                        Integer id = value.f0;
                        if (!ds2Cache.containsKey(id)) {  // key不存在，创建list并添加value
                            List<Tuple3<Integer, String, Integer>> ds2List = new ArrayList<>();
                            ds2List.add(value);
                            ds2Cache.put(id, ds2List);
                        } else {  // key存在，取出list并添加value
                            ds2Cache.get(id).add(value);
                        }

                        if (ds1Cache.containsKey(id)) {
                            List<Tuple2<Integer, String>> ds2list = ds1Cache.get(id);
                            for (Tuple2<Integer, String> value1 : ds2list) {
                                out.collect("s1:" + value1 + "<===>" + "s2" + value);
                            }
                        }
                    }
                });

        res.print();

        env.execute();
    }
}
