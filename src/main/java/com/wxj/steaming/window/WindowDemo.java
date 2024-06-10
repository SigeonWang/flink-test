package com.wxj.steaming.window;

import com.wxj.bean.WaterSensor;
import com.wxj.function.WaterSensorMapFunction;
import org.apache.commons.lang3.time.DateFormatUtils;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.connector.file.src.FileSource;
import org.apache.flink.connector.file.src.reader.TextLineInputFormat;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.*;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/**
 * @Author: xingjian wang
 * @Date: 2024/5/30 16:15
 * @Description: TODO
 */
public class WindowDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);

        SingleOutputStreamOperator<WaterSensor> ds = env.socketTextStream("localhost", 6666)
                .map(new WaterSensorMapFunction());
        // 使用lambda表达式只调用一个已经存在的方法且不做其它操作时，可以使用双冒号"::"的形式，称为"方法引用"
        KeyedStream<WaterSensor, String> keyDs = ds.keyBy(WaterSensor::getId);

        // TODO 1、指定窗口分配器
        // 指定使用什么窗口？ ---  计数 or 时间？滚动 or 滑动 or 会话？

        // 1.1、没有keyBy的窗口。窗口所有数据进入同一子任务，并行度变为1。
//        keyDs.windowAll();
        // 1.2、有keyBy的窗口。数据按key分为多条逻辑流，各自独立统计计算。

        // TODO 1) 基于时间（处理时间 / 事件时间）
        // 基于 处理时间 的 滚动窗口
        WindowedStream<WaterSensor, String, TimeWindow> windowDs = keyDs
                .window(TumblingProcessingTimeWindows.of(Time.seconds(10)));
                // 一天大小的滚动窗口，窗口从北京时间的00:00:00开始
//                .window(TumblingProcessingTimeWindows.of(Time.days(1), Time.hours(-8)));
        // 基于 处理时间 的 滑动窗口
//        keyDs.window(SlidingProcessingTimeWindows.of(Time.seconds(10), Time.seconds(2)));
        // 基于 处理时间 的 静态会话窗口
//        keyDs.window(ProcessingTimeSessionWindows.withGap(Time.seconds(5)));
        // 基于 处理时间 的 动态会话窗口
//        keyDs.window(ProcessingTimeSessionWindows.withDynamicGap(new SessionWindowTimeGapExtractor<WaterSensor>() {
//            /**
//             * 根据从每条数据中提取的字段动态更新会话窗口超时时间
//             */
//            @Override
//            public long extract(WaterSensor element) {
//                return element.getTs() * 1000;  // 单位ms
//            }
//        }));

        // TODO 2) 基于计数
        // 基于 计数 的 滚动窗口
//        keyDs.countWindow(10);
        // 基于 计数 的 滑动窗口
//        keyDs.countWindow(10, 2);
        // 基于 计数 的 全局窗口。计数窗口的底层调用就是GlobalWindows，需要自定义触发器，很少使用。
//        keyDs.window(GlobalWindows.create());

        // TODO 2、指定窗口函数
        // 指定窗口中数据的计算逻辑

        // TODO 1）增量聚合
        // 来一条数据计算一条，窗口触发的时候输出计算结果

        // reduce():
        //      传入 ReduceFunction 实现类。泛型参数：T 输入输出的数据类型
        //      1、相同key第一条数据来的时候不会调用reduce
        //      2、增量聚合：来一条数据计算一次，但是不会输出
        //      3、等到窗口触发计算，输出结果
        //      4、输入、中间结果、输出的数据类型一样，不够灵活。
//        windowDs.reduce(new ReduceFunction<WaterSensor>() {
//            @Override
//            public WaterSensor reduce(WaterSensor value1, WaterSensor value2) throws Exception {
//                System.out.println("value1="+value1+" value2="+value2);
//                return new WaterSensor(value2.getId(), value2.getTs(), value1.getVc()+value2.getVc());
//            }
//        }).print();

        // aggregate()
        //      传入 AggregateFunction 实现类。泛型参数：IN 输入, ACC 累加器, OUT 输出
        //      1、窗口的第一条数据到来，创建窗口并创建累加器
        //      2、增量计算：来一条计算一条，调用一次add方法，但不会输出
        //      3、等到窗口触发计算，调用一次getResult方法，输出结果
        //      4、输入、中间结果、输出的数据类型可以不同，比reduce灵活
//        windowDs.aggregate(new AggregateFunction<WaterSensor, Integer, String>() {
//            /**
//             * 初始化累加器
//             */
//            @Override
//            public Integer createAccumulator() {
//                System.out.println("初始化初始化为 0");
//                return 0;
//            }
//
//            /**
//             * 聚合逻辑
//             */
//            @Override
//            public Integer add(WaterSensor value, Integer accumulator) {
//                System.out.println("调用 add 方法. value=" + value);
//                return value.getVc()+accumulator;
//            }
//
//            /**
//             * 获取结果，窗口触发时输出
//             */
//            @Override
//            public String getResult(Integer accumulator) {
//                System.out.println("调用 getResult 方法");
//                return accumulator.toString();
//            }
//
//            /**
//             * 会话窗口才会用到，其他不用
//             */
//            @Override
//            public Integer merge(Integer a, Integer b) {
//                System.out.println("调用 merge 方法");
//                return null;
//            }
//        }).print();

        // 2）TODO 2）全窗口函数
        // 数据来了先不计算，窗口触发的时候再一起计算并输出结果

        // apply():
        //      传入 WindowFunction 实现类。泛型参数: IN 输入类型, OUT 输出类型, KEY key类型, W 窗口类型
        //      1、WindowFunction 可以获取到包含窗口所有数据的可迭代集合（Iterable），还可以拿到窗口（Window）本身的信息；
        //      2、能提供的上下文信息较少，也没有更高级的功能，推荐使用更高级的 ProcessWindowFunction。
//        windowDs.apply(new WindowFunction<WaterSensor, String, String, TimeWindow>() {
//            /**
//             *
//             * @param s         分区key
//             * @param window    窗口对象
//             * @param input     窗口缓存的数据
//             * @param out       采集器
//             * @throws Exception
//             */
//            @Override
//            public void apply(String s, TimeWindow window, Iterable<WaterSensor> input, Collector<String> out) throws Exception {
//                // TODO
//            }
//        }).print();

        // process():
        //      传入 ProcessWindowFunction 实现类。泛型参数: IN 输入类型, OUT 输出类型, KEY key类型, W 窗口类型
        windowDs.process(new ProcessWindowFunction<WaterSensor, String, String, TimeWindow>() {
            /**
             *
             * @param key       分区key
             * @param context   上下文
             * @param elements  窗口缓存的数据
             * @param out       采集器
             * @throws Exception
             */
            @Override
            public void process(String key, Context context, Iterable<WaterSensor> elements, Collector<String> out) throws Exception {
                // 通过上下文 context 可以拿到 window对象、侧输出流等更多信息
                TimeWindow window = context.window();
                long start = window.getStart();
                long end = window.getEnd();
                String windowStart = DateFormatUtils.format(start, "yyyy-MM-dd HH:mm:ss.SSS");
                String windowEnd = DateFormatUtils.format(end, "yyyy-MM-dd HH:mm:ss.SSS");
                // spliterator()方法返回一个java.util.Spliterator对象
                //      Spliterator 是Java 8中新增的一个接口，它用于支持数据流式处理。
                //      它是"分割器"（split-iterator）的缩写，意味着可以将数据流分割成多个小块，这些小块可以被独立处理。
                //      这个接口可以用于处理集合、数组、I/O缓冲区和Java流等数据集合
                long cnt = elements.spliterator().estimateSize();  // 数据的估计条数
                out.collect("key="+key+"的窗口[" +windowStart+", "+windowEnd+"] 包含 "
                        +cnt+" 条数据 => " + elements.toString());
            }
        }).print();

        env.execute();
    }
}
