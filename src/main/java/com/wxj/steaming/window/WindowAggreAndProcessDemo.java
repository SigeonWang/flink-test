package com.wxj.steaming.window;

import com.wxj.bean.WaterSensor;
import com.wxj.function.WaterSensorMapFunction;
import org.apache.commons.lang3.time.DateFormatUtils;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/**
 * @Author: xingjian wang
 * @Date: 2024/5/30 18:17
 * @Description: TODO
 */
public class WindowAggreAndProcessDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);

        // 创建 KeyedStream
        SingleOutputStreamOperator<WaterSensor> ds = env.socketTextStream("localhost", 6666)
                .map(new WaterSensorMapFunction());
        KeyedStream<WaterSensor, String> keyDs = ds.keyBy(WaterSensor::getId);

        // 指定窗口分配器
        WindowedStream<WaterSensor, String, TimeWindow> windowDs = keyDs
                .window(TumblingProcessingTimeWindows.of(Time.seconds(10)));

        // TODO 指定聚合函数：增量聚合aggregate + 全窗口聚合process
        /**
         * 增量聚合 Aggregate + 全窗口 process
         *      1、增量聚合函数处理数据： 来一条计算一条
         *      2、窗口触发时，增量聚合函数 的结果（只有一条）传递给 全窗口函数
         *      3、经过全窗口函数的处理包装后，输出
         *
         * 结合两者的优点：
         *      1、增量聚合： 来一条计算一条，存储中间的计算结果，占用的空间少
         *      2、全窗口函数： 可以通过 上下文 实现灵活的功能
         *
         * 注意：reduce() 类似，也可以结合两者使用！
         */
        windowDs.aggregate(new MyAggregateFunction(), new MyProcessWindowsFunction()).print();

        env.execute();
    }

    public static class MyAggregateFunction implements AggregateFunction<WaterSensor, Integer, String> {
        @Override
        public Integer createAccumulator() {
            System.out.println("初始化初始化为 0");
            return 0;
        }

        @Override
        public Integer add(WaterSensor value, Integer accumulator) {
            System.out.println("调用 add 方法. value=" + value);
            return value.getVc() + accumulator;
        }

        @Override
        public String getResult(Integer accumulator) {
            System.out.println("调用 getResult 方法");
            return accumulator.toString();
        }

        @Override
        public Integer merge(Integer a, Integer b) {
            System.out.println("调用 merge 方法");
            return null;
        }
    }

    // 注意全窗口聚合函数的输入是增量聚合函数的输出，类型要对应！
    public static class MyProcessWindowsFunction extends ProcessWindowFunction<String, String, String, TimeWindow> {
        @Override
        public void process(String s, Context context, Iterable<String> elements, Collector<String> out) throws Exception {
            TimeWindow window = context.window();
            long start = window.getStart();
            long end = window.getEnd();
            String windowStart = DateFormatUtils.format(start, "yyyy-MM-dd HH:mm:ss.SSS");
            String windowEnd = DateFormatUtils.format(end, "yyyy-MM-dd HH:mm:ss.SSS");
            long cnt = elements.spliterator().estimateSize();  // 数据的估计条数
            out.collect("key=" + s + "的窗口[" + windowStart + ", " + windowEnd + "] 包含 " + cnt + " 条数据 => " + elements.toString());
        }
    }
}
