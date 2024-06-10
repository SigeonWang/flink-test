package com.wxj.steaming.sink;

import com.wxj.bean.WaterSensor;
import com.wxj.function.WaterSensorMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.connector.jdbc.JdbcStatementBuilder;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

/**
 * @Author: xingjian wang
 * @Date: 2024/5/30 15:14
 * @Description: TODO
 *
 * 注意：
 * 自定义Sink想要实现状态一致性并不容易，所以一般只在没有其它选择时使用。
 * 实际项目中用到的外部连接器Flink官方基本都已实现，而且在不断地扩充，因此自定义的场景并不常见
 */
public class CustomSinkDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);

        SingleOutputStreamOperator<String> ds = env.fromElements("s1,1,2", "s2,2,4", "s3,5,8");

        // 注意，这里还是使用的旧API的 addSink() 方法，因为JdbcSink.sink()返回的还是SinkFunction接口，而不是新的Sink写法。
        ds.addSink(new MySink());

        env.execute();
    }

    public static class MySink extends RichSinkFunction<String> {
        Connection conn = null;  // 一般也是通过状态实现，而不是这样创建变量

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            // 创建连接等操作
//            conn = new xxx;
        }

        @Override
        public void close() throws Exception {
            super.close();
            // 清理，断开连接等操作
        }

        /**
         * 核心处理逻辑
         *
         * @param value
         * @param context
         * @throws Exception
         */
        @Override
        public void invoke(String value, Context context) throws Exception {

        }
    }
}
