package com.wxj.steaming.sink;

import com.wxj.bean.WaterSensor;
import com.wxj.function.WaterSensorMapFunction;
import org.apache.flink.api.connector.sink.Sink;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.connector.jdbc.JdbcStatementBuilder;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

import java.sql.PreparedStatement;
import java.sql.SQLException;

/**
 * @Author: xingjian wang
 * @Date: 2024/5/30 13:31
 * @Description: TODO
 */
public class MysqlSinkDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);

        SingleOutputStreamOperator<WaterSensor> ds = env.fromElements("s1,1,2", "s2,2,4", "s3,5,8")
                .map(new WaterSensorMapFunction());

        /**
         * TODO 写入 mysql
         *
         *    1、只能用老的sink写法： addsink
         *    2、JDBCSink的4个参数:
         *        参数1： 执行的sql，一般就是 insert into
         *        参数2： 预编译sql， 对占位符填充
         *        参数3： 执行选项。批次大小、重试次数等
         *        参数4： 连接选项。url、用户名、密码等
         */
        SinkFunction<WaterSensor> jdbcSink = JdbcSink.sink(
                "insert into test.ws values(?,?,?)",
                new JdbcStatementBuilder<WaterSensor>() {
                    @Override
                    public void accept(PreparedStatement preparedStatement, WaterSensor waterSensor) throws SQLException {
                        preparedStatement.setString(1, waterSensor.getId());
                        preparedStatement.setLong(2, waterSensor.getTs());
                        preparedStatement.setInt(3, waterSensor.getVc());
                    }
                },
                JdbcExecutionOptions.builder()
                        .withMaxRetries(3)
                        .withBatchSize(100)
                        .withBatchIntervalMs(3 * 1000)
                        .build(),
                new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                        .withUrl("jdbc:mysql://localhost:3306/test?serverTimezone=Asia/Shanghai&useUnicode=true&characterEncoding=UTF-8")
                        .withUsername("root")
                        .withPassword("12345678")
                        .withConnectionCheckTimeoutSeconds(60 * 1000)
                        .build()
        );

        // 注意，这里还是使用的旧API的 addSink() 方法，因为JdbcSink.sink()返回的还是SinkFunction接口，而不是新的Sink写法。
        ds.addSink(jdbcSink);

        env.execute();
    }
}
