package com.iotdb.flink;

import com.fasterxml.jackson.databind.ObjectMapper;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import org.apache.iotdb.session.pool.SessionPool;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;


import java.util.Arrays;
import java.util.List;
import java.util.Properties;

public class KafkaToIoTDB {

    public static void main(String[] args) throws Exception {

        // ------------------------------------------------------------
        // Environment
        // ------------------------------------------------------------

        final StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        // ------------------------------------------------------------
        // Kafka Configuration
        // ------------------------------------------------------------

        String kafkaBootstrap = "kafka:29092";
        String kafkaTopic = "sensor-topic";

        Properties props = new Properties();
        props.setProperty("bootstrap.servers", kafkaBootstrap);
        props.setProperty("group.id", "flink-iotdb-group");

        FlinkKafkaConsumer<String> consumer =
                new FlinkKafkaConsumer<>(
                        kafkaTopic,
                        new SimpleStringSchema(),
                        props
                );

        consumer.setStartFromLatest();

        // ------------------------------------------------------------
        // Kafka Stream
        // ------------------------------------------------------------

        DataStream<String> rawStream = env.addSource(consumer);

        ObjectMapper mapper = new ObjectMapper();

        DataStream<SensorData> sensorStream =
                rawStream.map(json -> mapper.readValue(json, SensorData.class));

        // ------------------------------------------------------------
        // IoTDB Sink
        // ------------------------------------------------------------

        sensorStream.addSink(new RichSinkFunction<SensorData>() {

            private transient SessionPool sessionPool;

            @Override
            public void open(Configuration parameters) throws Exception {

                super.open(parameters);

                sessionPool = new SessionPool.Builder()
                        .host("iotdb")
                        .port(6667)
                        .user("root")
                        .password("root")
                        .maxSize(5)
                        .build();
            }

            @Override
            public void invoke(SensorData record, Context context)
                    throws Exception {

                try {

                    String device =
                            "root.factory1." + record.getMachine_id();

                    long timestamp = record.getTimestamp();

                    List<String> measurements = Arrays.asList(
                            "gas_temperature",
                            "gas_pressure",
                            "humidity",
                            "spin_rate",
                            "torque",
                            "status",
                            "fault_code"
                    );

                    List<Object> values = Arrays.asList(
                            record.getGas_temperature(),
                            record.getGas_pressure(),
                            record.getHumidity(),
                            record.getSpin_rate(),
                            record.getTorque(),
                            record.getStatus(),
                            record.getFault_code()
                    );

                    List<TSDataType> types =
                            Arrays.asList(
                                    TSDataType.DOUBLE,
                                    TSDataType.DOUBLE,
                                    TSDataType.DOUBLE,
                                    TSDataType.DOUBLE,
                                    TSDataType.DOUBLE,
                                    TSDataType.TEXT,
                                    TSDataType.TEXT
                            );

                    sessionPool.insertRecord(
                            device,
                            timestamp,
                            measurements,
                            types,
                            values
                    );

                } catch (IoTDBConnectionException |
                         StatementExecutionException e) {

                    e.printStackTrace();
                }
            }

            @Override
            public void close() throws Exception {

                if (sessionPool != null) {
                    sessionPool.close();
                }

                super.close();
            }
        });

        // ------------------------------------------------------------
        // Execute
        // ------------------------------------------------------------

        env.execute("Kafka To IoTDB Pipeline");
    }
}
