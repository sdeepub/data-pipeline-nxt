package com.iotdb.flink;

import com.fasterxml.jackson.databind.ObjectMapper;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.runtime.state.storage.FileSystemCheckpointStorage;

import org.apache.iotdb.session.pool.SessionPool;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.write.record.Tablet;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;

import java.util.ArrayList;
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

        env.setParallelism(4);

	//Checkpoint every 30 seconds
	env.enableCheckpointing(30000);

	env.getCheckpointConfig()
	    .setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);

	env.getCheckpointConfig()
	    .setMinPauseBetweenCheckpoints(10000);

	env.getCheckpointConfig()
	    .setCheckpointTimeout(60000);

	env.getCheckpointConfig()
	    .setMaxConcurrentCheckpoints(1);

		// SAVEPOINT SUPPORT: Retain externalized checkpoints for manual savepoints
        env.getCheckpointConfig().enableExternalizedCheckpoints(
	       CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
 
        // SAVEPOINT STORAGE: Specify directory for recovery on restart
        env.getCheckpointConfig().setCheckpointStorage("file:///tmp/flink-checkpoints");

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
        // Kafka Stream Processing Layer
        // ------------------------------------------------------------
        DataStream<String> rawStream = env.addSource(consumer)
	    .name("Kafka Source");

        // Recommendation 1 Fix: Use a RichMapFunction to safely reuse a single
        // thread-safe ObjectMapper instance instead of creating one per record.
        DataStream<SensorData> sensorStream = rawStream.map(new RichMapFunction<String, SensorData>() {
            private transient ObjectMapper mapper;

            @Override
            public void open(Configuration parameters) throws Exception {
                super.open(parameters);
                this.mapper = new ObjectMapper();
            }

            @Override
            public SensorData map(String json) throws Exception {
                return mapper.readValue(json, SensorData.class);
            }
        })
	    .name("JSON Parser");

        // ------------------------------------------------------------
        // High-Throughput Batch IoTDB Tablet Sink
        // ------------------------------------------------------------
        sensorStream.addSink(new RichSinkFunction<SensorData>() {

            private transient SessionPool sessionPool;
            
            // Recommendation 2 Sinks: In-memory batch buffers and management properties
            private transient List<SensorData> batchBuffer;
            private int batchSizeThreshold;
            private long lastFlushTime;
            private long maxFlushIntervalMs;

            @Override
            public void open(Configuration parameters) throws Exception {
                super.open(parameters);

                // Initialize Native IoTDB Session Connection Pool
                sessionPool = new SessionPool.Builder()
                        .host("iotdb")
                        .port(6667)
                        .user("root")
                        .password("root")
                        .maxSize(5)
                        .build();

                // Initialize batch buffer state parameters
                this.batchBuffer = new ArrayList<>();
                this.batchSizeThreshold = 1000;      // Flush when 1,000 records accumulate
                this.maxFlushIntervalMs = 3000;      // Force safety flush every 3 seconds
                this.lastFlushTime = System.currentTimeMillis();
            }

            @Override
            public void invoke(SensorData record, Context context) throws Exception {
                // Buffer the record into memory
                batchBuffer.add(record);

                // Check processing thresholds to determine if we perform a high-performance batch write
                long currentTime = System.currentTimeMillis();
                if (batchBuffer.size() >= batchSizeThreshold || (currentTime - lastFlushTime) >= maxFlushIntervalMs) {
                    flushBatch();
                }
            }

            /**
             * Formats accumulated stream records into structures optimized for IoTDB 
             * column-major binary storage alignment, then executes a high-speed flush via insertTablet.
             */
            private void flushBatch() {
                if (batchBuffer.isEmpty()) {
                    lastFlushTime = System.currentTimeMillis();
                    return;
                }

                try {
                    // Define fixed schema attributes for measurement lines
		    List<MeasurementSchema> schemas = Arrays.asList(
								    new MeasurementSchema("gas_temperature", TSDataType.DOUBLE),
								    new MeasurementSchema("gas_pressure", TSDataType.DOUBLE),
								    new MeasurementSchema("humidity", TSDataType.DOUBLE),
								    new MeasurementSchema("spin_rate", TSDataType.DOUBLE),
								    new MeasurementSchema("torque", TSDataType.DOUBLE),
								    new MeasurementSchema("status", TSDataType.TEXT),
								    new MeasurementSchema("fault_code", TSDataType.TEXT)
								    );

                    // To optimize path ingestion, we group data segments by device identity paths
                    // avoiding structural wildcard mismatches at scale.
                    java.util.Map<String, List<SensorData>> recordsByDevice = new java.util.HashMap<>();
                    for (SensorData record : batchBuffer) {
                        String devicePath = "root.factory1." + record.getMachine_id();
                        recordsByDevice.computeIfAbsent(devicePath, k -> new ArrayList<>()).add(record);
                    }

                    // Process and transmit aggregated records for each distinct device path
                    for (java.util.Map.Entry<String, List<SensorData>> entry : recordsByDevice.entrySet()) {
                        String deviceId = entry.getKey();
                        List<SensorData> deviceRecords = entry.getValue();
                        int numRows = deviceRecords.size();

                        // Construct the optimized binary Tablet representation
			Tablet tablet = new Tablet(deviceId, schemas, numRows);
                        tablet.rowSize = numRows;

                        for (int i = 0; i < numRows; i++) {
                            SensorData data = deviceRecords.get(i);
                            
                            // Map the raw physical data timestamp
                            tablet.addTimestamp(i, data.getTimestamp());

                            // Map matching metrics natively down column blocks
			    tablet.addValue("gas_temperature", i, data.getGas_temperature());
                            tablet.addValue("gas_pressure", i, data.getGas_pressure());
                            tablet.addValue("humidity", i, data.getHumidity());
                            tablet.addValue("spin_rate", i, data.getSpin_rate());
                            tablet.addValue("torque", i, data.getTorque());
                            tablet.addValue("status", i, data.getStatus());
                            tablet.addValue("fault_code", i, data.getFault_code());
                        }

                        // Stream optimized binary tablet block directly to the database over native protocol
                        sessionPool.insertTablet(tablet);
                    }

                } catch (IoTDBConnectionException | StatementExecutionException e) {
                    System.err.println("Failed to insert Tablet batch to IoTDB due to database exception:");
                    e.printStackTrace();
                    // In real production networks, route poisoned entries to a Dead Letter Queue (DLQ) here
                } finally {
                    // Purge internal transient storage arrays and cycle timestamps
                    batchBuffer.clear();
                    lastFlushTime = System.currentTimeMillis();
                }
            }

            @Override
            public void close() throws Exception {
                // Ensure any remaining records left in the buffer during a shutdown or checkpoint are fully flushed
                if (batchBuffer != null && !batchBuffer.isEmpty()) {
                    flushBatch();
                }

                // Close out down-stream session pool connections cleanly
                if (sessionPool != null) {
                    sessionPool.close();
                }

                super.close();
            }
        })
	    .name("IoTDB Tablet Sink");

        // ------------------------------------------------------------
        // Execute Application
        // ------------------------------------------------------------
	env.getCheckpointConfig()
	    .setCheckpointStorage("file:///tmp/flink-checkpoints");
	
        env.execute("Kafka To IoTDB High-Performance Native Pipeline");
    }
}
