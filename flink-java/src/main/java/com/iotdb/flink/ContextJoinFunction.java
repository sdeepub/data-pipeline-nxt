package com.iotdb.flink;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction;
import org.apache.flink.util.Collector;

public class ContextJoinFunction
    extends KeyedCoProcessFunction<
        String,
        SensorData,
        ContextEvent,
        ContextAwareTelemetry> {

    private transient ValueState<ContextEvent> currentContext;

    @Override
    public void open(Configuration parameters) throws Exception {

        ValueStateDescriptor<ContextEvent> desc =
            new ValueStateDescriptor<>(
                "current-context",
                ContextEvent.class);

        currentContext = getRuntimeContext().getState(desc);
    }

    @Override
    public void processElement1(
            SensorData telemetry,
            Context ctx,
            Collector<ContextAwareTelemetry> out)
            throws Exception {

        ContextEvent context = currentContext.value();

        ContextAwareTelemetry enriched =
            new ContextAwareTelemetry();

        enriched.setTelemetry(telemetry);
        enriched.setDeviceId(telemetry.getMachine_id());

        if (context != null) {
            enriched.setRunId(context.getRun_id());
            enriched.setEventType(context.getEvent_type());
            enriched.setStartTs(context.getStart_ts());
            enriched.setEndTs(context.getEnd_ts());
        }

        out.collect(enriched);
    }

    @Override
    public void processElement2(
            ContextEvent context,
            Context ctx,
            Collector<ContextAwareTelemetry> out)
            throws Exception {

        currentContext.update(context);
    }
}
