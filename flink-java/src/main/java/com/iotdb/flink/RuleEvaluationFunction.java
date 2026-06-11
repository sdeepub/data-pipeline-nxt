package com.iotdb.flink;

import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction;
import org.apache.flink.util.Collector;

public class RuleEvaluationFunction
    extends KeyedBroadcastProcessFunction<
        String,
        ContextAwareTelemetry,
        Rule,
        EvaluationResult> {

    private final MapStateDescriptor<String, Rule> rulesStateDesc;

    public RuleEvaluationFunction(
            MapStateDescriptor<String, Rule> rulesStateDesc) {

        this.rulesStateDesc = rulesStateDesc;
    }

    @Override
    public void processElement(
            ContextAwareTelemetry event,
            ReadOnlyContext ctx,
            Collector<EvaluationResult> out)
            throws Exception {

        EvaluationResult result =
            new EvaluationResult();

        SensorData telemetry =
            event.getTelemetry();

        result.device_id = telemetry.getMachine_id();
        result.timestamp = telemetry.getTimestamp();

        result.gas_temperature =
            telemetry.getGas_temperature();

        result.gas_pressure =
            telemetry.getGas_pressure();

        result.humidity =
            telemetry.getHumidity();

        result.spin_rate =
            telemetry.getSpin_rate();

        result.torque =
            telemetry.getTorque();

        result.status =
            telemetry.getStatus();

        result.fault_code =
            telemetry.getFault_code();

        result.passed = true;

        out.collect(result);
    }

    @Override
    public void processBroadcastElement(
            Rule rule,
            Context ctx,
            Collector<EvaluationResult> out)
            throws Exception {

        BroadcastState<String, Rule> state =
            ctx.getBroadcastState(rulesStateDesc);

        state.put(rule.getRule_id(), rule);
    }
}
