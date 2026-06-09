package com.iotdb.flink.operators;

import com.iotdb.flink.manifest.Manifest;
import org.apache.flink.api.common.state.*;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import org.apache.flink.api.common.state.StateTtlConfig;
import java.time.Duration;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * EdgeTrimmingFunction
 *
 * Keyed ProcessFunction that buffers telemetry and applies edge trimming:
 * - Drops (or tags) first N seconds (lead) and last N seconds (trail) of each run
 * - Buffers telemetry until run metadata (trackin/trackout) arrives
 * - Handles late trackout events (within grace period)
 * - Prevents unbounded state growth with TTL
 *
 * State Management:
 * - runWindowState: stores current run boundaries (startTime, endTime)
 * - telemetryBufferState: buffers telemetry events waiting for run metadata
 * - stateExpiryTimer: cleans up state after grace period
 *
 * Outputs:
 * - Main output: trimmed telemetry (passed filter)
 * - edgeTrimmedEvents side-output: events that were trimmed (for monitoring)
 * - pendingEvents side-output: events buffered but not yet processed
 *
 * Configuration (via Manifest):
 *   manifest.edgeTrimming.leadSeconds   -> lead trim window
 *   manifest.edgeTrimming.trailSeconds  -> trail trim window
 *   manifest.edgeTrimming.action        -> "drop" or "tag"
 *
 * Usage in pipeline:
 *   DataStream<EnrichedEvent> trimmed = enriched
 *       .keyBy(e -> e.deviceId)
 *       .process(new EdgeTrimmingFunction())
 *       .name("edge-trimming");
 *
 *   // Access side-outputs
 *   OutputTag<EdgeTrimEvent> trimmedTag = EdgeTrimmingFunction.TRIMMED_TAG;
 *   DataStream<EdgeTrimEvent> trimmedEvents = trimmed.getSideOutput(trimmedTag);
 */
public class EdgeTrimmingFunction extends KeyedProcessFunction<String, EnrichedEvent, EnrichedEvent> {

    private static final Logger log = LoggerFactory.getLogger(EdgeTrimmingFunction.class);

    // Side output tags
    public static final OutputTag<EdgeTrimEvent> TRIMMED_TAG =
            new OutputTag<EdgeTrimEvent>("edge-trimmed") {};

    public static final OutputTag<PendingEvent> PENDING_TAG =
            new OutputTag<PendingEvent>("pending-events") {};

    // Constants
    private static final long BUFFER_TIMEOUT_MS = 60_000; // 1 minute grace period for late trackout
    private static final int MAX_BUFFER_SIZE = 10_000; // prevent unbounded buffering
    private static final long TTL_MS = 300_000; // 5 minute state TTL (clean up old runs)

    // Broadcast state for manifests (set externally)
    private final MapStateDescriptor<String, Manifest> manifestStateDesc;

    // ─────────────────────────────────────────────────────────────────────────
    // State declarations
    // ─────────────────────────────────────────────────────────────────────────

    // Current run window for this device
    private ValueState<RunWindow> runWindowState;

    // Buffered telemetry waiting for run metadata
    private ListState<EnrichedEvent> telemetryBufferState;

    // Timestamp of last activity (for TTL)
    private ValueState<Long> lastActivityState;

    // ─────────────────────────────────────────────────────────────────────────
    // Constructor
    // ─────────────────────────────────────────────────────────────────────────

    public EdgeTrimmingFunction(MapStateDescriptor<String, Manifest> manifestStateDesc) {
        this.manifestStateDesc = manifestStateDesc;
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Lifecycle
    // ─────────────────────────────────────────────────────────────────────────

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);

	StateTtlConfig ttlConfig =
            StateTtlConfig
	    .newBuilder(Duration.ofMillis(TTL_MS))
	    .build();

        ValueStateDescriptor<RunWindow> runDesc =
                new ValueStateDescriptor<>("runWindow", RunWindow.class);
        runDesc.enableTimeToLive(Time.milliseconds(TTL_MS));
        runWindowState = getRuntimeContext().getState(runDesc);

        ListStateDescriptor<EnrichedEvent> bufferDesc =
                new ListStateDescriptor<>("telemetryBuffer", EnrichedEvent.class);
        bufferDesc.enableTimeToLive(Time.milliseconds(TTL_MS));
        telemetryBufferState = getRuntimeContext().getListState(bufferDesc);

        ValueStateDescriptor<Long> activityDesc =
                new ValueStateDescriptor<>("lastActivity", Long.class);
        activityDesc.enableTimeToLive(Time.milliseconds(TTL_MS));
        lastActivityState = getRuntimeContext().getState(activityDesc);

        log.debug("[EdgeTrimmingFunction] Open complete");
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Process telemetry events
    // ─────────────────────────────────────────────────────────────────────────

    @Override
    public void processElement(EnrichedEvent event,
                              KeyedProcessFunction<String, EnrichedEvent, EnrichedEvent>.Context ctx,
                              Collector<EnrichedEvent> out) throws Exception {

        updateLastActivity();

        RunWindow run = runWindowState.value();
        Manifest manifest = getManifest(ctx, event.deviceType);

        // Case 1: We have run metadata (trackin already arrived)
        if (run != null && run.startTime != null) {
            processTelemetryWithRunMetadata(event, run, manifest, ctx, out);
            return;
        }

        // Case 2: No run metadata yet — buffer and register timer for late trackout
        bufferTelemetry(event, ctx);
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Process context events (trackin/trackout)
    // ─────────────────────────────────────────────────────────────────────────

    public void processContextEvent(ContextEvent context,
                                    KeyedProcessFunction<String, EnrichedEvent, EnrichedEvent>.Context ctx,
                                    Collector<EnrichedEvent> out) throws Exception {

        updateLastActivity();

        Manifest manifest = getManifest(ctx, context.deviceType);

        // Update run window
        RunWindow run = new RunWindow(context.runId, context.startTime, context.endTime);
        runWindowState.update(run);

        log.debug("[EdgeTrimmingFunction] {} Run {} updated: start={}, end={}",
                ctx.getCurrentKey(), context.runId, context.startTime, context.endTime);

        // Flush buffered telemetry now that we have run metadata
        flushBuffer(run, manifest, ctx, out);
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Telemetry processing with run metadata
    // ─────────────────────────────────────────────────────────────────────────

    private void processTelemetryWithRunMetadata(EnrichedEvent event,
                                                  RunWindow run,
                                                  Manifest manifest,
                                                  KeyedProcessFunction<String, EnrichedEvent, EnrichedEvent>.Context ctx,
                                                  Collector<EnrichedEvent> out) throws Exception {

        if (run.startTime == null) {
            // startTime is null but endTime might not be (shouldn't happen)
            log.warn("[EdgeTrimmingFunction] {} Run has null startTime but non-null endTime",
                    ctx.getCurrentKey());
            out.collect(event); // pass through
            return;
        }

        // Get trimming config from manifest
        long leadMs = manifest.edgeTrimming.getLeadMillis();
        long trailMs = manifest.edgeTrimming.getTrailMillis();
        String action = manifest.edgeTrimming.action;

        // Compute effective window
        long effectiveStart = run.startTime + leadMs;
        long effectiveEnd = run.endTime != null ? run.endTime - trailMs : Long.MAX_VALUE;

        boolean inRange = event.ts >= effectiveStart && event.ts <= effectiveEnd;

        if (inRange) {
            // Pass through (not trimmed)
            out.collect(event);
        } else {
            // Trimmed event
            if ("tag".equals(action)) {
                // Tag it and pass through
                event.edgeTrimmed = true;
                out.collect(event);
                emitTrimmedEvent(event, ctx, "tagged");
            } else {
                // Drop it (default action)
                emitTrimmedEvent(event, ctx, "dropped");
                log.debug("[EdgeTrimmingFunction] {} Event {} dropped (outside run window)",
                        ctx.getCurrentKey(), event.ts);
            }
        }
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Buffering & flushing
    // ─────────────────────────────────────────────────────────────────────────

    private void bufferTelemetry(EnrichedEvent event,
                                 KeyedProcessFunction<String, EnrichedEvent, EnrichedEvent>.Context ctx) throws Exception {

        // Check buffer size
        List<EnrichedEvent> buffer = new ArrayList<>();
        for (EnrichedEvent e : telemetryBufferState.get()) {
            buffer.add(e);
        }

        if (buffer.size() >= MAX_BUFFER_SIZE) {
            log.warn("[EdgeTrimmingFunction] {} Buffer full ({}), dropping oldest events",
                    ctx.getCurrentKey(), MAX_BUFFER_SIZE);
            // Remove oldest 10%
            int removeCount = Math.max(1, MAX_BUFFER_SIZE / 10);
            for (int i = 0; i < removeCount && !buffer.isEmpty(); i++) {
                buffer.remove(0);
            }
        }

        buffer.add(event);
        telemetryBufferState.update(buffer);

        // Register timer for late trackout (if not already registered)
        ctx.timerService().registerProcessingTimeTimer(
                System.currentTimeMillis() + BUFFER_TIMEOUT_MS
        );

        log.debug("[EdgeTrimmingFunction] {} Buffered event (buffer size: {})",
                ctx.getCurrentKey(), buffer.size());
    }

    private void flushBuffer(RunWindow run,
                             Manifest manifest,
                             KeyedProcessFunction<String, EnrichedEvent, EnrichedEvent>.Context ctx,
                             Collector<EnrichedEvent> out) throws Exception {

        List<EnrichedEvent> buffer = new ArrayList<>();
        for (EnrichedEvent e : telemetryBufferState.get()) {
            buffer.add(e);
        }

        int flushed = 0;
        for (EnrichedEvent event : buffer) {
            processTelemetryWithRunMetadata(event, run, manifest, ctx, out);
            flushed++;
        }

        telemetryBufferState.clear();

        if (flushed > 0) {
            log.debug("[EdgeTrimmingFunction] {} Flushed {} buffered events",
                    ctx.getCurrentKey(), flushed);
        }
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Timers (for buffer timeout)
    // ─────────────────────────────────────────────────────────────────────────

    @Override
    public void onTimer(long timestamp,
                        KeyedProcessFunction<String, EnrichedEvent, EnrichedEvent>.OnTimerContext ctx,
                        Collector<EnrichedEvent> out) throws Exception {

        // Check if buffer still has data (late trackout didn't arrive)
        List<EnrichedEvent> buffer = new ArrayList<>();
        for (EnrichedEvent e : telemetryBufferState.get()) {
            buffer.add(e);
        }

        if (!buffer.isEmpty()) {
            RunWindow run = runWindowState.value();

            if (run == null || run.startTime == null) {
                // No trackout ever arrived — emit as pending
                for (EnrichedEvent event : buffer) {
                    ctx.output(PENDING_TAG, new PendingEvent(
                            event.deviceId,
                            event.ts,
                            "No trackout received within timeout"
                    ));
                }

                log.warn("[EdgeTrimmingFunction] {} Timeout: {} events pending, no trackout",
                        ctx.getCurrentKey(), buffer.size());
            } else {
                // We have startTime but no endTime (trackout never arrived)
                // Assume run ended and flush with computed end time
                run.endTime = System.currentTimeMillis();
                Manifest manifest = getRuntimeContext().getState(manifestStateDesc).value();

                if (manifest != null) {
                    flushBuffer(run, manifest, ctx, out);
                    log.warn("[EdgeTrimmingFunction] {} Flushed {} buffered events (assuming run end)",
                            ctx.getCurrentKey(), buffer.size());
                }
            }

            telemetryBufferState.clear();
        }
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Helpers
    // ─────────────────────────────────────────────────────────────────────────

    private Manifest getManifest(KeyedProcessFunction<String, EnrichedEvent, EnrichedEvent>.Context ctx,
                                  String deviceType) throws Exception {
        // In real implementation, get from broadcast state
        // This is a stub — manifests should come from broadcast state
        return null; // TODO: wire broadcast state properly
    }

    private void emitTrimmedEvent(EnrichedEvent event,
                                  KeyedProcessFunction<String, EnrichedEvent, EnrichedEvent>.Context ctx,
                                  String action) {
        ctx.output(TRIMMED_TAG, new EdgeTrimEvent(
                event.deviceId,
                event.ts,
                action
        ));
    }

    private void updateLastActivity() throws Exception {
        lastActivityState.update(System.currentTimeMillis());
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Inner classes
    // ─────────────────────────────────────────────────────────────────────────

    public static class RunWindow {
        public String runId;
        public Long startTime;
        public Long endTime;

        public RunWindow() {}

        public RunWindow(String runId, Long startTime, Long endTime) {
            this.runId = runId;
            this.startTime = startTime;
            this.endTime = endTime;
        }

        @Override
        public String toString() {
            return String.format("RunWindow[%s: %d - %s]", runId, startTime, endTime);
        }
    }

    public static class EdgeTrimEvent {
        public String deviceId;
        public Long timestamp;
        public String action; // "dropped" or "tagged"

        public EdgeTrimEvent() {}

        public EdgeTrimEvent(String deviceId, Long timestamp, String action) {
            this.deviceId = deviceId;
            this.timestamp = timestamp;
            this.action = action;
        }

        @Override
        public String toString() {
            return String.format("EdgeTrimEvent[%s @ %d: %s]", deviceId, timestamp, action);
        }
    }

    public static class PendingEvent {
        public String deviceId;
        public Long timestamp;
        public String reason;

        public PendingEvent() {}

        public PendingEvent(String deviceId, Long timestamp, String reason) {
            this.deviceId = deviceId;
            this.timestamp = timestamp;
            this.reason = reason;
        }

        @Override
        public String toString() {
            return String.format("PendingEvent[%s @ %d: %s]", deviceId, timestamp, reason);
        }
    }

    // Placeholder data classes (should come from your codebase)
    public static class EnrichedEvent {
        public String deviceId;
        public String deviceType;
        public String runId;
        public Long ts;
        public Map<String, Object> measurements;
        public boolean edgeTrimmed = false;
    }

    public static class ContextEvent {
        public String deviceId;
        public String deviceType;
        public String runId;
        public Long startTime;
        public Long endTime;
    }
}
